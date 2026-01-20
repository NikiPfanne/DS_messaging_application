"""
Distributed messaging server implementation.
Features:
- TCP connections for clients
- UDP multicast for server discovery & coordination
- LeLann-Chang-Roberts (LCR) leader election (Ring topology)
- Heartbeat-based fault detection
- Load balancing
"""

import socket
import threading
import time
import struct
import logging
from typing import Dict, Set, Optional, List
from protocol import (
    Message, MessageType, HeartbeatMessage, LeaderElectionMessage,
    LeaderAnnouncementMessage, ServerResponse, ClientMessage
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class Server:
    """Distributed messaging server"""
    
    # Multicast configuration for Discovery & Announcements
    MULTICAST_GROUP = '224.0.0.1'
    MULTICAST_PORT = 5007
    
    # Timeouts and intervals
    HEARTBEAT_INTERVAL = 2.0  # seconds
    FAILURE_TIMEOUT = 6.0  # seconds (3 missed heartbeats)
    ELECTION_TIMEOUT = 5.0  # seconds
    
    def __init__(self, server_id: str, tcp_port: int, max_clients: int = 100):
        self.server_id = server_id
        self.tcp_port = tcp_port
        self.max_clients = max_clients
        
        # Server state
        self.is_running = False
        self.is_leader = False
        self.current_leader = None
        self.election_in_progress = False
        self.election_initiator = None
        self.leader_lock = threading.Lock()  # Protect leader state
        
        # Client management
        self.clients: Dict[str, socket.socket] = {}
        self.client_lock = threading.Lock()
        
        # Server discovery and fault detection
        # server_id -> {last_heartbeat, host, port, is_leader, load}
        self.known_servers: Dict[str, dict] = {}  
        self.servers_lock = threading.Lock()
        
        # Sockets
        self.tcp_socket = None
        self.udp_socket = None
        
        # Threads
        self.threads: List[threading.Thread] = []

        # Coordination observation
        self.last_coordination_seen = time.time()
        self.start_time_monotonic = time.monotonic()
        self.forwarded_ids: Set[str] = set()
        self.last_election_attempt = 0.0
        
        self.logger = logging.getLogger(f"Server-{server_id}")
    
    def start(self):
        """Start the server"""
        self.is_running = True
        
        # Setup TCP socket for client connections
        self._setup_tcp_socket()
        
        # Setup UDP multicast socket for server coordination
        self._setup_udp_socket()
        
        # Start background threads
        self._start_threads()
        
        self.logger.info(f"Server {self.server_id} started on TCP port {self.tcp_port}")
    
    def stop(self):
        """Stop the server"""
        self.is_running = False
        
        # Close all client connections
        with self.client_lock:
            for client_id, client_socket in list(self.clients.items()):
                try:
                    client_socket.close()
                except:
                    pass
            self.clients.clear()
        
        # Close server sockets
        if self.tcp_socket:
            self.tcp_socket.close()
        if self.udp_socket:
            self.udp_socket.close()
        
        # Wait for threads to finish
        for thread in self.threads:
            try:
                thread.join(timeout=2.0)
            except RuntimeError:
                pass
        
        self.logger.info(f"Server {self.server_id} stopped")
    
    def _setup_tcp_socket(self):
        """Setup TCP socket for client connections"""
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind(('0.0.0.0', self.tcp_port))
        self.tcp_socket.listen(self.max_clients)
    
    def _setup_udp_socket(self):
        """Setup UDP multicast socket for server coordination"""
        try:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Bind to multicast port
            self.udp_socket.bind(('', self.MULTICAST_PORT))
            
            # Join multicast group
            mreq = struct.pack("4sl", socket.inet_aton(self.MULTICAST_GROUP), socket.INADDR_ANY)
            self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            
            # Allow sending multicast messages
            self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            
            self.logger.info("UDP multicast enabled")
        except Exception as e:
            self.logger.warning(f"Could not setup UDP multicast: {e}")
            self.logger.warning("Running in single-server mode (coordination disabled)")
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.bind(('127.0.0.1', 0))
    
    def _start_threads(self):
        """Start all background threads"""
        threads = [
            threading.Thread(target=self._accept_clients, daemon=True),
            threading.Thread(target=self._heartbeat_sender, daemon=True),
            threading.Thread(target=self._heartbeat_receiver, daemon=True),
            threading.Thread(target=self._failure_detector, daemon=True),
            threading.Thread(target=self._ensure_leader_when_alone, daemon=True),
            threading.Thread(target=self._leader_monitor, daemon=True),
        ]
        
        for thread in threads:
            thread.start()
            self.threads.append(thread)
    
    def _accept_clients(self):
        self.tcp_socket.settimeout(1.0)
        while self.is_running:
            try:
                client_socket, address = self.tcp_socket.accept()
                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address),
                    daemon=True
                )
                thread.start()
                self.threads.append(thread)
            except socket.timeout:
                continue
            except Exception as e:
                if self.is_running:
                    self.logger.error(f"Error accepting client: {e}")
    
    def _handle_client(self, client_socket: socket.socket, address):
        client_id = None
        try:
            try:
                client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            except (OSError, AttributeError):
                pass
            client_socket.settimeout(None)
            
            while self.is_running:
                length_data = self._recv_exactly(client_socket, 4)
                if not length_data: break
                
                length = int.from_bytes(length_data, byteorder='big')
                if length > 1024 * 1024: break
                
                msg_data = self._recv_exactly(client_socket, length)
                if not msg_data: break
                
                msg = Message.from_json(msg_data.decode('utf-8'))
                
                if msg.msg_type == MessageType.CLIENT_REGISTER:
                    client_id = msg.sender_id
                    with self.client_lock:
                        if client_id in self.clients:
                            self._send_message(client_socket, ServerResponse(self.server_id, False, "ID taken"))
                            break
                        self.clients[client_id] = client_socket
                    
                    self._send_message(client_socket, ServerResponse(self.server_id, True, "Registered", {'server_id': self.server_id}))
                    self.logger.info(f"Client {client_id} registered from {address}")
                
                elif msg.msg_type == MessageType.CLIENT_MESSAGE:
                    self._handle_client_message(msg, client_socket)
                
                elif msg.msg_type == MessageType.CLIENT_UNREGISTER:
                    break
        except Exception:
            pass
        finally:
            if client_id:
                with self.client_lock:
                    self.clients.pop(client_id, None)
            try:
                client_socket.close()
            except:
                pass

    def _recv_exactly(self, sock: socket.socket, n: int) -> Optional[bytes]:
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk: return None
            data += chunk
        return data

    def _send_message(self, sock: socket.socket, msg: Message):
        try:
            sock.sendall(msg.to_bytes())
        except Exception:
            pass

    def _handle_client_message(self, msg: Message, client_socket: socket.socket):
        content = msg.payload.get('content', '')
        recipient = msg.payload.get('recipient')
        
        self._send_message(client_socket, ServerResponse(self.server_id, True, "Received", {'message_id': msg.message_id}))
        
        if recipient:
            self._deliver_message(msg.sender_id, recipient, content, msg_id=msg.message_id)
        else:
            self._broadcast_message(msg.sender_id, content, forward=True, msg_id=msg.message_id, skip_sender=True)

    def _deliver_message(self, sender: str, recipient: str, content: str, msg_id: str = None):
        if msg_id is None: msg_id = f"{sender}:{recipient}:{time.time()}"
        forwarded = False
        with self.client_lock:
            if recipient in self.clients:
                if recipient == sender: forwarded = True
                else:
                    try:
                        self._send_message(self.clients[recipient], ClientMessage(sender, content))
                        return
                    except: forwarded = True
            else: forwarded = True

        if forwarded:
            self._send_udp_message(Message(MessageType.FORWARD_MESSAGE, self.server_id, {
                'original_sender': sender, 'recipient': recipient, 'content': content, 'msg_id': msg_id, 'broadcast': False
            }), use_multicast=True)

    def _broadcast_message(self, sender: str, content: str, forward: bool = False, msg_id: str = None, skip_sender: bool = True):
        if msg_id is None: msg_id = f"bcast:{sender}:{time.time()}"
        with self.client_lock:
            targets = [cid for cid in self.clients.keys() if not (skip_sender and cid == sender)]
        for recipient in targets:
            try:
                self._send_message(self.clients[recipient], ClientMessage(sender, content))
            except: pass
        if forward:
            self._send_udp_message(Message(MessageType.FORWARD_MESSAGE, self.server_id, {
                'original_sender': sender, 'recipient': None, 'content': content, 'broadcast': True, 'msg_id': msg_id
            }), use_multicast=True)

    def _send_udp_message(self, msg: Message, target_host: str = None, target_port: int = None, use_multicast: bool = False):
        """Helper to send UDP messages via Multicast or Unicast"""
        try:
            data = msg.to_json().encode('utf-8')
            if use_multicast:
                self.udp_socket.sendto(data, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
            elif target_host:
                port = target_port if target_port else self.MULTICAST_PORT
                self.udp_socket.sendto(data, (target_host, port))
        except Exception as e:
            self.logger.debug(f"UDP send error: {e}")

    # --- Coordination Logic ---

    def _heartbeat_sender(self):
        """Send periodic heartbeats via UDP multicast (Dynamic Discovery)"""
        while self.is_running:
            try:
                with self.leader_lock: is_leader = self.is_leader
                heartbeat = HeartbeatMessage(self.server_id, self.tcp_port, is_leader, len(self.clients))
                self._send_udp_message(heartbeat, use_multicast=True)
                time.sleep(self.HEARTBEAT_INTERVAL)
            except Exception:
                pass
    
    def _heartbeat_receiver(self):
        """Receive heartbeats from other servers via UDP"""
        self.udp_socket.settimeout(1.0)
        while self.is_running:
            try:
                data, address = self.udp_socket.recvfrom(4096)
                sender_ip = address[0] # Extract IP for unicast responses
                
                msg = Message.from_json(data.decode('utf-8'))
                
                # CRITICAL: Ignore own heartbeats so we know if we are truly alone
                if msg.sender_id == self.server_id:
                    continue
                
                self.last_coordination_seen = time.time()
                
                if msg.msg_type == MessageType.HEARTBEAT:
                    self._handle_heartbeat(msg, sender_ip)
                elif msg.msg_type == MessageType.LEADER_ELECTION:
                    self._handle_election_message(msg)
                elif msg.msg_type == MessageType.LEADER_ANNOUNCEMENT:
                    self._handle_leader_announcement(msg)
                elif msg.msg_type == MessageType.FORWARD_MESSAGE:
                    self._handle_forward_message(msg)
                elif msg.msg_type == MessageType.SERVER_DISCOVERY:
                    self._handle_client_discovery(address)
            except socket.timeout:
                continue
            except Exception as e:
                if self.is_running: self.logger.error(f"Error receiving multicast: {e}")

    def _handle_heartbeat(self, msg: Message, sender_ip: str):
        """Handle heartbeat and update known_servers list (Dynamic Discovery)"""
        server_id = msg.sender_id
        with self.servers_lock:
            self.known_servers[server_id] = {
                'last_heartbeat': time.time(),
                'host': sender_ip, # Store IP for Unicast Ring communication
                'port': msg.payload.get('server_port'),
                'is_leader': msg.payload.get('is_leader', False),
                'load': msg.payload.get('load', 0)
            }
            if msg.payload.get('is_leader'):
                with self.leader_lock: self.current_leader = server_id

    def _get_neighbor(self) -> Optional[dict]:
        """
        Determine the right neighbor in the Ring.
        """
        with self.servers_lock:
            # Filter for active servers only
            active_servers = [sid for sid, info in self.known_servers.items() 
                              if time.time() - info['last_heartbeat'] <= self.FAILURE_TIMEOUT]
            
            # Form the ring including self
            ring_members = sorted(active_servers + [self.server_id])
            
            if len(ring_members) <= 1:
                return None
            
            my_index = ring_members.index(self.server_id)
            neighbor_id = ring_members[(my_index + 1) % len(ring_members)]
            
            if neighbor_id == self.server_id:
                return None
                
            return self.known_servers.get(neighbor_id)

    def _initiate_election(self):
        """
        Initiate leader election using LCR algorithm.
        """
        with self.leader_lock:
            if self.election_in_progress: return
            self.election_in_progress = True
            self.election_initiator = self.server_id
            self.last_election_attempt = time.monotonic()
        
        self.logger.info("Initiating leader election (LCR Ring)")
        
        election_msg = LeaderElectionMessage(self.server_id, self.server_id, self.server_id)
        
        neighbor = self._get_neighbor()
        if neighbor:
            # SEND TO NEIGHBOR ONLY (Unicast)
            self.logger.info(f"Sending election token to neighbor at {neighbor['host']}")
            self._send_udp_message(election_msg, target_host=neighbor['host'])
            threading.Thread(target=self._election_timeout, daemon=True).start()
        else:
            # No neighbors? We are leader.
            self.logger.info("No neighbors found for ring. Becoming leader immediately.")
            self._become_leader()

    def _handle_election_message(self, msg: Message):
        candidate_id = msg.payload.get('candidate_id')
        initiator_id = msg.payload.get('initiator_id')
        
        neighbor = self._get_neighbor()
        
        if candidate_id > self.server_id:
            if neighbor:
                fwd_msg = LeaderElectionMessage(self.server_id, candidate_id, initiator_id)
                self._send_udp_message(fwd_msg, target_host=neighbor['host'])
        
        elif candidate_id < self.server_id:
            if not self.election_in_progress:
                self._initiate_election()

        elif candidate_id == self.server_id:
            self.logger.info("Election token returned. We won.")
            self._become_leader()

    def _become_leader(self):
        with self.leader_lock:
            self.is_leader = True
            self.current_leader = self.server_id
            self.election_in_progress = False
        
        self.logger.info(f"Server {self.server_id} became LEADER")
        announcement = LeaderAnnouncementMessage(self.server_id, self.server_id)
        self._send_udp_message(announcement, use_multicast=True)

    def _election_timeout(self):
        time.sleep(self.ELECTION_TIMEOUT)
        with self.leader_lock:
            if self.election_in_progress:
                neighbor = self._get_neighbor()
                if not neighbor:
                    self.is_leader = True
                    self.current_leader = self.server_id
                    self.election_in_progress = False
                    self.logger.info("Election timeout with no peers. Forcing leadership.")
                    announcement = LeaderAnnouncementMessage(self.server_id, self.server_id)
                    self._send_udp_message(announcement, use_multicast=True)
                else:
                    self.election_in_progress = False 

    def _failure_detector(self):
        while self.is_running:
            time.sleep(self.HEARTBEAT_INTERVAL)
            current_time = time.time()
            failed_servers = []
            leader_failed = False
            
            with self.servers_lock:
                for server_id, info in list(self.known_servers.items()):
                    if current_time - info['last_heartbeat'] > self.FAILURE_TIMEOUT:
                        failed_servers.append(server_id)
                        del self.known_servers[server_id]
            
            if failed_servers:
                self.logger.warning(f"Detected failed servers: {failed_servers}")
                with self.leader_lock:
                    if self.current_leader in failed_servers:
                        leader_failed = True
                        self.current_leader = None
                        self.is_leader = False
                if leader_failed:
                    self.logger.warning("Leader failed, initiating election")
                    self._initiate_election()

    def _handle_leader_announcement(self, msg: Message):
        leader_id = msg.payload.get('leader_id')
        with self.leader_lock:
            self.current_leader = leader_id
            self.election_in_progress = False
            if leader_id != self.server_id:
                self.is_leader = False
                self.logger.info(f"Acknowledged {leader_id} as leader")
            else:
                self.is_leader = True 

    def _handle_forward_message(self, msg: Message):
        payload = msg.payload or {}
        if payload.get('msg_id') in self.forwarded_ids: return
        self.forwarded_ids.add(payload.get('msg_id'))
        
        if payload.get('broadcast'):
            self._broadcast_message(payload['original_sender'], payload['content'], False, payload['msg_id'], False)
        else:
            recipient = payload.get('recipient')
            with self.client_lock:
                if recipient in self.clients:
                    try: self._send_message(self.clients[recipient], ClientMessage(payload['original_sender'], payload['content']))
                    except: pass
                else:
                    self._send_udp_message(msg, use_multicast=True)

    def _ensure_leader_when_alone(self):
        """Monitor environment and self-elect if alone."""
        while self.is_running:
            time.sleep(self.HEARTBEAT_INTERVAL)
            with self.leader_lock:
                if self.is_leader: continue
            
            with self.servers_lock:
                # IMPORTANT FIX: If list is empty, we are alone.
                if len(self.known_servers) == 0:
                     self.logger.info("No known servers in list. Assuming leadership immediately.")
                     self._become_leader()
                     continue

                active = [s for s, i in self.known_servers.items() if time.time() - i['last_heartbeat'] < self.FAILURE_TIMEOUT]
            
            time_since_coord = time.time() - self.last_coordination_seen
            
            if not active and time_since_coord > self.FAILURE_TIMEOUT:
                self.logger.info(f"Alone in network (Last seen: {time_since_coord:.1f}s ago). Becoming leader.")
                self._become_leader()

    def _leader_monitor(self):
        while self.is_running:
            time.sleep(self.HEARTBEAT_INTERVAL)
            with self.leader_lock:
                if not self.current_leader and not self.is_leader and not self.election_in_progress:
                    if time.monotonic() - self.last_election_attempt > self.HEARTBEAT_INTERVAL * 2:
                        self._initiate_election()

    def _handle_client_discovery(self, client_address):
        """
        Handle discovery request from a client.
        IMPORTANT FIX: Respond even if NOT leader so client can connect.
        """
        # We always respond so the client stops trying dead servers.
        # Ideally point to the leader, or self if we don't know who the leader is.
        
        target_server_id = self.current_leader if self.current_leader else self.server_id
        
        # If we are the target (or we are alone/leaderless), use our own info
        if target_server_id == self.server_id:
             host = 'localhost'
             port = self.tcp_port
        else:
             # Look up leader info
             with self.servers_lock:
                 info = self.known_servers.get(target_server_id)
                 if info:
                     host = info.get('host', 'localhost')
                     port = info.get('port', 5000)
                 else:
                     # Fallback to self if leader info missing
                     target_server_id = self.server_id
                     host = 'localhost'
                     port = self.tcp_port

        resp = Message(MessageType.SERVER_ANNOUNCE, self.server_id, {
            'host': host, 
            'port': port, 
            'server_id': target_server_id
        })
        self.udp_socket.sendto(resp.to_json().encode('utf-8'), client_address)
        self.logger.info(f"Responded to discovery from {client_address} pointing to {target_server_id}")

    def get_status(self) -> dict:
        with self.client_lock: c = len(self.clients)
        with self.servers_lock: s = len(self.known_servers)
        with self.leader_lock: return {
            'server_id': self.server_id, 'tcp_port': self.tcp_port,
            'is_leader': self.is_leader, 'current_leader': self.current_leader,
            'connected_clients': c, 'known_servers': s, 'election_in_progress': self.election_in_progress
        }

def main():
    import sys
    if len(sys.argv) < 3:
        print("Usage: python server.py <server_id> <tcp_port>")
        sys.exit(1)
    
    server_id = sys.argv[1]
    tcp_port = int(sys.argv[2])
    server = Server(server_id, tcp_port)
    server.start()
    
    try:
        while True:
            time.sleep(1)
            status = server.get_status()
            leader_display = 'YES' if status['is_leader'] else (status['current_leader'] or 'None')
            print(f"\rStatus: Clients={status['connected_clients']}, Servers={status['known_servers']}, Leader={leader_display}   ", end='', flush=True)
    except KeyboardInterrupt:
        server.stop()

if __name__ == '__main__':
    main()