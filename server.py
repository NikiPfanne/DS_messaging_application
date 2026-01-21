"""
Distributed messaging server implementation.
Features:
- TCP connections for clients
- UDP multicast for server discovery & coordination
- LeLann-Chang-Roberts (LCR) leader election (Ring topology)
- Heartbeat-based fault detection
- Load balancing

According to project requirements:
- Servers send heartbeats every 2 seconds to all servers (UDP multicast)
- Server is considered failed after 6 seconds (3 missed heartbeats)
- Dynamic ring: bypass failed nodes during election
- Clients use UDP multicast for server discovery
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
    
    # Timeouts and intervals (per PDF requirements)
    HEARTBEAT_INTERVAL = 2.0  # seconds - heartbeat every 2 seconds
    FAILURE_TIMEOUT = 6.0     # seconds - 3 missed heartbeats = failed
    ELECTION_TIMEOUT = 5.0    # seconds
    STARTUP_GRACE_PERIOD = 3.0  # Wait before self-electing if alone
    
    def __init__(self, server_id: str, tcp_port: int, max_clients: int = 100):
        self.server_id = server_id
        self.tcp_port = tcp_port
        self.max_clients = max_clients
        
        self.is_running = False
        self.is_leader = False
        self.current_leader: Optional[str] = None
        self.election_in_progress = False
        self.election_participant = False  # Track if we're part of an ongoing election
        
        # Thread-safe locks
        self.leader_lock = threading.RLock()
        
        self.clients: Dict[str, socket.socket] = {}
        self.client_lock = threading.RLock()
        
        # server_id -> {last_heartbeat, host, port, is_leader, load}
        self.known_servers: Dict[str, dict] = {}  
        self.servers_lock = threading.RLock()
        
        self.tcp_socket = None
        self.udp_socket = None
        self.threads: List[threading.Thread] = []

        self.last_coordination_seen = time.time()
        self.start_time = time.time()
        self.forwarded_ids: Set[str] = set()
        self.last_election_attempt = 0.0
        
        self.logger = logging.getLogger(f"Server-{server_id}")
    
    def start(self):
        self.is_running = True
        self.start_time = time.time()
        self._setup_tcp_socket()
        self._setup_udp_socket()
        self._start_threads()
        self.logger.info(f"Server {self.server_id} started on TCP port {self.tcp_port}")
    
    def stop(self):
        self.is_running = False
        with self.client_lock:
            for s in self.clients.values():
                try: s.close()
                except: pass
            self.clients.clear()
        if self.tcp_socket: self.tcp_socket.close()
        if self.udp_socket: self.udp_socket.close()
        self.logger.info(f"Server {self.server_id} stopped")
    
    def _setup_tcp_socket(self):
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind(('0.0.0.0', self.tcp_port))
        self.tcp_socket.listen(self.max_clients)
    
    def _setup_udp_socket(self):
        try:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_socket.bind(('', self.MULTICAST_PORT))
            mreq = struct.pack("4sl", socket.inet_aton(self.MULTICAST_GROUP), socket.INADDR_ANY)
            self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            self.logger.info("UDP multicast enabled")
        except Exception as e:
            self.logger.error(f"UDP Setup failed: {e}")

    def _start_threads(self):
        threads = [
            threading.Thread(target=self._accept_clients, daemon=True),
            threading.Thread(target=self._heartbeat_sender, daemon=True),
            threading.Thread(target=self._heartbeat_receiver, daemon=True),
            threading.Thread(target=self._failure_detector, daemon=True),
            threading.Thread(target=self._leader_monitor, daemon=True),
        ]
        for t in threads:
            t.start()
            self.threads.append(t)
    
    # ==================== CLIENT HANDLING ====================
    
    def _accept_clients(self):
        self.tcp_socket.settimeout(1.0)
        while self.is_running:
            try:
                client_socket, address = self.tcp_socket.accept()
                threading.Thread(target=self._handle_client, args=(client_socket, address), daemon=True).start()
            except socket.timeout: continue
            except Exception: pass
    
    def _handle_client(self, client_socket: socket.socket, address):
        client_id = None
        try:
            client_socket.settimeout(None)
            while self.is_running:
                length_data = self._recv_exactly(client_socket, 4)
                if not length_data: break
                length = int.from_bytes(length_data, 'big')
                if length > 1024 * 1024: break  # 1MB limit
                msg_data = self._recv_exactly(client_socket, length)
                if not msg_data: break
                
                msg = Message.from_json(msg_data.decode('utf-8'))
                
                if msg.msg_type == MessageType.CLIENT_REGISTER:
                    client_id = msg.sender_id
                    with self.client_lock:
                        self.clients[client_id] = client_socket
                    self._send_tcp(client_socket, ServerResponse(self.server_id, True, "Registered"))
                    self.logger.info(f"Client {client_id} registered from {address}")
                
                elif msg.msg_type == MessageType.CLIENT_MESSAGE:
                    self._handle_client_message(msg, client_socket)
                
                elif msg.msg_type == MessageType.CLIENT_UNREGISTER:
                    break
        except Exception as e:
            self.logger.debug(f"Client handler error: {e}")
        finally:
            if client_id:
                with self.client_lock: self.clients.pop(client_id, None)
                self.logger.info(f"Client {client_id} disconnected")
            try: client_socket.close()
            except: pass

    def _recv_exactly(self, sock, n):
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk: return None
            data += chunk
        return data

    def _send_tcp(self, sock, msg):
        try: sock.sendall(msg.to_bytes())
        except: pass

    def _handle_client_message(self, msg: Message, client_socket: socket.socket):
        """Handle message from client - either private or broadcast"""
        content = msg.payload.get('content', '')
        recipient = msg.payload.get('recipient')
        
        # Send ACK to client
        self._send_tcp(client_socket, ServerResponse(self.server_id, True, "Received", {'message_id': msg.message_id}))
        
        if recipient:
            # Private message to specific recipient
            self._deliver_private_message(msg.sender_id, recipient, content, msg.message_id)
        else:
            # Broadcast to all clients
            self._broadcast_message(msg.sender_id, content, msg.message_id)

    def _deliver_private_message(self, sender: str, recipient: str, content: str, msg_id: str):
        """Deliver private message - local first, then forward via multicast"""
        delivered_locally = False
        
        # 1. Try local delivery first
        with self.client_lock:
            if recipient in self.clients:
                try:
                    self._send_tcp(self.clients[recipient], ClientMessage(sender, content))
                    self.logger.info(f"Delivered private message from {sender} to {recipient} (local)")
                    delivered_locally = True
                except Exception as e:
                    self.logger.debug(f"Local delivery failed: {e}")
        
        # 2. Always forward via multicast so other servers can deliver
        #    (recipient might be connected to another server)
        if not delivered_locally:
            fwd_msg = Message(MessageType.FORWARD_MESSAGE, self.server_id, {
                'original_sender': sender, 
                'recipient': recipient, 
                'content': content, 
                'msg_id': msg_id, 
                'broadcast': False
            })
            self._send_udp_multicast(fwd_msg)
            self.logger.info(f"Forwarded private message for {recipient} via multicast")

    def _broadcast_message(self, sender: str, content: str, msg_id: str):
        """Broadcast message to all clients on all servers"""
        # 1. Send to all local clients (except sender)
        with self.client_lock:
            for cid, sock in self.clients.items():
                if cid != sender:
                    try:
                        self._send_tcp(sock, ClientMessage(sender, content))
                    except: pass
        
        # 2. Forward to other servers via multicast
        fwd_msg = Message(MessageType.FORWARD_MESSAGE, self.server_id, {
            'original_sender': sender, 
            'recipient': None, 
            'content': content, 
            'msg_id': msg_id, 
            'broadcast': True
        })
        self._send_udp_multicast(fwd_msg)

    def _handle_forward_message(self, msg: Message):
        """Handle forwarded message from another server"""
        payload = msg.payload
        msg_id = payload.get('msg_id')
        
        # Avoid processing same message twice
        if msg_id in self.forwarded_ids: 
            return
        self.forwarded_ids.add(msg_id)
        
        # Cleanup old message IDs (keep last 1000)
        if len(self.forwarded_ids) > 1000:
            self.forwarded_ids = set(list(self.forwarded_ids)[-500:])
        
        sender = payload['original_sender']
        content = payload['content']
        recipient = payload.get('recipient')
        is_broadcast = payload.get('broadcast', False)
        
        with self.client_lock:
            if is_broadcast:
                # Broadcast: deliver to all local clients except sender
                for cid, sock in self.clients.items():
                    if cid != sender:
                        try:
                            self._send_tcp(sock, ClientMessage(sender, content))
                        except: pass
            else:
                # Private message: only deliver if recipient is connected here
                if recipient and recipient in self.clients:
                    try:
                        self._send_tcp(self.clients[recipient], ClientMessage(sender, content))
                        self.logger.info(f"Delivered forwarded private message to {recipient}")
                    except: pass

    # ==================== UDP COMMUNICATION ====================
    
    def _send_udp_multicast(self, msg: Message):
        """Send message to all servers via UDP multicast"""
        try:
            data = msg.to_json().encode('utf-8')
            self.udp_socket.sendto(data, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
        except Exception as e:
            self.logger.debug(f"UDP multicast error: {e}")

    # ==================== HEARTBEAT & DISCOVERY ====================

    def _heartbeat_sender(self):
        """Send heartbeat every 2 seconds to all servers via UDP multicast"""
        while self.is_running:
            try:
                with self.leader_lock: 
                    is_leader = self.is_leader
                with self.client_lock:
                    load = len(self.clients)
                
                hb = HeartbeatMessage(self.server_id, self.tcp_port, is_leader, load)
                self._send_udp_multicast(hb)
                time.sleep(self.HEARTBEAT_INTERVAL)
            except: pass

    def _heartbeat_receiver(self):
        """Receive and process UDP messages from other servers"""
        self.udp_socket.settimeout(1.0)
        while self.is_running:
            try:
                data, addr = self.udp_socket.recvfrom(4096)
                msg = Message.from_json(data.decode('utf-8'))
                
                # Ignore own messages
                if msg.sender_id == self.server_id: 
                    continue
                
                self.last_coordination_seen = time.time()
                
                if msg.msg_type == MessageType.HEARTBEAT:
                    self._handle_heartbeat(msg, addr[0])
                elif msg.msg_type == MessageType.LEADER_ELECTION:
                    self._handle_election_message(msg)
                elif msg.msg_type == MessageType.LEADER_ANNOUNCEMENT:
                    self._handle_leader_announcement(msg)
                elif msg.msg_type == MessageType.FORWARD_MESSAGE:
                    self._handle_forward_message(msg)
                elif msg.msg_type == MessageType.SERVER_DISCOVERY:
                    self._handle_client_discovery(addr)
                    
            except socket.timeout: 
                continue
            except Exception as e:
                self.logger.debug(f"Receiver error: {e}")

    def _handle_heartbeat(self, msg: Message, ip: str):
        """Process heartbeat from another server - updates known_servers"""
        with self.servers_lock:
            self.known_servers[msg.sender_id] = {
                'last_heartbeat': time.time(),
                'host': ip,
                'port': msg.payload['server_port'],
                'is_leader': msg.payload['is_leader'],
                'load': msg.payload['load']
            }
        
        # Update leader info if sender claims to be leader
        if msg.payload['is_leader']:
            with self.leader_lock: 
                self.current_leader = msg.sender_id

    def _handle_client_discovery(self, addr):
        """Handle discovery request from client - respond with best server"""
        # Determine which server to recommend
        best_server_id = self.server_id
        best_host = 'localhost'
        best_port = self.tcp_port
        min_load = len(self.clients)
        
        with self.servers_lock:
            for sid, info in self.known_servers.items():
                if time.time() - info['last_heartbeat'] <= self.FAILURE_TIMEOUT:
                    if info['load'] < min_load:
                        min_load = info['load']
                        best_server_id = sid
                        best_host = info['host']
                        best_port = info['port']
        
        # If we're the best or leader, use localhost for local clients
        if best_server_id == self.server_id:
            best_host = 'localhost'
            best_port = self.tcp_port
        
        resp = Message(MessageType.SERVER_ANNOUNCE, self.server_id, {
            'host': best_host, 
            'port': best_port, 
            'server_id': best_server_id
        })
        try:
            self.udp_socket.sendto(resp.to_json().encode('utf-8'), addr)
            self.logger.info(f"Responded to client discovery with server {best_server_id}")
        except: pass

    # ==================== FAILURE DETECTION ====================

    def _failure_detector(self):
        """Detect failed servers (3 missed heartbeats = 6 seconds)"""
        while self.is_running:
            time.sleep(self.HEARTBEAT_INTERVAL)
            
            current_time = time.time()
            failed_servers = []
            
            with self.servers_lock:
                for sid, info in list(self.known_servers.items()):
                    if current_time - info['last_heartbeat'] > self.FAILURE_TIMEOUT:
                        failed_servers.append(sid)
                        del self.known_servers[sid]
            
            if failed_servers:
                self.logger.warning(f"Detected failed servers: {failed_servers}")
                
                # Check if leader failed - trigger re-election
                with self.leader_lock:
                    if self.current_leader in failed_servers:
                        self.logger.warning("LEADER FAILED - initiating re-election")
                        self.current_leader = None
                        self.is_leader = False
                        self.election_in_progress = False  # Reset so we can start new election
                
                # Always try to initiate election when leader fails
                with self.leader_lock:
                    if self.current_leader is None and not self.is_leader:
                        self._initiate_election()

    # ==================== LEADER ELECTION (LeLann-Chang-Roberts) ====================
    
    def _get_ring(self) -> List[str]:
        """Get sorted list of all active servers (including self) = the ring"""
        with self.servers_lock:
            active = [sid for sid, info in self.known_servers.items() 
                      if time.time() - info['last_heartbeat'] <= self.FAILURE_TIMEOUT]
        return sorted(active + [self.server_id])
    
    def _get_next_neighbor(self) -> Optional[str]:
        """Get next server in ring (clockwise neighbor)"""
        ring = self._get_ring()
        if len(ring) <= 1:
            return None  # Only self in ring
        
        idx = ring.index(self.server_id)
        next_idx = (idx + 1) % len(ring)
        neighbor_id = ring[next_idx]
        
        if neighbor_id == self.server_id:
            return None
        return neighbor_id

    def _initiate_election(self):
        """Start LeLann-Chang-Roberts election"""
        with self.leader_lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True
            self.election_participant = True
            self.last_election_attempt = time.time()
        
        self.logger.info("INITIATING ELECTION")
        
        neighbor = self._get_next_neighbor()
        if neighbor is None:
            # No other servers - become leader immediately
            self.logger.info("No other servers in ring - becoming leader")
            self._become_leader()
            return
        
        # Send election message with our ID as candidate
        # Using multicast but with target_id for ring semantics
        election_msg = LeaderElectionMessage(self.server_id, self.server_id, self.server_id)
        election_msg.payload['target_id'] = neighbor
        self._send_udp_multicast(election_msg)
        self.logger.info(f"Sent election message to neighbor {neighbor}")
        
        # Start timeout thread
        threading.Thread(target=self._election_timeout, daemon=True).start()

    def _handle_election_message(self, msg: Message):
        """Handle election message - LeLann-Chang-Roberts algorithm"""
        target_id = msg.payload.get('target_id')
        
        # Ring enforcement: only process if message is for us
        if target_id and target_id != self.server_id:
            return
        
        candidate_id = msg.payload['candidate_id']
        initiator_id = msg.payload['initiator_id']
        
        self.logger.info(f"Received election msg: candidate={candidate_id}, initiator={initiator_id}")
        
        # Mark that we're participating in election
        with self.leader_lock:
            self.election_in_progress = True
            self.election_participant = True
        
        neighbor = self._get_next_neighbor()
        
        if candidate_id == self.server_id:
            # Our ID came back around the ring - we are the leader!
            self.logger.info("My ID returned - I WIN the election!")
            self._become_leader()
            
        elif candidate_id > self.server_id:
            # Candidate has higher ID - forward it
            if neighbor:
                fwd_msg = LeaderElectionMessage(self.server_id, candidate_id, initiator_id)
                fwd_msg.payload['target_id'] = neighbor
                self._send_udp_multicast(fwd_msg)
                self.logger.info(f"Forwarding higher candidate {candidate_id} to {neighbor}")
            else:
                # No neighbor - the candidate should become leader
                # But we can't forward, so just acknowledge
                pass
                
        elif candidate_id < self.server_id:
            # Our ID is higher - replace candidate with our ID
            if neighbor:
                fwd_msg = LeaderElectionMessage(self.server_id, self.server_id, initiator_id)
                fwd_msg.payload['target_id'] = neighbor
                self._send_udp_multicast(fwd_msg)
                self.logger.info(f"Replacing with my higher ID {self.server_id}, sending to {neighbor}")
            else:
                # No neighbor and we're highest - become leader
                self._become_leader()

    def _become_leader(self):
        """Become the leader and announce to all servers"""
        with self.leader_lock:
            self.is_leader = True
            self.current_leader = self.server_id
            self.election_in_progress = False
            self.election_participant = False
        
        self.logger.info(f"*** I AM NOW THE LEADER ***")
        
        # Announce to all servers via multicast
        announcement = LeaderAnnouncementMessage(self.server_id, self.server_id)
        self._send_udp_multicast(announcement)

    def _handle_leader_announcement(self, msg: Message):
        """Handle leader announcement from winning server"""
        leader_id = msg.payload['leader_id']
        
        with self.leader_lock:
            self.current_leader = leader_id
            self.election_in_progress = False
            self.election_participant = False
            
            if leader_id == self.server_id:
                self.is_leader = True
                self.logger.info("Confirmed as LEADER")
            else:
                self.is_leader = False
                self.logger.info(f"Acknowledged new leader: {leader_id}")

    def _election_timeout(self):
        """Handle election timeout - become leader if no result"""
        time.sleep(self.ELECTION_TIMEOUT)
        
        with self.leader_lock:
            if self.election_in_progress and self.current_leader is None:
                # Election didn't complete - check if we should become leader
                neighbor = self._get_next_neighbor()
                if neighbor is None:
                    # We're alone - become leader
                    self.logger.info("Election timeout, no neighbors - becoming leader")
                    self.is_leader = True
                    self.current_leader = self.server_id
                    self.election_in_progress = False
                    self.election_participant = False
                    
                    # Announce
                    announcement = LeaderAnnouncementMessage(self.server_id, self.server_id)
                    self._send_udp_multicast(announcement)
                else:
                    # Still have neighbors but election didn't complete - retry
                    self.election_in_progress = False
                    self.logger.info("Election timeout - will retry")

    def _leader_monitor(self):
        """Monitor and ensure a leader exists"""
        # Wait for startup grace period
        time.sleep(self.STARTUP_GRACE_PERIOD)
        
        # Check if we should self-elect at startup
        with self.servers_lock:
            has_peers = len(self.known_servers) > 0
        
        with self.leader_lock:
            has_leader = self.current_leader is not None or self.is_leader
        
        if not has_peers and not has_leader:
            self.logger.info("Startup: alone in network - becoming leader")
            self._become_leader()
        
        # Continuous monitoring
        while self.is_running:
            time.sleep(self.HEARTBEAT_INTERVAL * 2)
            
            with self.leader_lock:
                has_leader = self.current_leader is not None or self.is_leader
                election_running = self.election_in_progress
            
            if not has_leader and not election_running:
                # No leader and no election - start one
                elapsed = time.time() - self.last_election_attempt
                if elapsed > self.HEARTBEAT_INTERVAL * 2:
                    self.logger.info("No leader detected - initiating election")
                    self._initiate_election()

    # ==================== STATUS ====================
    
    def get_status(self):
        with self.client_lock: c = len(self.clients)
        with self.servers_lock: s = len(self.known_servers)
        with self.leader_lock:
            leader_str = 'ME' if self.is_leader else (self.current_leader or 'None')
            return {
                'server_id': self.server_id,
                'clients': c, 
                'servers': s, 
                'leader': leader_str,
                'is_leader': self.is_leader,
                'election_in_progress': self.election_in_progress
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
    
    print(f"Server {server_id} running on port {tcp_port}")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            time.sleep(2)
            st = server.get_status()
            print(f"\rClients={st['clients']} | Servers={st['servers']} | Leader={st['leader']} | Election={st['election_in_progress']}   ", end='', flush=True)
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.stop()


if __name__ == '__main__':
    main()
