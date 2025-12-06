"""
Distributed messaging server implementation.
Features:
- TCP connections for clients
- UDP multicast for server coordination
- LeLann-Chang-Roberts leader election
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
    
    # Multicast configuration
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
        
        # Client management
        self.clients: Dict[str, socket.socket] = {}
        self.client_lock = threading.Lock()
        
        # Server discovery and fault detection
        self.known_servers: Dict[str, dict] = {}  # server_id -> {last_heartbeat, port, is_leader, load}
        self.servers_lock = threading.Lock()
        
        # Sockets
        self.tcp_socket = None
        self.udp_socket = None
        
        # Threads
        self.threads: List[threading.Thread] = []
        
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
            thread.join(timeout=2.0)
        
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
            # Create a dummy socket that won't actually work for multicast
            # but won't crash the application
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.bind(('127.0.0.1', 0))  # Bind to localhost on random port
    
    def _start_threads(self):
        """Start all background threads"""
        threads = [
            threading.Thread(target=self._accept_clients, daemon=True),
            threading.Thread(target=self._heartbeat_sender, daemon=True),
            threading.Thread(target=self._heartbeat_receiver, daemon=True),
            threading.Thread(target=self._failure_detector, daemon=True),
        ]
        
        for thread in threads:
            thread.start()
            self.threads.append(thread)
    
    def _accept_clients(self):
        """Accept incoming client connections (TCP)"""
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
        """Handle individual client connection"""
        client_id = None
        try:
            client_socket.settimeout(30.0)
            
            while self.is_running:
                # Receive message length prefix
                length_data = self._recv_exactly(client_socket, 4)
                if not length_data:
                    break
                
                length = int.from_bytes(length_data, byteorder='big')
                if length > 1024 * 1024:  # 1MB limit
                    self.logger.warning(f"Message too large from {address}")
                    break
                
                # Receive message data
                msg_data = self._recv_exactly(client_socket, length)
                if not msg_data:
                    break
                
                # Parse message
                msg = Message.from_json(msg_data.decode('utf-8'))
                
                # Handle different message types
                if msg.msg_type == MessageType.CLIENT_REGISTER:
                    client_id = msg.sender_id
                    with self.client_lock:
                        self.clients[client_id] = client_socket
                    response = ServerResponse(
                        self.server_id,
                        True,
                        f"Registered as {client_id}",
                        {'server_id': self.server_id}
                    )
                    self._send_message(client_socket, response)
                    self.logger.info(f"Client {client_id} registered from {address}")
                
                elif msg.msg_type == MessageType.CLIENT_MESSAGE:
                    self._handle_client_message(msg, client_socket)
                
                elif msg.msg_type == MessageType.CLIENT_UNREGISTER:
                    break
        
        except Exception as e:
            self.logger.error(f"Error handling client {client_id}: {e}")
        
        finally:
            if client_id:
                with self.client_lock:
                    self.clients.pop(client_id, None)
                self.logger.info(f"Client {client_id} disconnected")
            try:
                client_socket.close()
            except:
                pass
    
    def _recv_exactly(self, sock: socket.socket, n: int) -> Optional[bytes]:
        """Receive exactly n bytes from socket"""
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data
    
    def _send_message(self, sock: socket.socket, msg: Message):
        """Send message through socket"""
        try:
            sock.sendall(msg.to_bytes())
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
    
    def _handle_client_message(self, msg: Message, client_socket: socket.socket):
        """Handle client message - store and forward to recipient if specified"""
        content = msg.payload.get('content', '')
        recipient = msg.payload.get('recipient')
        
        self.logger.info(f"Message from {msg.sender_id}: {content}")
        
        # Send acknowledgment
        response = ServerResponse(
            self.server_id,
            True,
            "Message received",
            {'message_id': msg.message_id}
        )
        self._send_message(client_socket, response)
        
        # If recipient specified, try to deliver
        if recipient:
            self._deliver_message(msg.sender_id, recipient, content)
    
    def _deliver_message(self, sender: str, recipient: str, content: str):
        """Deliver message to recipient (local or via another server)"""
        with self.client_lock:
            if recipient in self.clients:
                # Local delivery
                try:
                    msg = ClientMessage(sender, content)
                    self._send_message(self.clients[recipient], msg)
                    self.logger.info(f"Delivered message from {sender} to {recipient}")
                except Exception as e:
                    self.logger.error(f"Failed to deliver to {recipient}: {e}")
            else:
                # Message will be queued or forwarded
                self.logger.info(f"Recipient {recipient} not connected to this server")
    
    def _heartbeat_sender(self):
        """Send periodic heartbeats via UDP multicast"""
        while self.is_running:
            try:
                heartbeat = HeartbeatMessage(
                    self.server_id,
                    self.tcp_port,
                    self.is_leader,
                    len(self.clients)
                )
                
                msg_data = heartbeat.to_json().encode('utf-8')
                try:
                    self.udp_socket.sendto(
                        msg_data,
                        (self.MULTICAST_GROUP, self.MULTICAST_PORT)
                    )
                except PermissionError:
                    # Multicast not available in this environment
                    pass
                
                time.sleep(self.HEARTBEAT_INTERVAL)
            except Exception as e:
                if self.is_running:
                    self.logger.debug(f"Heartbeat sender error: {e}")
    
    def _heartbeat_receiver(self):
        """Receive heartbeats from other servers via UDP multicast"""
        self.udp_socket.settimeout(1.0)
        while self.is_running:
            try:
                data, address = self.udp_socket.recvfrom(4096)
                msg = Message.from_json(data.decode('utf-8'))
                
                # Ignore own messages
                if msg.sender_id == self.server_id:
                    continue
                
                # Handle different message types
                if msg.msg_type == MessageType.HEARTBEAT:
                    self._handle_heartbeat(msg)
                elif msg.msg_type == MessageType.LEADER_ELECTION:
                    self._handle_election_message(msg)
                elif msg.msg_type == MessageType.LEADER_ANNOUNCEMENT:
                    self._handle_leader_announcement(msg)
            
            except socket.timeout:
                continue
            except Exception as e:
                if self.is_running:
                    self.logger.error(f"Error receiving multicast: {e}")
    
    def _handle_heartbeat(self, msg: Message):
        """Handle heartbeat from another server"""
        server_id = msg.sender_id
        
        with self.servers_lock:
            self.known_servers[server_id] = {
                'last_heartbeat': time.time(),
                'port': msg.payload.get('server_port'),
                'is_leader': msg.payload.get('is_leader', False),
                'load': msg.payload.get('load', 0)
            }
            
            # Update leader information
            if msg.payload.get('is_leader'):
                self.current_leader = server_id
    
    def _failure_detector(self):
        """Detect failed servers based on missed heartbeats"""
        while self.is_running:
            time.sleep(self.HEARTBEAT_INTERVAL)
            
            current_time = time.time()
            failed_servers = []
            
            with self.servers_lock:
                for server_id, info in list(self.known_servers.items()):
                    if current_time - info['last_heartbeat'] > self.FAILURE_TIMEOUT:
                        failed_servers.append(server_id)
                        del self.known_servers[server_id]
            
            if failed_servers:
                self.logger.warning(f"Detected failed servers: {failed_servers}")
                
                # If leader failed, start election
                if self.current_leader in failed_servers:
                    self.logger.warning("Leader failed, initiating election")
                    self.current_leader = None
                    self.is_leader = False
                    self._initiate_election()
    
    def _initiate_election(self):
        """Initiate leader election using LeLann-Chang-Roberts algorithm"""
        if self.election_in_progress:
            return
        
        self.election_in_progress = True
        self.election_initiator = self.server_id
        
        self.logger.info("Initiating leader election")
        
        # Send election message with own ID as candidate
        election_msg = LeaderElectionMessage(
            self.server_id,
            self.server_id,  # candidate_id
            self.server_id   # initiator_id
        )
        
        msg_data = election_msg.to_json().encode('utf-8')
        try:
            self.udp_socket.sendto(
                msg_data,
                (self.MULTICAST_GROUP, self.MULTICAST_PORT)
            )
        except (PermissionError, OSError):
            # Multicast not available, become leader immediately
            self.logger.warning("Multicast not available, becoming leader")
            self._become_leader()
            return
        
        # Start election timeout
        threading.Thread(target=self._election_timeout, daemon=True).start()
    
    def _handle_election_message(self, msg: Message):
        """Handle election message (LeLann-Chang-Roberts)"""
        candidate_id = msg.payload.get('candidate_id')
        initiator_id = msg.payload.get('initiator_id')
        
        # If message returned to initiator with own ID, we're the leader
        if initiator_id == self.server_id and candidate_id == self.server_id:
            self._become_leader()
            return
        
        # If candidate ID is greater than ours, forward it
        if candidate_id > self.server_id:
            election_msg = LeaderElectionMessage(
                self.server_id,
                candidate_id,
                initiator_id
            )
            msg_data = election_msg.to_json().encode('utf-8')
            try:
                self.udp_socket.sendto(
                    msg_data,
                    (self.MULTICAST_GROUP, self.MULTICAST_PORT)
                )
            except (PermissionError, OSError):
                pass
        # If our ID is greater, substitute it
        elif self.server_id > candidate_id:
            election_msg = LeaderElectionMessage(
                self.server_id,
                self.server_id,
                initiator_id
            )
            msg_data = election_msg.to_json().encode('utf-8')
            try:
                self.udp_socket.sendto(
                    msg_data,
                    (self.MULTICAST_GROUP, self.MULTICAST_PORT)
                )
            except (PermissionError, OSError):
                pass
    
    def _become_leader(self):
        """Become the leader and announce"""
        self.is_leader = True
        self.current_leader = self.server_id
        self.election_in_progress = False
        
        self.logger.info(f"Server {self.server_id} became LEADER")
        
        # Announce leadership
        announcement = LeaderAnnouncementMessage(self.server_id, self.server_id)
        msg_data = announcement.to_json().encode('utf-8')
        try:
            self.udp_socket.sendto(
                msg_data,
                (self.MULTICAST_GROUP, self.MULTICAST_PORT)
            )
        except (PermissionError, OSError):
            pass
    
    def _handle_leader_announcement(self, msg: Message):
        """Handle leader announcement"""
        leader_id = msg.payload.get('leader_id')
        self.current_leader = leader_id
        self.election_in_progress = False
        
        if leader_id != self.server_id:
            self.is_leader = False
            self.logger.info(f"Acknowledged {leader_id} as leader")
    
    def _election_timeout(self):
        """Handle election timeout"""
        time.sleep(self.ELECTION_TIMEOUT)
        
        if self.election_in_progress:
            # If still in election, become leader by default
            self.logger.warning("Election timeout, becoming leader")
            self._become_leader()
    
    def get_status(self) -> dict:
        """Get server status"""
        with self.client_lock:
            num_clients = len(self.clients)
        
        with self.servers_lock:
            num_servers = len(self.known_servers)
        
        return {
            'server_id': self.server_id,
            'tcp_port': self.tcp_port,
            'is_leader': self.is_leader,
            'current_leader': self.current_leader,
            'connected_clients': num_clients,
            'known_servers': num_servers,
            'election_in_progress': self.election_in_progress
        }


def main():
    """Main entry point for server"""
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
            time.sleep(1)
            status = server.get_status()
            print(f"\rStatus: Clients={status['connected_clients']}, "
                  f"Servers={status['known_servers']}, "
                  f"Leader={'YES' if status['is_leader'] else status['current_leader'] or 'NONE'}", 
                  end='', flush=True)
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.stop()


if __name__ == '__main__':
    main()
