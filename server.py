"""
Distributed messaging server implementation.
Features:
- TCP connections for clients
- UDP multicast for server coordination + discovery
- LeLann-Chang-Roberts leader election
- Heartbeat-based fault detection
- Load balancing
- Dynamic discovery (SERVER_DISCOVERY / SERVER_ANNOUNCE)
- Client message ACKs (MESSAGE_ACK)
- FIFO ordering for inter-server forwarded messages (best-effort via seq per original_sender)
"""

import socket
import threading
import time
import struct
import logging
import uuid
from collections import defaultdict
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
    FAILURE_TIMEOUT = 6.0     # seconds (3 missed heartbeats)
    ELECTION_TIMEOUT = 5.0    # seconds

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
        self.leader_lock = threading.Lock()

        # Client management
        self.clients: Dict[str, socket.socket] = {}
        self.client_lock = threading.Lock()

        # Server discovery and fault detection
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

        # FIFO state for inter-server forwarded messages (per original_sender)
        self.last_delivered_seq = defaultdict(int)    # sender -> last seq delivered
        self.fifo_buffer = defaultdict(dict)          # sender -> {seq: payload}
        self.fifo_lock = threading.Lock()

        self.logger = logging.getLogger(f"Server-{server_id}")

    def start(self):
        """Start the server"""
        self.is_running = True

        self._setup_tcp_socket()
        self._setup_udp_socket()
        self._start_threads()

        self.logger.info(f"Server {self.server_id} started on TCP port {self.tcp_port}")

    def stop(self):
        """Stop the server"""
        self.is_running = False

        with self.client_lock:
            for _, client_socket in list(self.clients.items()):
                try:
                    client_socket.close()
                except Exception:
                    pass
            self.clients.clear()

        if self.tcp_socket:
            try:
                self.tcp_socket.close()
            except Exception:
                pass
        if self.udp_socket:
            try:
                self.udp_socket.close()
            except Exception:
                pass

        for thread in self.threads:
            try:
                thread.join(timeout=2.0)
            except Exception:
                pass

        self.logger.info(f"Server {self.server_id} stopped")

    def _setup_tcp_socket(self):
        """Setup TCP socket for client connections"""
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind(('0.0.0.0', self.tcp_port))
        self.tcp_socket.listen(self.max_clients)

    def _setup_udp_socket(self):
        """Setup UDP multicast socket for server coordination + discovery"""
        try:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            self.udp_socket.bind(('', self.MULTICAST_PORT))

            mreq = struct.pack("4sl", socket.inet_aton(self.MULTICAST_GROUP), socket.INADDR_ANY)
            self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

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
            try:
                client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            except (OSError, AttributeError):
                pass
            client_socket.settimeout(None)

            while self.is_running:
                length_data = self._recv_exactly(client_socket, 4)
                if not length_data:
                    break

                length = int.from_bytes(length_data, byteorder='big')
                if length > 1024 * 1024:
                    self.logger.warning(f"Message too large from {address}")
                    break

                msg_data = self._recv_exactly(client_socket, length)
                if not msg_data:
                    break

                msg = Message.from_json(msg_data.decode('utf-8'))

                if msg.msg_type == MessageType.CLIENT_REGISTER:
                    client_id = msg.sender_id

                    with self.client_lock:
                        if client_id in self.clients:
                            response = ServerResponse(
                                self.server_id,
                                False,
                                f"Client ID {client_id} already connected on this server"
                            )
                            self._send_message(client_socket, response)
                            break

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
            except (OSError, socket.error):
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
        """Handle client message - ACK + deliver (unicast or broadcast)"""
        content = msg.payload.get('content', '')
        recipient = msg.payload.get('recipient')
        seq = msg.payload.get('seq')  # may be None

        self.logger.info(f"Message from {msg.sender_id}: {content}")

        # Explicit ACK for chat messages
        ack = Message(
            MessageType.MESSAGE_ACK,
            self.server_id,
            {
                'acked_message_id': msg.message_id,
                'status': 'PROCESSED'
            }
        )
        self._send_message(client_socket, ack)

        if recipient:
            self._deliver_message(msg.sender_id, recipient, content, msg_id=msg.message_id, seq=seq)
        else:
            self._broadcast_message(msg.sender_id, content, forward=True, msg_id=msg.message_id, seq=seq, skip_sender=True)

    def _deliver_message(self, sender: str, recipient: str, content: str, msg_id: str = None, seq: Optional[int] = None):
        """Deliver message to recipient (local or forward via multicast)"""
        if msg_id is None:
            msg_id = str(uuid.uuid4())

        forwarded = False
        with self.client_lock:
            if recipient in self.clients:
                if recipient == sender:
                    forwarded = True
                else:
                    try:
                        out = ClientMessage(sender, content, recipient=None, seq=seq, message_id=msg_id)
                        self._send_message(self.clients[recipient], out)
                        self.logger.info(f"Delivered message from {sender} to {recipient}")
                        return
                    except (socket.error, OSError) as e:
                        self.logger.error(f"Failed to deliver to {recipient}: {e}")
                        forwarded = True
            else:
                forwarded = True

        if forwarded:
            fwd = Message(
                MessageType.FORWARD_MESSAGE,
                self.server_id,
                {
                    'original_sender': sender,
                    'recipient': recipient,
                    'content': content,
                    'msg_id': msg_id,
                    'broadcast': False,
                    'seq': seq
                }
            )
            try:
                self.udp_socket.sendto(
                    fwd.to_json().encode('utf-8'),
                    (self.MULTICAST_GROUP, self.MULTICAST_PORT)
                )
                self.logger.info(f"Forwarded message from {sender} to {recipient} via multicast")
            except (PermissionError, OSError) as e:
                self.logger.error(f"Failed to forward to {recipient}: {e}")

    def _broadcast_message(self, sender: str, content: str, forward: bool = False,
                           msg_id: str = None, seq: Optional[int] = None, skip_sender: bool = True):
        """Broadcast to all local clients; optionally forward to other servers."""
        if msg_id is None:
            msg_id = str(uuid.uuid4())

        with self.client_lock:
            targets = [cid for cid in self.clients.keys() if not (skip_sender and cid == sender)]

        for recipient in targets:
            try:
                out = ClientMessage(sender, content, recipient=None, seq=seq, message_id=msg_id)
                self._send_message(self.clients[recipient], out)
                self.logger.info(f"Broadcast from {sender} to {recipient}")
            except (socket.error, OSError) as e:
                self.logger.error(f"Failed to broadcast to {recipient}: {e}")

        if forward:
            fwd = Message(
                MessageType.FORWARD_MESSAGE,
                self.server_id,
                {
                    'original_sender': sender,
                    'recipient': None,
                    'content': content,
                    'broadcast': True,
                    'msg_id': msg_id,
                    'seq': seq
                }
            )
            try:
                self.udp_socket.sendto(
                    fwd.to_json().encode('utf-8'),
                    (self.MULTICAST_GROUP, self.MULTICAST_PORT)
                )
                self.logger.info(f"Forwarded broadcast from {sender} via multicast")
            except (PermissionError, OSError) as e:
                self.logger.error(f"Failed to forward broadcast: {e}")

    def _heartbeat_sender(self):
        """Send periodic heartbeats via UDP multicast"""
        while self.is_running:
            try:
                with self.leader_lock:
                    is_leader = self.is_leader

                heartbeat = HeartbeatMessage(
                    self.server_id,
                    self.tcp_port,
                    is_leader,
                    len(self.clients)
                )

                msg_data = heartbeat.to_json().encode('utf-8')
                try:
                    self.udp_socket.sendto(
                        msg_data,
                        (self.MULTICAST_GROUP, self.MULTICAST_PORT)
                    )
                except PermissionError:
                    pass

                time.sleep(self.HEARTBEAT_INTERVAL)
            except Exception as e:
                if self.is_running:
                    self.logger.debug(f"Heartbeat sender error: {e}")

    def _heartbeat_receiver(self):
        """Receive multicast messages from other servers + discovery requests"""
        self.udp_socket.settimeout(1.0)
        while self.is_running:
            try:
                data, address = self.udp_socket.recvfrom(4096)
                self.last_coordination_seen = time.time()
                msg = Message.from_json(data.decode('utf-8'))

                if msg.msg_type == MessageType.SERVER_DISCOVERY:
                    self._handle_server_discovery(msg, address)
                    continue

                if msg.sender_id == self.server_id:
                    continue

                if msg.msg_type == MessageType.HEARTBEAT:
                    self._handle_heartbeat(msg, address)
                elif msg.msg_type == MessageType.LEADER_ELECTION:
                    self._handle_election_message(msg)
                elif msg.msg_type == MessageType.LEADER_ANNOUNCEMENT:
                    self._handle_leader_announcement(msg)
                elif msg.msg_type == MessageType.FORWARD_MESSAGE:
                    self._handle_forward_message(msg)

            except socket.timeout:
                continue
            except Exception as e:
                if self.is_running:
                    self.logger.error(f"Error receiving multicast: {e}")

    def _handle_server_discovery(self, msg: Message, address):
        """Reply to discovery request with SERVER_ANNOUNCE via multicast."""
        requester_ip = address[0]

        with self.leader_lock:
            is_leader = self.is_leader
            current_leader = self.current_leader if self.current_leader is not None else (
                self.server_id if self.is_leader else None
            )

        announce_payload = {
            'host': requester_ip,
            'tcp_port': self.tcp_port,
            'server_id': self.server_id,
            'is_leader': is_leader,
            'current_leader': current_leader,
            'load': len(self.clients),
            'capacity': self.max_clients,
            'ts': time.time(),
        }

        announce = Message(MessageType.SERVER_ANNOUNCE, self.server_id, announce_payload)

        try:
            self.udp_socket.sendto(
                announce.to_json().encode('utf-8'),
                (self.MULTICAST_GROUP, self.MULTICAST_PORT)
            )
            self.logger.info(f"Replied to SERVER_DISCOVERY from {address[0]} with SERVER_ANNOUNCE")
        except (PermissionError, OSError) as e:
            self.logger.warning(f"Failed to send SERVER_ANNOUNCE (multicast unavailable?): {e}")

    def _handle_heartbeat(self, msg: Message, address):
        """Handle heartbeat from another server"""
        server_id = msg.sender_id

        with self.servers_lock:
            is_new = server_id not in self.known_servers
            self.known_servers[server_id] = {
                'last_heartbeat': time.time(),
                'port': msg.payload.get('server_port'),
                'is_leader': msg.payload.get('is_leader', False),
                'load': msg.payload.get('load', 0),
                'host': address[0],
            }

        if is_new:
            self.logger.info(
                f"Discovered new server via heartbeat: {server_id} @ {address[0]}:{msg.payload.get('server_port')}"
            )

        if msg.payload.get('is_leader'):
            with self.leader_lock:
                self.current_leader = server_id

    def _failure_detector(self):
        """Detect failed servers based on missed heartbeats"""
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

    def _deliver_forward_payload_inorder(self, payload: dict):
        """Deliver a forwarded payload to local clients (no forwarding here)."""
        orig_sender = payload.get('original_sender')
        recipient = payload.get('recipient')
        content = payload.get('content', '')
        msg_id = payload.get('msg_id')
        is_broadcast = payload.get('broadcast', False)
        seq = payload.get('seq')

        if is_broadcast:
            self._broadcast_message(orig_sender, content, forward=False, msg_id=msg_id, seq=seq, skip_sender=False)
            return

        if not recipient:
            return

        with self.client_lock:
            if recipient in self.clients:
                try:
                    out = ClientMessage(orig_sender, content, recipient=None, seq=seq, message_id=msg_id)
                    self._send_message(self.clients[recipient], out)
                    self.logger.info(f"Forward-delivered message from {orig_sender} to {recipient}")
                except (socket.error, OSError) as e:
                    self.logger.error(f"Failed forward delivery to {recipient}: {e}")

    def _handle_forward_message(self, msg: Message):
        """Handle forwarded client message from another server (FIFO per original_sender via seq)."""
        payload = msg.payload or {}

        orig_sender = payload.get('original_sender')
        recipient = payload.get('recipient')
        content = payload.get('content', '')
        msg_id = payload.get('msg_id')
        is_broadcast = payload.get('broadcast', False)
        seq = payload.get('seq')

        # validate minimal fields
        if not orig_sender or not msg_id:
            return
        if (not is_broadcast) and (not recipient):
            return

        # dedup
        if msg_id in self.forwarded_ids:
            return
        self.forwarded_ids.add(msg_id)

        # No seq => best-effort (no ordering across servers)
        if seq is None:
            self._deliver_forward_payload_inorder(payload)
            return

        # FIFO per original sender (across servers)
        with self.fifo_lock:
            last = self.last_delivered_seq[orig_sender]

            if seq <= last:
                return  # old/duplicate

            if seq > last + 1:
                self.fifo_buffer[orig_sender][seq] = payload
                return  # wait for gap

            # seq == last+1 -> deliver and flush buffer
            self._deliver_forward_payload_inorder(payload)
            self.last_delivered_seq[orig_sender] = seq

            next_seq = seq + 1
            while next_seq in self.fifo_buffer[orig_sender]:
                p = self.fifo_buffer[orig_sender].pop(next_seq)
                self._deliver_forward_payload_inorder(p)
                self.last_delivered_seq[orig_sender] = next_seq
                next_seq += 1

    def _ensure_leader_when_alone(self):
        """If no other servers are known and no leader is set, self-assign as leader"""
        while self.is_running:
            time.sleep(self.HEARTBEAT_INTERVAL)

            with self.leader_lock:
                no_leader = self.current_leader is None
                already_leader = self.is_leader
                election_running = self.election_in_progress

            now = time.time()
            with self.servers_lock:
                active_servers = [
                    sid for sid, info in self.known_servers.items()
                    if now - info.get('last_heartbeat', 0) <= self.FAILURE_TIMEOUT
                ]

            idle_long_enough = (now - self.last_coordination_seen) > self.FAILURE_TIMEOUT
            started_long_enough = (time.monotonic() - self.start_time_monotonic) > self.FAILURE_TIMEOUT

            if (self.is_running and not already_leader and not election_running and no_leader
                    and not active_servers and (idle_long_enough or started_long_enough)):
                self.logger.info("No active peers/leader detected; self-electing as leader")
                self._become_leader()

    def _leader_monitor(self):
        """Ensure a leader exists; if none is known and no election running, start one."""
        while self.is_running:
            time.sleep(self.HEARTBEAT_INTERVAL)

            with self.leader_lock:
                has_leader = self.current_leader is not None or self.is_leader
                election_running = self.election_in_progress
            now = time.monotonic()

            if not self.is_running:
                break

            if (not has_leader and not election_running
                    and now - self.last_election_attempt > self.HEARTBEAT_INTERVAL * 2):
                self.logger.info("No leader known; initiating election")
                self._initiate_election()

    def _initiate_election(self):
        """Initiate leader election using LeLann-Chang-Roberts algorithm"""
        with self.leader_lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True
            self.election_initiator = self.server_id
            self.last_election_attempt = time.monotonic()

        self.logger.info("Initiating leader election")

        election_msg = LeaderElectionMessage(
            self.server_id,
            self.server_id,
            self.server_id
        )

        msg_data = election_msg.to_json().encode('utf-8')
        try:
            self.udp_socket.sendto(
                msg_data,
                (self.MULTICAST_GROUP, self.MULTICAST_PORT)
            )
        except (PermissionError, OSError):
            self.logger.warning("Multicast not available, becoming leader")
            self._become_leader()
            return

        threading.Thread(target=self._election_timeout, daemon=True).start()

    def _handle_election_message(self, msg: Message):
        """Handle election message (LeLann-Chang-Roberts)"""
        candidate_id = msg.payload.get('candidate_id')
        initiator_id = msg.payload.get('initiator_id')

        if initiator_id == self.server_id and candidate_id == self.server_id:
            self._become_leader()
            return

        if candidate_id > self.server_id:
            election_msg = LeaderElectionMessage(self.server_id, candidate_id, initiator_id)
        else:
            election_msg = LeaderElectionMessage(self.server_id, self.server_id, initiator_id)

        msg_data = election_msg.to_json().encode('utf-8')
        try:
            self.udp_socket.sendto(msg_data, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
        except (PermissionError, OSError):
            pass

    def _become_leader(self):
        """Become the leader and announce"""
        with self.leader_lock:
            self.is_leader = True
            self.current_leader = self.server_id
            self.election_in_progress = False

        self.logger.info(f"Server {self.server_id} became LEADER")

        announcement = LeaderAnnouncementMessage(self.server_id, self.server_id)
        msg_data = announcement.to_json().encode('utf-8')
        try:
            self.udp_socket.sendto(msg_data, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
        except (PermissionError, OSError):
            pass

    def _handle_leader_announcement(self, msg: Message):
        """Handle leader announcement"""
        leader_id = msg.payload.get('leader_id')

        with self.leader_lock:
            self.current_leader = leader_id
            self.election_in_progress = False

            if leader_id != self.server_id:
                self.is_leader = False
                self.logger.info(f"Acknowledged {leader_id} as leader")
            else:
                self.logger.info("Confirmed as leader")

    def _election_timeout(self):
        """Handle election timeout"""
        time.sleep(self.ELECTION_TIMEOUT)

        with self.leader_lock:
            if self.election_in_progress:
                self.logger.warning("Election timeout, becoming leader")
                self.is_leader = True
                self.current_leader = self.server_id
                self.election_in_progress = False

        self.logger.info(f"Server {self.server_id} became LEADER (timeout)")

        announcement = LeaderAnnouncementMessage(self.server_id, self.server_id)
        msg_data = announcement.to_json().encode('utf-8')
        try:
            self.udp_socket.sendto(msg_data, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
        except (PermissionError, OSError):
            pass

    def get_status(self) -> dict:
        """Get server status"""
        with self.client_lock:
            num_clients = len(self.clients)

        with self.servers_lock:
            num_servers = len(self.known_servers)

        with self.leader_lock:
            is_leader = self.is_leader
            current_leader = self.current_leader
            election_in_progress = self.election_in_progress

        return {
            'server_id': self.server_id,
            'tcp_port': self.tcp_port,
            'is_leader': is_leader,
            'current_leader': current_leader,
            'connected_clients': num_clients,
            'known_servers': num_servers,
            'election_in_progress': election_in_progress
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
            time.sleep(1)
            status = server.get_status()
            print(
                f"\rStatus: Clients={status['connected_clients']}, "
                f"Servers={status['known_servers']}, "
                f"Leader={'YES' if status['is_leader'] else status['current_leader'] or 'NONE'}",
                end='',
                flush=True
            )
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.stop()


if __name__ == '__main__':
    main()
