"""
Distributed messaging server implementation.
Features:
- TCP connections for clients
- UDP multicast for server discovery & coordination
- LeLann-Chang-Roberts (LCR) leader election (Ring topology)
- Heartbeat-based fault detection

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
import json
import uuid

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

        # Globally unique identity for leader election (not lexicographic server_id)
        self.server_uuid = str(uuid.uuid4())
        self.server_uuid_int = uuid.UUID(self.server_uuid).int
        
        self.is_running = False
        self.is_leader = False
        self.current_leader: Optional[str] = None
        self.election_in_progress = False
        self.election_participant = False  # Track if we're part of an ongoing election
        
        # Thread-safe locks
        self.leader_lock = threading.RLock()
        
        self.clients: Dict[str, socket.socket] = {}
        self.client_lock = threading.RLock()
        
        # server_id -> {last_heartbeat, host, port, is_leader, server_uuid}
        self.known_servers: Dict[str, dict] = {}  
        self.servers_lock = threading.RLock()
        
        self.tcp_socket = None
        self.udp_socket = None
        self.threads: List[threading.Thread] = []
        # Detected interface IP used for multicast/unicast replies
        self.interface_ip = '0.0.0.0'

        self.last_coordination_seen = time.time()
        self.start_time = time.time()
        self.forwarded_ids: Set[str] = set()
        self.last_election_attempt = 0.0
        
        # Client sequencing: client_id -> last_seq
        self.client_seq: Dict[str, int] = {}
        self.seq_lock = threading.RLock()
        # Buffers for out-of-order messages: client_id -> {seq: Message}
        self.buffers: Dict[str, Dict[int, Message]] = {}
        self.buffers_lock = threading.RLock()

        # Cache of recently forwarded messages to support GAP requests
        # Structure: client_id -> {seq: (Message, timestamp)}
        self.resend_cache: Dict[str, Dict[int, tuple]] = {}
        self.cache_lock = threading.RLock()

        # Parameters
        self.GAP_REQUEST_DELAY = 0.5  # seconds before sending GAP request
        self.RESEND_CACHE_TTL = 60.0  # seconds to keep cached forwarded messages
        self.RESEND_CACHE_MAX = 1000
        self.BUFFER_MAX_PER_CLIENT = 500
        self.BUFFER_TTL = 60.0
        
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
        # Automatische Suche nach der richtigen LAN/WLAN IP
        target_ip = '0.0.0.0'
        try:
            # Wir holen alle IPs des Computers
            hostname = socket.gethostname()
            all_ips = socket.gethostbyname_ex(hostname)[2]
            
            # Wir filtern: Keine 127.0.0.1 und keine VirtualBox (meist 192.168.56.x)
            # Wir nehmen die erste IP, die nach "echtem" Netzwerk aussieht
            for ip in all_ips:
                if not ip.startswith("127.") and not ip.startswith("169.254."):
                    target_ip = ip
                    break
                    
            self.logger.info(f"Auto-selected Interface IP: {target_ip}")
            # Save chosen interface for later (discovery responses)
            self.interface_ip = target_ip
        except:
            target_ip = '0.0.0.0'
            self.interface_ip = target_ip

        try:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_socket.bind(('', self.MULTICAST_PORT))
            
            # Interface explizit setzen (Fix für Windows Hotspot)
            if target_ip != '0.0.0.0':
                self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(target_ip))
                # Mitgliedschaft auf diesem Interface anmelden
                mreq = struct.pack("4s4s", socket.inet_aton(self.MULTICAST_GROUP), socket.inet_aton(target_ip))
                self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            else:
                # Fallback für Standard-Verhalten
                mreq = struct.pack("4sl", socket.inet_aton(self.MULTICAST_GROUP), socket.INADDR_ANY)
                self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

            # Ensure interface_ip is set even if target_ip was 0.0.0.0
            if self.interface_ip == '0.0.0.0':
                try:
                    # try to infer a usable IP using the socket
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.connect(("8.8.8.8", 80))
                    self.interface_ip = s.getsockname()[0]
                    s.close()
                except Exception:
                    # keep as 0.0.0.0 if nothing works
                    pass

            self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            self.logger.info(f"UDP multicast enabled on {target_ip}")
        except Exception as e:
            self.logger.error(f"UDP Setup failed: {e}")

    def _start_threads(self):
        threads = [
            threading.Thread(target=self._accept_clients, daemon=True),
            threading.Thread(target=self._heartbeat_sender, daemon=True),
            threading.Thread(target=self._heartbeat_receiver, daemon=True),
            threading.Thread(target=self._failure_detector, daemon=True),
            threading.Thread(target=self._leader_monitor, daemon=True),
            threading.Thread(target=self._cache_and_buffer_cleaner, daemon=True),
        ]
        for t in threads:
            t.start()
            self.threads.append(t)
    
    
    # ==================== UUID HELPERS ====================

    @staticmethod
    def _safe_uuid_int(u: Optional[str]) -> int:
        """Parse UUID string to comparable int. Unknown UUIDs sort last."""
        try:
            return uuid.UUID(str(u)).int
        except Exception:
            return (1 << 128) - 1

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
        seq = msg.payload.get('seq')  # Extract sequence number
        # New sequencing logic: ensure in-order processing across servers
        sender = msg.sender_id
        if seq is None:
            # No seq: treat as normal immediate message
            if recipient:
                self._deliver_private_message(sender, recipient, content, msg.message_id, None)
            else:
                self._broadcast_message(sender, content, msg.message_id, None)
            # ACK immediately
            self._send_tcp(client_socket, ServerResponse(self.server_id, True, "Received", {'message_id': msg.message_id}))
            return

        # Acquire seq state
        with self.seq_lock:
            last = self.client_seq.get(sender, 0)

        if seq == last + 1:
            # In-order: process, update seq, forward and ACK
            # Update last seq
            with self.seq_lock:
                self.client_seq[sender] = seq

            # Forward / deliver
            if recipient:
                self._deliver_private_message(sender, recipient, content, msg.message_id, seq)
            else:
                self._broadcast_message(sender, content, msg.message_id, seq)

            # Cache forwarded message for potential resends
            with self.cache_lock:
                self.resend_cache.setdefault(sender, {})[seq] = (msg, time.time())
                # Evict oldest if too big
                if len(self.resend_cache[sender]) > self.RESEND_CACHE_MAX:
                    # remove smallest seq keys
                    keys = sorted(self.resend_cache[sender].keys())
                    for k in keys[:len(keys) - self.RESEND_CACHE_MAX]:
                        self.resend_cache[sender].pop(k, None)

            # ACK to client
            self._send_tcp(client_socket, ServerResponse(self.server_id, True, "Received", {'message_id': msg.message_id}))

            # Try to process buffered messages for this sender
            self._process_buffer_for_sender(sender)

        elif seq <= last:
            # Duplicate or old: ACK and ignore
            self._send_tcp(client_socket, ServerResponse(self.server_id, True, "DuplicateIgnored", {'message_id': msg.message_id}))
        else:
            # Gap detected: buffer message and schedule gap request
            with self.buffers_lock:
                b = self.buffers.setdefault(sender, {})
                if len(b) < self.BUFFER_MAX_PER_CLIENT:
                    b[seq] = msg
                else:
                    # Buffer full: drop and log
                    self.logger.warning(f"Buffer full for {sender}; dropping seq={seq}")

            # Schedule GAP request after delay
            threading.Thread(target=self._gap_request_after_delay, args=(sender, last + 1), daemon=True).start()

    def _deliver_private_message(self, sender: str, recipient: str, content: str, msg_id: str, seq: Optional[int] = None):
        """Deliver private message - local first, then forward via multicast"""
        delivered_locally = False
        
        # 1. Try local delivery first
        with self.client_lock:
            if recipient in self.clients:
                try:
                    client_msg = ClientMessage(sender, content, seq=seq)
                    self._send_tcp(self.clients[recipient], client_msg)
                    self.logger.info(f"Delivered private message from {sender} to {recipient} (local) seq={seq}")
                    delivered_locally = True
                except Exception as e:
                    self.logger.debug(f"Local delivery failed: {e}")
                    # Notify origin sender if connected here
                    try:
                        if sender in self.clients:
                            note = ServerResponse(self.server_id, False, f"Delivery to {recipient} failed", {'original_msg_id': msg_id, 'recipient': recipient, 'seq': seq})
                            self._send_tcp(self.clients[sender], note)
                    except Exception:
                        pass
        
        # 2. Always forward via multicast so other servers can deliver
        #    (recipient might be connected to another server)
        if not delivered_locally:
            fwd_msg = Message(MessageType.FORWARD_MESSAGE, self.server_id, {
                'original_sender': sender, 
                'recipient': recipient, 
                'content': content, 
                'msg_id': msg_id,
                'seq': seq,
                'broadcast': False
            })
            # Cache before sending
            if seq is not None:
                with self.cache_lock:
                    self.resend_cache.setdefault(sender, {})[seq] = (fwd_msg, time.time())
            self._send_udp_multicast(fwd_msg)
            self.logger.info(f"Forwarded private message for {recipient} via multicast seq={seq}")

    def _broadcast_message(self, sender: str, content: str, msg_id: str, seq: Optional[int] = None):
        """Broadcast message to all clients on all servers"""
        # 1. Send to all local clients (except sender)
        with self.client_lock:
            for cid, sock in self.clients.items():
                if cid != sender:
                    try:
                        client_msg = ClientMessage(sender, content, seq=seq)
                        self._send_tcp(sock, client_msg)
                        self.logger.info(f"Delivered broadcast message from {sender} to {cid} seq={seq}")
                    except Exception as e:
                        self.logger.debug(f"Broadcast delivery failed to {cid}: {e}")
                        # notify sender if connected here
                        try:
                            if sender in self.clients:
                                note = ServerResponse(self.server_id, False, f"Delivery to {cid} failed", {'recipient': cid, 'seq': seq})
                                self._send_tcp(self.clients[sender], note)
                        except Exception:
                            pass
        
        # 2. Forward to other servers via multicast
        fwd_msg = Message(MessageType.FORWARD_MESSAGE, self.server_id, {
            'original_sender': sender, 
            'recipient': None, 
            'content': content, 
            'msg_id': msg_id,
            'seq': seq,
            'broadcast': True
        })
        if seq is not None:
            with self.cache_lock:
                self.resend_cache.setdefault(sender, {})[seq] = (fwd_msg, time.time())
                if len(self.resend_cache[sender]) > self.RESEND_CACHE_MAX:
                    keys = sorted(self.resend_cache[sender].keys())
                    for k in keys[:len(keys) - self.RESEND_CACHE_MAX]:
                        self.resend_cache[sender].pop(k, None)
        self._send_udp_multicast(fwd_msg)
        self.logger.info(f"Forwarded broadcast message from {sender} via multicast seq={seq}")

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
        seq = payload.get('seq')  # Extract seq from forwarded message

        # Sequencing: ensure we only deliver in-order; buffer out-of-order
        with self.seq_lock:
            last = self.client_seq.get(sender, 0)

        if seq is None:
            # No seq: deliver immediately
            with self.client_lock:
                if is_broadcast:
                    for cid, sock in self.clients.items():
                        if cid != sender:
                            try:
                                client_msg = ClientMessage(sender, content, seq=None)
                                self._send_tcp(sock, client_msg)
                            except: pass
                else:
                    if recipient and recipient in self.clients:
                        try:
                            client_msg = ClientMessage(sender, content, seq=None)
                            self._send_tcp(self.clients[recipient], client_msg)
                            self.logger.info(f"Delivered forwarded private message to {recipient} (no-seq)")
                        except: pass
            return

        if seq == last + 1:
            # In-order: deliver and update
            with self.seq_lock:
                self.client_seq[sender] = seq
            with self.client_lock:
                if is_broadcast:
                    for cid, sock in self.clients.items():
                        if cid != sender:
                            try:
                                client_msg = ClientMessage(sender, content, seq=seq)
                                self._send_tcp(sock, client_msg)
                            except: pass
                else:
                    if recipient and recipient in self.clients:
                        try:
                            client_msg = ClientMessage(sender, content, seq=seq)
                            self._send_tcp(self.clients[recipient], client_msg)
                            self.logger.info(f"Delivered forwarded private message to {recipient} seq={seq}")
                        except: pass
                        
                            # Notify origin sender if connected here
                        try:
                                if sender in self.clients:
                                    note = ServerResponse(self.server_id, False, f"Delivery to {recipient} failed", {'recipient': recipient, 'seq': seq})
                                    self._send_tcp(self.clients[sender], note)
                        except Exception:
                                pass

            # Process any buffered subsequent messages
            self._process_buffer_for_sender(sender)

        elif seq <= last:
            # Duplicate: ignore
            return
        else:
            # Future message: buffer it and schedule gap request
            with self.buffers_lock:
                b = self.buffers.setdefault(sender, {})
                if len(b) < self.BUFFER_MAX_PER_CLIENT:
                    b[seq] = msg
                else:
                    self.logger.warning(f"Buffer full for {sender}; dropping forwarded seq={seq}")
            threading.Thread(target=self._gap_request_after_delay, args=(sender, last + 1), daemon=True).start()

    # ==================== UDP COMMUNICATION ====================
    
    def _send_udp_multicast(self, msg: Message):
        """Send message to all servers via UDP multicast"""
        try:
            data = msg.to_json().encode('utf-8')
            self.udp_socket.sendto(data, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
        except Exception as e:
            self.logger.debug(f"UDP multicast error: {e}")

    def _send_udp_unicast(self, host: str, port: int, msg: Message):
        """Send message to specific host:port via UDP (no length prefix)."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(msg.to_json().encode('utf-8'), (host, port))
            s.close()
        except Exception as e:
            self.logger.debug(f"UDP unicast error to {host}:{port}: {e}")

    # ==================== HEARTBEAT & DISCOVERY ====================

    def _heartbeat_sender(self):
        """Send heartbeat every 2 seconds to all servers via UDP multicast"""
        while self.is_running:
            try:
                with self.leader_lock: 
                    is_leader = self.is_leader
                hb = HeartbeatMessage(self.server_id, self.tcp_port, is_leader, self.server_uuid)
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
                elif msg.msg_type == MessageType.GAP_REQUEST:
                    self._handle_gap_request(msg)
                elif msg.msg_type == MessageType.GAP_RESPONSE:
                    self._handle_gap_response(msg)
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
                'is_leader': msg.payload.get('is_leader', False),
                'server_uuid': msg.payload.get('server_uuid'),
                'uuid_int': self._safe_uuid_int(msg.payload.get('server_uuid'))
            }
        
        # Update leader info if sender claims to be leader
        if msg.payload['is_leader']:
            with self.leader_lock: 
                self.current_leader = msg.sender_id

        # Optionally track last_seq per client if heartbeat carries it (not implemented here)

    def _handle_client_discovery(self, addr):
        """Handle discovery request from client.

        Node/load balancing intentionally removed: we return the current leader if known,
        otherwise this server. The client then connects via TCP using the returned host/port.
        """
        # Small delay to ensure client is listening on multicast group
        time.sleep(0.05)

        # Default: ourselves
        chosen_server_id = self.server_id
        chosen_host = self.interface_ip if self.interface_ip != '0.0.0.0' else addr[0]
        chosen_port = self.tcp_port

        # Prefer known leader if alive
        with self.leader_lock:
            leader_id = self.current_leader

        if leader_id:
            with self.servers_lock:
                info = self.known_servers.get(leader_id)
            if info and (time.time() - info.get('last_heartbeat', 0) <= self.FAILURE_TIMEOUT):
                chosen_server_id = leader_id
                chosen_host = info.get('host', chosen_host)
                chosen_port = info.get('port', chosen_port)

        resp = Message(MessageType.SERVER_ANNOUNCE, self.server_id, {
            'assigned_host': chosen_host,
            'assigned_tcp_port': chosen_port,
            'server_id': chosen_server_id
        })

        try:
            payload = resp.to_json().encode('utf-8')

            # 1) Unicast reply directly to requester (more reliable on some networks)
            try:
                self.udp_socket.sendto(payload, addr)
            except Exception as e:
                self.logger.debug(f"Unicast discovery reply failed to {addr}: {e}")

            # 2) Multicast reply for clients listening on the group
            try:
                self.udp_socket.sendto(payload, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
            except Exception as e:
                self.logger.debug(f"Multicast discovery reply failed: {e}")

            self.logger.info(
                f"Responded to client discovery with server {chosen_server_id} at {chosen_host}:{chosen_port}"
            )
        except Exception:
            pass

    # ==================== GAP / RESEND HANDLING ====================


    def _gap_request_after_delay(self, client_id: str, missing_seq: int):
        time.sleep(self.GAP_REQUEST_DELAY)
        # Check if still missing
        with self.seq_lock:
            last = self.client_seq.get(client_id, 0)
        if missing_seq <= last:
            return

        # Build GAP_REQUEST
        req = Message(MessageType.GAP_REQUEST, self.server_id, {
            'client_id': client_id,
            'missing_seq': missing_seq
        })
        # Multicast request; servers with cache will respond
        self._send_udp_multicast(req)
        self.logger.info(f"Sent GAP_REQUEST for {client_id} seq={missing_seq}")

    def _handle_gap_request(self, msg: Message):
        payload = msg.payload
        client_id = payload.get('client_id')
        missing_seq = payload.get('missing_seq')

        # If we have cached message(s), send them back directly to requester (unicast if known)
        with self.cache_lock:
            client_cache = self.resend_cache.get(client_id, {})
            entry = client_cache.get(missing_seq)

        if not entry:
            return

        fwd_msg = entry[0]

        # Try unicast to requester if we know host/port
        requester = msg.sender_id
        with self.servers_lock:
            info = self.known_servers.get(requester)

        if info:
            try:
                self._send_udp_unicast(info['host'], info['port'], fwd_msg)
                self.logger.info(f"Responded to GAP_REQUEST from {requester} for {client_id} seq={missing_seq}")
                return
            except Exception:
                pass

        # Fallback: multicast the original forward message
        self._send_udp_multicast(fwd_msg)
        self.logger.info(f"Multicasted resend for {client_id} seq={missing_seq}")

    def _handle_gap_response(self, msg: Message):
        # We accept direct forwarded messages as normal FORWARD_MESSAGE, so GAP_RESPONSE is not used here.
        pass

    def _process_buffer_for_sender(self, sender: str):
        """Iteratively flush buffered messages for a sender if next seq present."""
        while True:
            with self.seq_lock:
                last = self.client_seq.get(sender, 0)
            next_seq = last + 1
            with self.buffers_lock:
                b = self.buffers.get(sender, {})
                msg = b.get(next_seq)
                if not msg:
                    return
                # pop and process
                b.pop(next_seq, None)

            # Deliver the buffered message (it's a forwarded message object)
            payload = msg.payload
            content = payload.get('content')
            recipient = payload.get('recipient')
            is_broadcast = payload.get('broadcast', False)
            seq = payload.get('seq')

            # update last
            with self.seq_lock:
                self.client_seq[sender] = seq

            with self.client_lock:
                if is_broadcast:
                    for cid, sock in self.clients.items():
                        if cid != sender:
                            try:
                                client_msg = ClientMessage(sender, content, seq=seq)
                                self._send_tcp(sock, client_msg)
                            except: pass
                else:
                    if recipient and recipient in self.clients:
                        try:
                            client_msg = ClientMessage(sender, content, seq=seq)
                            self._send_tcp(self.clients[recipient], client_msg)
                        except: pass
                        try:
                                if sender in self.clients:
                                    note = ServerResponse(self.server_id, False, f"Delivery to {recipient} failed", {'recipient': recipient, 'seq': seq})
                                    self._send_tcp(self.clients[sender], note)
                        except Exception:
                                pass

    def _cache_and_buffer_cleaner(self):
        """Periodically evict old entries from resend_cache and buffers."""
        while self.is_running:
            now = time.time()
            # Clean cache
            with self.cache_lock:
                for client_id in list(self.resend_cache.keys()):
                    entries = self.resend_cache.get(client_id, {})
                    for seq, (m, ts) in list(entries.items()):
                        if now - ts > self.RESEND_CACHE_TTL:
                            entries.pop(seq, None)
                    if not entries:
                        self.resend_cache.pop(client_id, None)

            # Clean buffers by TTL
            with self.buffers_lock:
                for client_id in list(self.buffers.keys()):
                    buf = self.buffers.get(client_id, {})
                    # We didn't store timestamps for buffered msgs; keep by count only
                    # If buffer too large, drop oldest seqs
                    if len(buf) > self.BUFFER_MAX_PER_CLIENT:
                        keys = sorted(buf.keys())
                        for k in keys[:len(buf) - self.BUFFER_MAX_PER_CLIENT]:
                            buf.pop(k, None)
                    if not buf:
                        self.buffers.pop(client_id, None)

            time.sleep(1.0)

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
        """Get ring of all active servers (including self), ordered by server_uuid (UUID int)."""
        with self.servers_lock:
            active = [sid for sid, info in self.known_servers.items()
                      if time.time() - info.get('last_heartbeat', 0) <= self.FAILURE_TIMEOUT]

        ring = active + [self.server_id]

        def key(sid: str) -> int:
            if sid == self.server_id:
                return self.server_uuid_int
            with self.servers_lock:
                info = self.known_servers.get(sid, {})
            return int(info.get('uuid_int', (1 << 128) - 1))

        return sorted(ring, key=key)
    
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
        election_msg = LeaderElectionMessage(
            self.server_id,
            self.server_id,
            self.server_uuid,
            self.server_id,
            self.server_uuid,
        )
        election_msg.payload['target_id'] = neighbor
        self._send_udp_multicast(election_msg)
        self.logger.info(f"Sent election message to neighbor {neighbor}")
        
        # Start timeout thread
        threading.Thread(target=self._election_timeout, daemon=True).start()

    def _handle_election_message(self, msg: Message):
        """Handle election message - LeLann-Chang-Roberts algorithm (UUID-based)."""
        target_id = msg.payload.get('target_id')

        # Ring enforcement: only process if message is for us
        if target_id and target_id != self.server_id:
            return

        candidate_sid = msg.payload.get('candidate_server_id')
        candidate_uuid = msg.payload.get('candidate_uuid')
        initiator_sid = msg.payload.get('initiator_server_id')
        initiator_uuid = msg.payload.get('initiator_uuid')

        cand_int = self._safe_uuid_int(candidate_uuid)
        my_int = self.server_uuid_int

        self.logger.info(
            f"Received election msg: candidate={candidate_sid}/{candidate_uuid}, initiator={initiator_sid}/{initiator_uuid}"
        )

        # Mark that we're participating in election
        with self.leader_lock:
            self.election_in_progress = True
            self.election_participant = True

        neighbor = self._get_next_neighbor()

        if candidate_sid == self.server_id and cand_int == my_int:
            self.logger.info("My UUID returned - I WIN the election!")
            self._become_leader()
            return

        if cand_int > my_int:
            if neighbor:
                fwd_msg = LeaderElectionMessage(
                    self.server_id,
                    candidate_sid,
                    candidate_uuid,
                    initiator_sid or self.server_id,
                    initiator_uuid or self.server_uuid,
                )
                fwd_msg.payload['target_id'] = neighbor
                self._send_udp_multicast(fwd_msg)
                self.logger.info(f"Forwarding higher candidate {candidate_sid}/{candidate_uuid} to {neighbor}")
            return

        if cand_int < my_int:
            if neighbor:
                fwd_msg = LeaderElectionMessage(
                    self.server_id,
                    self.server_id,
                    self.server_uuid,
                    initiator_sid or self.server_id,
                    initiator_uuid or self.server_uuid,
                )
                fwd_msg.payload['target_id'] = neighbor
                self._send_udp_multicast(fwd_msg)
                self.logger.info(f"Replacing with my higher UUID {self.server_id}/{self.server_uuid}, sending to {neighbor}")
            else:
                self._become_leader()
            return

        # Tie-breaker: UUID collision shouldn't happen; fall back to server_id so election terminates.
        if (candidate_sid or "") > self.server_id:
            if neighbor:
                fwd_msg = LeaderElectionMessage(
                    self.server_id,
                    candidate_sid,
                    candidate_uuid,
                    initiator_sid or self.server_id,
                    initiator_uuid or self.server_uuid,
                )
                fwd_msg.payload['target_id'] = neighbor
                self._send_udp_multicast(fwd_msg)
            return

        if neighbor:
            fwd_msg = LeaderElectionMessage(
                self.server_id,
                self.server_id,
                self.server_uuid,
                initiator_sid or self.server_id,
                initiator_uuid or self.server_uuid,
            )
            fwd_msg.payload['target_id'] = neighbor
            self._send_udp_multicast(fwd_msg)
        else:
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
        announcement = LeaderAnnouncementMessage(self.server_id, self.server_id, self.server_uuid)
        self._send_udp_multicast(announcement)

    def _handle_leader_announcement(self, msg: Message):
        """Handle leader announcement from winning server"""
        leader_id = msg.payload.get('leader_server_id')
        leader_uuid = msg.payload.get('leader_uuid')
        
        with self.leader_lock:
            self.current_leader = leader_id
            self.leader_uuid = leader_uuid
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
                    announcement = LeaderAnnouncementMessage(self.server_id, self.server_id, self.server_uuid)
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
