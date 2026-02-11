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
    LeaderAnnouncementMessage, ServerResponse, ClientMessage, MessageAckMessage
)
import json
import uuid
import config  # Import centralized configuration

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class Server:
    """Distributed messaging server"""
    
    # Use configuration from config.py for consistency across all components
    MULTICAST_GROUP = config.DEFAULT_MULTICAST_GROUP
    MULTICAST_PORT = config.DEFAULT_MULTICAST_PORT
    
    # Timeouts and intervals from config (per PDF requirements)
    HEARTBEAT_INTERVAL = config.HEARTBEAT_INTERVAL
    FAILURE_TIMEOUT = config.FAILURE_TIMEOUT
    ELECTION_TIMEOUT = config.ELECTION_TIMEOUT
    
    def __init__(self, server_id: str, tcp_port: int, max_clients: int = config.MAX_CLIENTS_PER_SERVER):
        self.server_id = server_id
        self.tcp_port = tcp_port
        self.max_clients = max_clients
        self.logger = logging.getLogger(f"Server-{server_id}")

        # 1. Alle lokalen IPs sammeln (für Multicast-Empfang auf allen Interfaces)
        self.all_local_ips = self._get_all_local_ips()
        self.logger.info(f"Available Local IPs: {self.all_local_ips}")
        
        # 2. Beste IP für primäres Multicast-SENDEN wählen (keine virtuellen Adapter!)
        self.interface_ip = self._choose_primary_interface()
        self.logger.info(f"Primary interface for sending: {self.interface_ip}")

        # Globally unique identity for leader election (not lexicographic server_id)
        self.server_uuid = str(uuid.uuid4())
        self.server_uuid_int = uuid.UUID(self.server_uuid).int
        
        self.leader_uuid = None # Initialize leader_uuid
        self.is_running = False
        self.is_leader = False
        self.current_leader: Optional[str] = None
        self.election_in_progress = False
        self.election_participant = False  # Track if we're part of an ongoing election
        
        # Thread-safe locks
        self.leader_lock = threading.RLock()
        
        self.clients: Dict[str, socket.socket] = {}
        self.client_lock = threading.RLock()
        
                        # server_uuid -> {server_id, last_heartbeat, host, port, is_leader}
        self.known_servers: Dict[str, dict] = {}  
        self.servers_lock = threading.RLock()
        
        self.tcp_socket = None
        self.udp_socket = None
        self.threads: List[threading.Thread] = []
        # Detected interface IP used for multicast/unicast replies
        # self.interface_ip = '0.0.0.0' # replaced above

        self.last_coordination_seen = time.time()
        self.start_time = time.time()
        self.forwarded_ids: Set[str] = set()
        self.last_election_attempt = 0.0
        self.ever_seen_peer = False  # Track if we've EVER seen another server
        
        # Deduplication for UDP messages (received via multiple interfaces)
        self.seen_msg_ids: Set[str] = set()
        self.seen_msg_lock = threading.Lock()
        
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
            
    def _get_all_local_ips(self):
        """
        Findet ALLE Netzwerk-IPs des PCs (WLAN, Ethernet, Hotspot, etc.)
        Keine IP-Filterung! Das funktioniert mit:
        - Normalem WLAN/LAN
        - iPhone Hotspot (172.20.10.x)
        - Android Hotspot (192.168.43.x)
        - Windows Mobile Hotspot (192.168.137.x)
        - Jeder anderen Netzwerkkonfiguration
        
        Doppelte Pakete werden durch das Deduplizierungs-System (seen_msg_ids) behandelt.
        """
        ips = []
        try:
            hostname = socket.gethostname()
            _, _, ip_list = socket.gethostbyname_ex(hostname)
            for ip in ip_list:
                # NUR Loopback ignorieren - alles andere ist ein echtes Netzwerk!
                if not ip.startswith("127."):
                    ips.append(ip)
        except Exception:
            pass
            
        return ips if ips else ['127.0.0.1']
    
    def _choose_primary_interface(self) -> str:
        """
        Wählt die beste IP für primäres Multicast-Senden.
        Priorisiert echte LAN-Adapter über virtuelle (Hyper-V, VirtualBox, Docker).
        
        Priorität:
        1. 192.168.0.x - 192.168.15.x (typische Router/DHCP Ranges)
        2. 192.168.x.x (anderes LAN)
        3. 10.x.x.x (größere Netzwerke)
        4. 172.20.10.x (iPhone Hotspot - echtes Netzwerk!)
        5. 192.168.43.x (Android Hotspot)
        6. 192.168.137.x (Windows Mobile Hotspot)
        7. Alles andere außer bekannte virtuelle Adapter
        
        Bekannte virtuelle Adapter (niedrigste Priorität):
        - 192.168.56.x (VirtualBox Host-Only)
        - 172.17-31.x.x AUSSER 172.20.10.x (Docker/Hyper-V, aber nicht iPhone!)
        """
        if not self.all_local_ips:
            return '127.0.0.1'
        
        def get_priority(ip: str) -> int:
            """Niedrigere Zahl = höhere Priorität"""
            parts = ip.split('.')
            if len(parts) != 4:
                return 999
            
            first = int(parts[0])
            second = int(parts[1])
            third = int(parts[2])
            
            # VirtualBox Host-Only - sehr niedrige Priorität
            if ip.startswith("192.168.56."):
                return 900
            
            # Docker/Hyper-V (172.17-31.x) AUSSER iPhone Hotspot (172.20.10.x)
            if first == 172:
                if second == 20 and third == 10:
                    # iPhone Hotspot - gute Priorität!
                    return 50
                elif 17 <= second <= 31:
                    # Docker/Hyper-V
                    return 800
            
            # Typische Router-DHCP Ranges (192.168.0-15.x)
            if first == 192 and second == 168:
                if 0 <= third <= 15:
                    return 10  # Beste Priorität
                elif third == 43:
                    # Android Hotspot
                    return 40
                elif third == 137:
                    # Windows Mobile Hotspot
                    return 45
                else:
                    return 20  # Anderes 192.168.x.x LAN
            
            # 10.x.x.x Netzwerke (Firmen/größere LANs)
            if first == 10:
                return 30
            
            # Alles andere
            return 100
        
        # Sortiere nach Priorität und nimm die beste
        sorted_ips = sorted(self.all_local_ips, key=get_priority)
        return sorted_ips[0]
    
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
            
            # Enable receiving broadcast messages
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            
            # Auf ALLEM lauschen
            self.udp_socket.bind(('', self.MULTICAST_PORT))
            
            # WICHTIG: Der Multicast-Gruppe auf ALLEN gefundenen Interfaces beitreten
            # Das garantiert, dass wir den Ruf hören, egal ob über WLAN oder LAN
            mreq_any = struct.pack("4sl", socket.inet_aton(self.MULTICAST_GROUP), socket.INADDR_ANY)
            self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq_any)

            for ip in self.all_local_ips:
                try:
                    # Spezifischen Beitritt versuchen (wichtig für Windows)
                    mreq = struct.pack("4s4s", socket.inet_aton(self.MULTICAST_GROUP), socket.inet_aton(ip))
                    self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
                    self.logger.info(f"Listening for Multicast on {ip}")
                except Exception:
                    pass

            self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 32)

            # NEU: Explizit das Sende-Interface setzen (auf die beste IP)
            # Damit gehen Heartbeats auch wirklich ins LAN raus
            if self.interface_ip and self.interface_ip != '127.0.0.1':
                try:
                    self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.interface_ip))
                    self.logger.info(f"Sending Multicast via {self.interface_ip}")
                except Exception as e:
                    self.logger.warning(f"Failed to set Multicast Interface to {self.interface_ip}: {e}")

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
            # ACK immediately - use MESSAGE_ACK so client can clear pending
            self._send_tcp(client_socket, MessageAckMessage(self.server_id, msg.message_id, True, "Received"))
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

            # ACK to client - use MESSAGE_ACK so client can clear pending
            self._send_tcp(client_socket, MessageAckMessage(self.server_id, msg.message_id, True, "Received"))

            # Try to process buffered messages for this sender
            self._process_buffer_for_sender(sender)

        elif seq <= last:
            # Duplicate or old: ACK and ignore
            self._send_tcp(client_socket, MessageAckMessage(self.server_id, msg.message_id, True, "DuplicateIgnored"))
        else:
            # Gap detected: buffer message and schedule gap request
            with self.buffers_lock:
                b = self.buffers.setdefault(sender, {})
                if len(b) < self.BUFFER_MAX_PER_CLIENT:
                    b[seq] = msg
                    # ACK immediately even though buffered - message is safe, will be delivered when gap is filled
                    self._send_tcp(client_socket, MessageAckMessage(self.server_id, msg.message_id, True, "Buffered"))
                else:
                    # Buffer full: drop and log - do NOT ACK so client may retry
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
                'broadcast': False,
                'sender_uuid': self.server_uuid
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
            'broadcast': True,
            'sender_uuid': self.server_uuid
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
    
    def _get_subnet_broadcast(self, ip: str) -> str:
        """Berechnet die Subnet-Broadcast-Adresse (z.B. 192.168.0.118 -> 192.168.0.255)"""
        parts = ip.split('.')
        if len(parts) == 4:
            # Annahme: /24 Subnetz (typisch für Heimnetzwerke)
            return f"{parts[0]}.{parts[1]}.{parts[2]}.255"
        return '255.255.255.255'

    def _send_udp_multicast(self, msg: Message):
        """Send message to all servers via UDP multicast AND broadcast for cross-subnet reach"""
        try:
            data = msg.to_json().encode('utf-8')
            
            # 1. Send via multicast on primary interface
            self.udp_socket.sendto(data, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
            
            # 2. Try sending multicast via each known interface (for multi-homed hosts)
            for ip in self.all_local_ips:
                try:
                    # Create temporary socket for this interface
                    temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                    temp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 32)
                    temp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(ip))
                    temp_sock.sendto(data, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
                    temp_sock.close()
                except Exception:
                    pass
            
            # 3. Subnet-spezifische Broadcasts für jede Interface-IP
            # Das funktioniert besser als 255.255.255.255 zwischen verschiedenen Geräten
            broadcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            broadcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            
            sent_broadcasts = set()
            for ip in self.all_local_ips:
                try:
                    subnet_broadcast = self._get_subnet_broadcast(ip)
                    if subnet_broadcast not in sent_broadcasts:
                        broadcast_sock.sendto(data, (subnet_broadcast, self.MULTICAST_PORT))
                        sent_broadcasts.add(subnet_broadcast)
                except Exception:
                    pass
            
            # 4. Auch globalen Broadcast als letzten Fallback
            try:
                broadcast_sock.sendto(data, ('255.255.255.255', self.MULTICAST_PORT))
            except Exception:
                pass
            
            broadcast_sock.close()
                
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
                
                # Ignore own messages - use UUID for reliable identification
                # msg.sender_id is just a name and can be duplicated!
                sender_uuid = msg.payload.get('sender_uuid') or msg.payload.get('server_uuid')
                if sender_uuid == self.server_uuid:
                    continue
                
                # Fallback for messages without UUID (e.g., client discovery)
                # But this is less reliable if two servers have same name
                if sender_uuid is None and msg.sender_id == self.server_id:
                    continue
                
                # Deduplicate messages received via multiple interfaces
                # Use message_id if available, otherwise create a hash from content
                dedup_key = msg.message_id
                if dedup_key:
                    with self.seen_msg_lock:
                        if dedup_key in self.seen_msg_ids:
                            continue  # Already processed this message
                        self.seen_msg_ids.add(dedup_key)
                        # Cleanup: keep only last 500 message IDs
                        if len(self.seen_msg_ids) > 1000:
                            self.seen_msg_ids = set(list(self.seen_msg_ids)[-500:])
                
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
        """Process heartbeat from another server - updates known_servers (keyed by UUID)"""
        sender_uuid = msg.payload.get('server_uuid')
        if not sender_uuid: return

        # Mark that we've seen at least one peer - critical for bootstrap logic
        self.ever_seen_peer = True

        with self.servers_lock:
            self.known_servers[sender_uuid] = {
                'server_id': msg.sender_id,
                'last_heartbeat': time.time(),
                'host': ip,
                'port': msg.payload['server_port'],
                'is_leader': msg.payload.get('is_leader', False),
                'uuid_int': self._safe_uuid_int(sender_uuid)
            }
        
        # Update leader info if sender claims to be leader
        if msg.payload.get('is_leader', False):
            with self.leader_lock:
                # Accept the existing leader - CRITICAL for LCR correctness
                # If someone is already leader, we must accept them and NOT start a new election
                self.current_leader = msg.sender_id
                self.leader_uuid = sender_uuid
                
                # If we were in an election, cancel it - leader already exists
                if self.election_in_progress:
                    self.logger.info(f"Cancelling election - leader {msg.sender_id} already exists")
                    self.election_in_progress = False
                    self.election_participant = False
                
                # If we thought we were leader but someone else claims leadership with higher UUID, yield
                if self.is_leader and sender_uuid != self.server_uuid:
                    sender_uuid_int = self._safe_uuid_int(sender_uuid)
                    if sender_uuid_int > self.server_uuid_int:
                        self.logger.info(f"Yielding leadership to {msg.sender_id} (higher UUID)")
                        self.is_leader = False

    def _handle_client_discovery(self, addr):
        """
        Handle discovery request from client.
        ALLE Server antworten auf Discovery-Requests!
        
        Begründung: Wenn der Leader auf einem anderen Gerät ist (z.B. anderer Laptop),
        könnte Multicast zwischen den Geräten nicht zuverlässig funktionieren.
        Daher antwortet JEDER Server mit seinen eigenen Daten.
        Der Client nimmt die erste gültige Antwort.
        """
        # Small delay to avoid packet collision + ensure client is listening
        time.sleep(0.05)

        client_ip = addr[0]
        
        # Standard: Wir nehmen unsere Haupt-IP
        chosen_host = self.interface_ip
        
        # INTELLIGENZ: Welche unserer IPs passt am besten zur Client-IP?
        # Wir vergleichen die ersten 3 Blöcke (z.B. "192.168.178")
        best_match_score = -1
        client_parts = client_ip.split('.')
        
        for my_ip in self.all_local_ips:
            score = 0
            my_parts = my_ip.split('.')
            # Zähle wie viele Teile am Anfang übereinstimmen
            for i in range(min(len(client_parts), len(my_parts))):
                if client_parts[i] == my_parts[i]:
                    score += 1
                else:
                    break
            
            if score > best_match_score:
                best_match_score = score
                chosen_host = my_ip
        
        chosen_port = self.tcp_port
        chosen_server_id = self.server_id
        
        # Include leader info so client knows the cluster state
        with self.leader_lock:
            is_leader = self.is_leader
            leader_id = self.current_leader

        resp = Message(MessageType.SERVER_ANNOUNCE, self.server_id, {
            'assigned_host': chosen_host,
            'assigned_tcp_port': chosen_port,
            'server_id': chosen_server_id,
            'is_leader': is_leader,
            'current_leader': leader_id
        })

        try:
            payload = resp.to_json().encode('utf-8')

            # 1) Unicast reply directly to requester (most reliable)
            try:
                self.udp_socket.sendto(payload, addr)
                self.logger.debug(f"Discovery: Unicast reply to {addr}")
            except Exception as e:
                self.logger.debug(f"Unicast discovery reply failed to {addr}: {e}")

            # 2) Multicast reply 
            try:
                self.udp_socket.sendto(payload, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
            except Exception as e:
                self.logger.debug(f"Multicast discovery reply failed: {e}")

            # 3) Broadcast replies (same approach as heartbeats - for cross-device reach)
            try:
                broadcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                broadcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                
                # Subnet-specific broadcast based on client IP
                client_subnet_broadcast = self._get_subnet_broadcast(client_ip)
                broadcast_sock.sendto(payload, (client_subnet_broadcast, self.MULTICAST_PORT))
                
                # Also our own subnet broadcasts
                for ip in self.all_local_ips:
                    subnet_broadcast = self._get_subnet_broadcast(ip)
                    if subnet_broadcast != client_subnet_broadcast:
                        try:
                            broadcast_sock.sendto(payload, (subnet_broadcast, self.MULTICAST_PORT))
                        except:
                            pass
                
                # Global broadcast fallback
                broadcast_sock.sendto(payload, ('255.255.255.255', self.MULTICAST_PORT))
                broadcast_sock.close()
            except Exception as e:
                self.logger.debug(f"Broadcast discovery reply failed: {e}")

            leader_status = "LEADER" if is_leader else f"follower (leader={leader_id})"
            self.logger.info(
                f"Responding to client discovery [{leader_status}]: {chosen_server_id} at {chosen_host}:{chosen_port}"
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
            'missing_seq': missing_seq,
            'sender_uuid': self.server_uuid
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

        # Try unicast to requester if we know host/port - use UUID as key!
        requester_uuid = msg.payload.get('sender_uuid')
        requester_id = msg.sender_id
        info = None
        if requester_uuid:
            with self.servers_lock:
                info = self.known_servers.get(requester_uuid)

        if info:
            try:
                self._send_udp_unicast(info['host'], info['port'], fwd_msg)
                self.logger.info(f"Responded to GAP_REQUEST from {requester_id} for {client_id} seq={missing_seq}")
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
            failed_uuids = []
            
            with self.servers_lock:
                for uuid_str, info in list(self.known_servers.items()):
                    if current_time - info['last_heartbeat'] > self.FAILURE_TIMEOUT:
                        failed_uuids.append(uuid_str)
                        del self.known_servers[uuid_str]
            
            if failed_uuids:
                self.logger.warning(f"Detected failed servers (UUIDs): {failed_uuids}")
                
                # Check if leader failed - trigger re-election
                with self.leader_lock:
                    # current_leader speichert jetzt UUID, nicht ID
                    if self.leader_uuid in failed_uuids:
                        self.logger.warning("LEADER FAILED - initiating re-election")
                        self.current_leader = None
                        self.leader_uuid = None
                        self.is_leader = False
                        self.election_in_progress = False
                
                # Always try to initiate election when leader fails
                with self.leader_lock:
                    if self.leader_uuid is None and not self.is_leader:
                        self._initiate_election()

    # ==================== LEADER ELECTION (LeLann-Chang-Roberts) ====================
    
    def _get_ring(self) -> List[str]:
        """Get ring of all active servers (including self), ordered by server_uuid (UUID int)."""
        with self.servers_lock:
            # Ring enthält jetzt UUIDs, nicht mehr Namen
            active_uuids = [uuid for uuid, info in self.known_servers.items()
                            if time.time() - info.get('last_heartbeat', 0) <= self.FAILURE_TIMEOUT]

        ring = active_uuids + [self.server_uuid]

        def key(u: str) -> int:
            return self._safe_uuid_int(u)

        return sorted(ring, key=key)
    
    def _get_next_neighbor(self) -> Optional[str]:
        """Get next server UUID in ring (clockwise neighbor)"""
        ring = self._get_ring()
        if len(ring) <= 1:
            return None  # Only self in ring
        
        try:
            idx = ring.index(self.server_uuid)
            next_idx = (idx + 1) % len(ring)
            neighbor_uuid = ring[next_idx]
            
            if neighbor_uuid == self.server_uuid:
                return None
            return neighbor_uuid
        except ValueError:
            return None

    def _initiate_election(self):
        """Start LeLann-Chang-Roberts election - only if no active leader exists"""
        
        # CRITICAL: Check if there's already a known leader that is still alive
        with self.leader_lock:
            if self.is_leader:
                # We're already the leader, no election needed
                return
            
            if self.leader_uuid is not None:
                # Check if the known leader is still alive
                with self.servers_lock:
                    leader_info = self.known_servers.get(self.leader_uuid)
                    if leader_info:
                        if time.time() - leader_info.get('last_heartbeat', 0) <= self.FAILURE_TIMEOUT:
                            # Leader is still alive, don't start election
                            self.logger.debug(f"Leader {self.current_leader} still alive, skipping election")
                            return
        
        # Also check if any server in our known_servers claims to be leader
        with self.servers_lock:
            for info in self.known_servers.values():
                if info.get('is_leader', False):
                    if time.time() - info.get('last_heartbeat', 0) <= self.FAILURE_TIMEOUT:
                        # A live leader exists, don't start election
                        self.logger.debug(f"Server {info.get('server_id')} claims leadership, skipping election")
                        return
        
        with self.leader_lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True
            self.election_participant = True
            self.last_election_attempt = time.time()
        
        self.logger.info("INITIATING ELECTION")
        
        neighbor_uuid = self._get_next_neighbor()
        if neighbor_uuid is None:
            # No other servers - become leader immediately
            self.logger.info("No other servers in ring - becoming leader")
            self._become_leader()
            return
        
        # We need the server_id of the neighbor to send the message (for logging mainly, multicast goes to all)
        neighbor_id = "unknown"
        with self.servers_lock:
            if neighbor_uuid in self.known_servers:
                neighbor_id = self.known_servers[neighbor_uuid].get('server_id', 'unknown')

        # Send election message with our ID as candidate
        # Using multicast but with target_id (UUID) for ring semantics
        election_msg = LeaderElectionMessage(
            self.server_id,
            self.server_id,
            self.server_uuid,
            self.server_id,
            self.server_uuid,
        )
        election_msg.payload['target_id'] = neighbor_id # Legacy: target_id was used for name matching
        election_msg.payload['target_uuid'] = neighbor_uuid # NEW: target_uuid for precise matching
        election_msg.payload['sender_uuid'] = self.server_uuid  # For self-identification in receiver
        
        self._send_udp_multicast(election_msg)
        self.logger.info(f"Sent election message to neighbor {neighbor_id} ({neighbor_uuid})")
        
        # Start timeout thread
        threading.Thread(target=self._election_timeout, daemon=True).start()

    def _handle_election_message(self, msg: Message):
        """Handle election message - LeLann-Chang-Roberts algorithm (UUID-based)."""
        target_uuid = msg.payload.get('target_uuid')
        target_id = msg.payload.get('target_id')

        # Ring enforcement: check if message is for us (by UUID or legacy ID fallback)
        if target_uuid:
            if target_uuid != self.server_uuid:
                return
        elif target_id and target_id != self.server_id:
            return

        candidate_sid = msg.payload.get('candidate_server_id')
        candidate_uuid = msg.payload.get('candidate_uuid')
        initiator_sid = msg.payload.get('initiator_server_id')
        initiator_uuid = msg.payload.get('initiator_uuid')

        cand_int = self._safe_uuid_int(candidate_uuid)
        my_int = self.server_uuid_int

        self.logger.info(
            f"Received election msg: candidate={candidate_sid}/{candidate_uuid}"
        )

        # Mark that we're participating in election
        with self.leader_lock:
            self.election_in_progress = True
            self.election_participant = True

        neighbor_uuid = self._get_next_neighbor()
        neighbor_id = "unknown"
        if neighbor_uuid:
            with self.servers_lock:
                if neighbor_uuid in self.known_servers:
                    neighbor_id = self.known_servers[neighbor_uuid].get('server_id', 'unknown')

        if candidate_uuid == self.server_uuid:
            self.logger.info("My UUID returned - I WIN the election!")
            self._become_leader()
            return

        if cand_int > my_int:
            if neighbor_uuid:
                fwd_msg = LeaderElectionMessage(
                    self.server_id,
                    candidate_sid,
                    candidate_uuid,
                    initiator_sid or self.server_id,
                    initiator_uuid or self.server_uuid,
                )
                fwd_msg.payload['target_id'] = neighbor_id
                fwd_msg.payload['target_uuid'] = neighbor_uuid
                fwd_msg.payload['sender_uuid'] = self.server_uuid
                self._send_udp_multicast(fwd_msg)
                self.logger.info(f"Forwarding higher candidate {candidate_sid} to {neighbor_id}")
            return

        if cand_int < my_int:
            if neighbor_uuid:
                fwd_msg = LeaderElectionMessage(
                    self.server_id,
                    self.server_id,
                    self.server_uuid,
                    initiator_sid or self.server_id,
                    initiator_uuid or self.server_uuid,
                )
                fwd_msg.payload['target_id'] = neighbor_id
                fwd_msg.payload['target_uuid'] = neighbor_uuid
                fwd_msg.payload['sender_uuid'] = self.server_uuid
                self._send_udp_multicast(fwd_msg)
                self.logger.info(f"Replacing with my higher UUID {self.server_id}, sending to {neighbor_id}")
            else:
                self._become_leader()
            return
        
        # Note: cand_int == my_int case cannot happen with UUIDs (they are unique)

    def _become_leader(self):
        """Become the leader and announce to all servers"""
        with self.leader_lock:
            self.is_leader = True
            self.current_leader = self.server_id
            self.leader_uuid = self.server_uuid  # Set our own UUID as leader UUID
            self.election_in_progress = False
            self.election_participant = False
        
        self.logger.info(f"*** I AM NOW THE LEADER ***")
        
        # Announce to all servers via multicast
        announcement = LeaderAnnouncementMessage(self.server_id, self.server_id, self.server_uuid)
        announcement.payload['sender_uuid'] = self.server_uuid
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
        """Handle election timeout - become leader if no result AND no existing leader"""
        time.sleep(self.ELECTION_TIMEOUT)
        
        # First check if any server claims to be leader (outside the lock)
        with self.servers_lock:
            for info in self.known_servers.values():
                if info.get('is_leader', False):
                    if time.time() - info.get('last_heartbeat', 0) <= self.FAILURE_TIMEOUT:
                        # A live leader exists, cancel election
                        with self.leader_lock:
                            self.election_in_progress = False
                            self.election_participant = False
                        self.logger.info(f"Election timeout cancelled - leader {info.get('server_id')} found")
                        return
        
        with self.leader_lock:
            # If a leader was already established during the election, don't override
            if self.leader_uuid is not None:
                self.election_in_progress = False
                self.election_participant = False
                self.logger.info("Election timeout - leader already established")
                return
            
            # Check UUID based leader
            if self.election_in_progress:
                # Election didn't complete - check if we should become leader
                neighbor_uuid = self._get_next_neighbor()
                if neighbor_uuid is None:
                    # We're alone - become leader
                    self.logger.info("Election timeout, no neighbors - becoming leader")
                    self.is_leader = True
                    self.current_leader = self.server_id
                    self.leader_uuid = self.server_uuid
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
        """Monitor and ensure a leader exists - triggers election when needed"""
        # Initial wait: give time to discover existing servers and leaders
        # Wait for a few heartbeat cycles to discover the network
        initial_wait = self.HEARTBEAT_INTERVAL * 3  # 6 seconds - enough to receive 2-3 heartbeats
        time.sleep(initial_wait)

        # Track when we last tried to start an election (to avoid spamming)
        last_election_trigger = 0.0

        # Continuous monitoring
        while self.is_running:
            time.sleep(self.HEARTBEAT_INTERVAL)
            
            with self.leader_lock:
                has_leader = self.leader_uuid is not None or self.is_leader
                election_running = self.election_in_progress
            
            with self.servers_lock:
                has_peers = len(self.known_servers) > 0
                # Check if any known server claims to be leader
                any_peer_is_leader = any(info.get('is_leader', False) for info in self.known_servers.values())
            
            # If any peer claims to be leader, accept them (don't start election)
            if any_peer_is_leader:
                continue
            
            # If we already have a leader, nothing to do
            if has_leader:
                continue
            
            # If election is already running, wait for it
            if election_running:
                continue
            
            # Cooldown: don't spam elections (wait at least ELECTION_TIMEOUT between attempts)
            if time.time() - last_election_trigger < self.ELECTION_TIMEOUT:
                continue
            
            # Fall A: Peers exist but no leader -> start election immediately
            # This is the normal case when multiple servers start together
            if has_peers and not has_leader:
                self.logger.info("Peers exist but no Leader detected - initiating election")
                last_election_trigger = time.time()
                self._initiate_election()
            
            # Fall B: We're alone (no peers) - become leader after waiting
            # Only if we've never seen any peer (true first-start scenario)
            elif not has_peers and not has_leader:
                elapsed_since_start = time.time() - self.start_time
                
                # Conservative: only bootstrap if we've been running for 8+ seconds
                # AND we've never seen another peer
                if elapsed_since_start > (self.HEARTBEAT_INTERVAL * 4) and not self.ever_seen_peer:
                    self.logger.info("No peers ever detected (true isolation) - becoming leader (Bootstrap)")
                    self._become_leader()

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
