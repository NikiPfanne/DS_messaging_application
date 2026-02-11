"""
Distributed messaging client implementation.
Features:
- TCP connection to server
- Interactive CLI (send broadcast/private, quit)
- Receive messages asynchronously
- Per-client sequencing (seq) for FIFO (useful for forwarded ordering)
- Explicit MESSAGE_ACK handling (server confirms processing)
- Automatic reconnect on TCP failure (BrokenPipe/ConnectionReset)
- UDP multicast discovery (SERVER_DISCOVERY / SERVER_ANNOUNCE) to get assigned server from leader
- Resend of unacknowledged (pending) messages after reconnect (same message_id + seq)
"""

import socket
import threading
import logging
import time
import uuid
import struct
from typing import Optional, Dict, Tuple, List

from protocol import Message, MessageType, ClientMessage
import config  # Import centralized configuration

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class Client:
    # Use configuration from config.py for consistency
    MULTICAST_GROUP = config.DEFAULT_MULTICAST_GROUP
    MULTICAST_PORT = config.DEFAULT_MULTICAST_PORT

    DISCOVERY_TIMEOUT = config.UDP_TIMEOUT * 2  # seconds to wait for SERVER_ANNOUNCE
    RECONNECT_BACKOFF = 1.0                     # seconds between reconnect attempts
    MAX_MESSAGE_SIZE = config.MAX_MESSAGE_SIZE

    def __init__(self, client_id: str, server_host: str, server_port: int):
        self.client_id = client_id
        self.server_host = server_host
        self.server_port = server_port

        self.sock: Optional[socket.socket] = None
        self.sock_lock = threading.Lock()

        self.is_running = False

        # Sequencing for FIFO per sender
        self.seq = 0

        # message_id -> {content, recipient, seq, ts}
        self.pending: Dict[str, dict] = {}
        self.pending_lock = threading.Lock()

        self.receiver_thread: Optional[threading.Thread] = None
        self.reconnect_lock = threading.Lock()

        self.logger = logging.getLogger(f"Client-{client_id}")

    # ----------------------------
    # Connection / lifecycle
    # ----------------------------
    def connect(self, start_receiver: bool = True):
        """Connect to server and register. Does NOT start a new receiver thread unless start_receiver=True."""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.server_host, self.server_port))
        s.settimeout(None)

        with self.sock_lock:
            # close old socket if any
            if self.sock:
                try:
                    self.sock.close()
                except Exception:
                    pass
            self.sock = s

        # Register
        reg = Message(MessageType.CLIENT_REGISTER, self.client_id, {})
        self._send_raw(reg)

        # Wait for server response (registration)
        resp = self._recv_message()
        if not resp:
            raise RuntimeError("No response from server during registration")

        if resp.msg_type == MessageType.SERVER_RESPONSE:
            ok = resp.payload.get("success", False)
            msg = resp.payload.get("message", "")
            if not ok:
                raise RuntimeError(f"Registration failed: {msg}")
            self.logger.info(f"Connected to server: {msg}")
            print(f"Connected as {self.client_id}")
        else:
            self.logger.info(f"Connected; got {resp.msg_type.value}")
            print(f"Connected as {self.client_id}")

        if start_receiver:
            self.is_running = True
            self.receiver_thread = threading.Thread(target=self._receiver_loop, daemon=True)
            self.receiver_thread.start()

    def close(self):
        self.is_running = False
        try:
            with self.sock_lock:
                if self.sock:
                    try:
                        unreg = Message(MessageType.CLIENT_UNREGISTER, self.client_id, {})
                        self._send_raw(unreg)
                    except Exception:
                        pass
                    try:
                        self.sock.close()
                    except Exception:
                        pass
                    self.sock = None
        except Exception:
            pass

    def run_cli(self):
        """Interactive command loop."""
        print("Commands:")
        print("  send <message...>                - Broadcast")
        print("  send all|* <message...>          - Broadcast")
        print("  send to <recipient> <message...> - Private")
        print("  send @<recipient> <message...>   - Private")
        print("  quit                              - Exit")
        print()

        while self.is_running:
            try:
                line = input(f"{self.client_id}> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if not line:
                continue

            if line.lower() in ("quit", "exit"):
                break

            if line.startswith("send "):
                self._handle_send_command(line)
            else:
                print(f"Unknown command: {line}")

        self.close()

    # ----------------------------
    # Sending
    # ----------------------------
    def _handle_send_command(self, line: str):
        parts = line.split()
        if len(parts) < 2:
            print("Usage: send <message...> | send to <recipient> <message...> | send @<recipient> <message...>")
            return

        recipient = None
        msg_text = None

        if len(parts) >= 3 and parts[1] == "to":
            recipient = parts[2]
            msg_text = " ".join(parts[3:]) if len(parts) > 3 else ""
        elif parts[1].startswith("@"):
            recipient = parts[1][1:]
            msg_text = " ".join(parts[2:]) if len(parts) > 2 else ""
        elif parts[1] in ("all", "*"):
            recipient = None
            msg_text = " ".join(parts[2:]) if len(parts) > 2 else ""
        else:
            recipient = None
            msg_text = " ".join(parts[1:])

        if not msg_text:
            print("Message cannot be empty.")
            return

        self.send_message(msg_text, recipient=recipient)

    def send_message(self, content: str, recipient: Optional[str] = None):
        """Send a chat message with seq + message_id; track pending until ACK."""
        self.seq += 1
        message_id = str(uuid.uuid4())

        msg = ClientMessage(
            self.client_id,
            content,
            recipient=recipient,
            seq=self.seq,
            message_id=message_id
        )

        with self.pending_lock:
            self.pending[message_id] = {
                "content": content,
                "recipient": recipient,
                "seq": self.seq,
                "ts": time.time(),
            }

        self._send(msg)

    def _send(self, msg: Message):
        """Send with reconnect-on-failure."""
        data = msg.to_bytes()
        try:
            with self.sock_lock:
                if not self.sock:
                    raise ConnectionError("Not connected")
                self.sock.sendall(data)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError, ConnectionError) as e:
            self.logger.warning(f"Send failed ({e}); reconnecting...")
            self._handle_disconnect()
            # retry once after reconnect
            with self.sock_lock:
                if not self.sock:
                    raise ConnectionError("Reconnect failed")
                self.sock.sendall(data)

    def _send_raw(self, msg: Message):
        """Send without auto-reconnect (used during connect/register)."""
        data = msg.to_bytes()
        with self.sock_lock:
            if not self.sock:
                raise RuntimeError("Not connected")
            self.sock.sendall(data)

    # ----------------------------
    # Receiving
    # ----------------------------
    def _receiver_loop(self):
        """Receive messages; auto-reconnect on socket failure."""
        while self.is_running:
            try:
                msg = self._recv_message()
                if not msg:
                    self.logger.warning("Connection lost; reconnecting...")
                    self._handle_disconnect()
                    continue

                if msg.msg_type == MessageType.MESSAGE_ACK:
                    acked_id = msg.payload.get("acked_message_id")
                    if acked_id:
                        with self.pending_lock:
                            self.pending.pop(acked_id, None)
                        self.logger.info(f"ACK received for message {acked_id}")
                    continue

                if msg.msg_type == MessageType.CLIENT_MESSAGE:
                    sender = msg.sender_id
                    content = msg.payload.get("content", "")
                    seq = msg.payload.get("seq")
                    if seq is not None:
                        print(f"\n[{sender}] {content} [seq={seq}]")
                    else:
                        print(f"\n[{sender}] {content}")
                    print(f"{self.client_id}> ", end="", flush=True)
                    continue

                if msg.msg_type == MessageType.SERVER_RESPONSE:
                    message = msg.payload.get("message", "")
                    print(f"\n[server] {message}")
                    print(f"{self.client_id}> ", end="", flush=True)
                    continue

                print(f"\n[info] Received {msg.msg_type.value}: {msg.payload}")
                print(f"{self.client_id}> ", end="", flush=True)

            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError) as e:
                if self.is_running:
                    self.logger.warning(f"Receiver socket error ({e}); reconnecting...")
                    self._handle_disconnect()
                    continue
            except Exception as e:
                if self.is_running:
                    self.logger.error(f"Receiver error: {e}")
                time.sleep(0.2)

        self.is_running = False

    def _recv_exactly(self, n: int) -> Optional[bytes]:
        with self.sock_lock:
            s = self.sock
        if not s:
            return None

        data = b""
        while len(data) < n:
            chunk = s.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    def _recv_message(self) -> Optional[Message]:
        length_data = self._recv_exactly(4)
        if not length_data:
            return None
        length = int.from_bytes(length_data, byteorder="big")
        if length <= 0 or length > self.MAX_MESSAGE_SIZE:
            return None
        payload = self._recv_exactly(length)
        if not payload:
            return None
        return Message.from_json(payload.decode("utf-8"))

    # ----------------------------
    # Reconnect + Discovery + Resend
    # ----------------------------
    def _handle_disconnect(self):
        """Close current TCP socket, discover assigned server via multicast, reconnect, resend pending."""
        # Ensure only one reconnect runs at a time
        with self.reconnect_lock:
            # close current socket
            with self.sock_lock:
                if self.sock:
                    try:
                        self.sock.close()
                    except Exception:
                        pass
                    self.sock = None

            # best-effort loop until reconnected or client stops
            while self.is_running:
                try:
                    host, port = self.discover_assigned_server()
                    if host and port:
                        self.server_host, self.server_port = host, port
                        self.logger.info(f"Reconnecting to assigned server {host}:{port}...")
                    else:
                        self.logger.warning("Discovery failed; retrying with last known server...")

                    # reconnect + register (do NOT start a second receiver thread)
                    self.connect(start_receiver=False)

                    # resend pending messages (same message_id + seq)
                    self._resend_pending()

                    self.logger.info("Reconnected successfully.")
                    # refresh prompt if user is typing
                    print(f"\n[info] Reconnected to {self.server_host}:{self.server_port}")
                    print(f"{self.client_id}> ", end="", flush=True)
                    return

                except Exception as e:
                    self.logger.warning(f"Reconnect attempt failed: {e}")
                    time.sleep(self.RECONNECT_BACKOFF)

    def _get_all_local_ips(self) -> List[str]:
        """Get all local IPs (for sending discovery on all interfaces)"""
        ips = []
        try:
            hostname = socket.gethostname()
            _, _, ip_list = socket.gethostbyname_ex(hostname)
            for ip in ip_list:
                if not ip.startswith("127."):
                    ips.append(ip)
        except Exception:
            pass
        return ips if ips else ['127.0.0.1']

    def _get_subnet_broadcast(self, ip: str) -> str:
        """Calculate subnet broadcast address (e.g., 192.168.0.118 -> 192.168.0.255)"""
        parts = ip.split('.')
        if len(parts) == 4:
            return f"{parts[0]}.{parts[1]}.{parts[2]}.255"
        return '255.255.255.255'

    def discover_assigned_server(self) -> Tuple[Optional[str], Optional[int]]:
        """
        Send SERVER_DISCOVERY over UDP multicast AND broadcast, wait for SERVER_ANNOUNCE.
        Uses same approach as server heartbeats for maximum compatibility.
        """
        # Create UDP socket for send/recv
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            if hasattr(socket, 'SO_REUSEPORT'):
                try:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                except:
                    pass
        except Exception:
            pass

        # Bind to multicast port to receive responses
        try:
            s.bind(('0.0.0.0', self.MULTICAST_PORT))
            
            # Join multicast group on all interfaces
            mreq_any = struct.pack('4sl', 
                                   socket.inet_aton(self.MULTICAST_GROUP), 
                                   socket.INADDR_ANY)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq_any)
            
            self.logger.info(f"Discovery socket bound to port {self.MULTICAST_PORT}")
        except OSError as e:
            self.logger.warning(f"Failed to bind discovery socket: {e}")
            s.close()
            return None, None

        s.settimeout(self.DISCOVERY_TIMEOUT)

        discovery = Message(
            MessageType.SERVER_DISCOVERY,
            self.client_id,
            {"ts": time.time()}
        )
        data_to_send = discovery.to_json().encode("utf-8")

        # === SEND DISCOVERY VIA MULTIPLE METHODS (like server heartbeats) ===
        local_ips = self._get_all_local_ips()
        self.logger.info(f"Discovery: Local IPs: {local_ips}")

        # 1. Send to multicast group
        try:
            s.sendto(data_to_send, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
            self.logger.info(f"Discovery: Sent to multicast {self.MULTICAST_GROUP}:{self.MULTICAST_PORT}")
        except Exception as e:
            self.logger.debug(f"Discovery: Multicast send failed: {e}")

        # 2. Send via each interface (multicast)
        for ip in local_ips:
            try:
                temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                temp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 32)
                temp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(ip))
                temp_sock.sendto(data_to_send, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
                temp_sock.close()
                self.logger.debug(f"Discovery: Sent multicast via {ip}")
            except Exception:
                pass

        # 3. Send subnet-specific broadcasts
        broadcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        broadcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        
        sent_broadcasts = set()
        for ip in local_ips:
            try:
                subnet_broadcast = self._get_subnet_broadcast(ip)
                if subnet_broadcast not in sent_broadcasts:
                    broadcast_sock.sendto(data_to_send, (subnet_broadcast, self.MULTICAST_PORT))
                    sent_broadcasts.add(subnet_broadcast)
                    self.logger.info(f"Discovery: Sent broadcast to {subnet_broadcast}:{self.MULTICAST_PORT}")
            except Exception as e:
                self.logger.debug(f"Discovery: Broadcast to {subnet_broadcast} failed: {e}")

        # 4. Global broadcast as fallback
        try:
            broadcast_sock.sendto(data_to_send, ('255.255.255.255', self.MULTICAST_PORT))
            self.logger.info(f"Discovery: Sent global broadcast 255.255.255.255:{self.MULTICAST_PORT}")
        except Exception:
            pass
        broadcast_sock.close()

        # === WAIT FOR RESPONSE ===
        try:
            self.logger.info(f"Discovery: Waiting for response (timeout={self.DISCOVERY_TIMEOUT}s)...")
            while True:
                data, addr = s.recvfrom(4096)
                msg = Message.from_json(data.decode("utf-8"))
                
                # Filter out our own discovery message (echo)
                if msg.msg_type == MessageType.SERVER_DISCOVERY:
                    if msg.sender_id == self.client_id:
                        self.logger.debug(f"Discovery: Ignoring echo from ourselves")
                        continue
                    else:
                        self.logger.debug(f"Discovery: Ignoring discovery from other client: {msg.sender_id}")
                        continue
                
                # We're looking for SERVER_ANNOUNCE
                if msg.msg_type != MessageType.SERVER_ANNOUNCE:
                    self.logger.debug(f"Discovery: Ignoring unexpected message type: {msg.msg_type}")
                    continue
                
                self.logger.info(f"Discovery: Received {len(data)} bytes from {addr}")

                host = msg.payload.get("assigned_host")
                port = msg.payload.get("assigned_tcp_port")
                server_id = msg.payload.get("server_id")
                
                if host and port:
                    self.logger.info(f"Discovery: SUCCESS - Server {server_id} at {host}:{port}")
                    return host, int(port)
                else:
                    self.logger.warning(f"Discovery: Invalid response - missing host or port in {msg.payload}")
                    continue
                    
        except socket.timeout:
            self.logger.warning(f"Discovery: TIMEOUT after {self.DISCOVERY_TIMEOUT}s - no server response")
            return None, None
        except Exception as e:
            self.logger.warning(f"Discovery: Receive error: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return None, None
        finally:
            try:
                s.close()
            except Exception:
                pass

    def _resend_pending(self):
        """Resend all un-ACKed messages in seq order (same message_id + seq)."""
        with self.pending_lock:
            items = list(self.pending.items())

        # sort by seq to preserve sender FIFO
        items.sort(key=lambda kv: kv[1].get("seq", 0))

        for message_id, meta in items:
            content = meta.get("content", "")
            recipient = meta.get("recipient")
            seq = meta.get("seq")

            # Recreate message with SAME message_id and SAME seq
            msg = ClientMessage(
                self.client_id,
                content,
                recipient=recipient,
                seq=seq,
                message_id=message_id
            )
            try:
                self._send_raw(msg)
            except Exception as e:
                # If resend fails, reconnect loop will run again from sender/receiver paths
                self.logger.warning(f"Resend failed for {message_id}: {e}")
                raise

    # ----------------------------
    # Entry point
    # ----------------------------
def main():
    import sys

    if len(sys.argv) < 2:
        print("Usage: python client.py <client_id> [<server_host> <server_port>]")
        print("If host/port omitted, the client discovers a server via UDP multicast.")
        sys.exit(1)

    client_id = sys.argv[1]

    # Manual connect mode (kept for debugging)
    if len(sys.argv) >= 4:
        host = sys.argv[2]
        port = int(sys.argv[3])
        print(f"Connecting to {host}:{port}...")
        c = Client(client_id, host, port)
        c.connect(start_receiver=True)
        c.run_cli()
        return

    # Auto-discovery mode (no host/port on CLI)
    print("Discovering server via UDP multicast...")
    c = Client(client_id, "0.0.0.0", 0)
    host, port = c.discover_assigned_server()
    if not host or not port:
        print("Discovery failed. Start a server and allow UDP 5007 in the firewall, or pass host+port explicitly.")
        sys.exit(2)

    c.server_host, c.server_port = host, port
    print(f"Connecting to {host}:{port}...")
    c.connect(start_receiver=True)
    c.run_cli()


if __name__ == "__main__":
    main()
