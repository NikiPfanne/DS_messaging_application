"""
Client for the distributed messaging system.
Connects to servers via TCP to send and receive messages.

Enhancement:
- Dynamic discovery (SERVER_DISCOVERY / SERVER_ANNOUNCE) via UDP multicast.
  If server_host/server_port are not provided, the client discovers servers
  and connects to the best candidate (prefer leader, else lowest load ratio).
"""

import socket
import threading
import time
import logging
from typing import Optional, Callable, List, Dict, Any, Tuple
from protocol import Message, MessageType, ClientMessage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# Must match server multicast config
MULTICAST_GROUP = '224.0.0.1'
MULTICAST_PORT = 5007

# Discovery behavior
DEFAULT_DISCOVERY_TIMEOUT = 2.0  # seconds
MAX_DISCOVERY_DATAGRAM = 65535


class Client:
    """Client for distributed messaging system"""

    def __init__(self, client_id: str, server_host: str = 'localhost', server_port: int = 5000):
        self.client_id = client_id
        self.server_host = server_host
        self.server_port = server_port

        self.socket: Optional[socket.socket] = None
        self.is_connected = False
        self.is_running = False

        self.message_callback: Optional[Callable] = None
        self.receive_thread: Optional[threading.Thread] = None

        self.logger = logging.getLogger(f"Client-{client_id}")

    def connect(self) -> bool:
        """Connect to the server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.server_host, self.server_port))
            self.is_connected = True
            self.is_running = True

            # Send registration message
            reg_msg = Message(MessageType.CLIENT_REGISTER, self.client_id)
            self._send_message(reg_msg)

            # Wait for registration response
            response = self._receive_message()
            if response and response.msg_type == MessageType.SERVER_RESPONSE:
                if response.payload.get('success'):
                    self.logger.info(f"Connected to server: {response.payload.get('message')}")

                    # Start receive thread
                    self.receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
                    self.receive_thread.start()

                    return True

            self.disconnect()
            return False

        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            self.is_connected = False
            return False

    def disconnect(self):
        """Disconnect from the server"""
        self.is_running = False

        if self.is_connected and self.socket:
            try:
                # Send unregister message
                unreg_msg = Message(MessageType.CLIENT_UNREGISTER, self.client_id)
                self._send_message(unreg_msg)
            except (socket.error, OSError):
                pass

        if self.socket:
            try:
                self.socket.close()
            except (socket.error, OSError):
                pass

        self.is_connected = False
        self.logger.info("Disconnected from server")

    def send_message(self, content: str, recipient: Optional[str] = None) -> bool:
        """Send a message"""
        if not self.is_connected:
            self.logger.error("Not connected to server")
            return False

        try:
            msg = ClientMessage(self.client_id, content, recipient)
            self._send_message(msg)
            return True
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            return False

    def set_message_callback(self, callback: Callable[[str, str], None]):
        """Set callback for incoming messages (callback receives sender and content)"""
        self.message_callback = callback

    def _send_message(self, msg: Message):
        """Send message through socket"""
        if not self.socket:
            raise Exception("Socket not initialized")
        self.socket.sendall(msg.to_bytes())

    def _receive_message(self) -> Optional[Message]:
        """Receive a single message from socket"""
        if not self.socket:
            return None

        try:
            # Receive length prefix
            length_data = self._recv_exactly(4)
            if not length_data:
                return None

            length = int.from_bytes(length_data, byteorder='big')
            if length > 1024 * 1024:  # 1MB limit
                self.logger.warning("Message too large")
                return None

            # Receive message data
            msg_data = self._recv_exactly(length)
            if not msg_data:
                return None

            return Message.from_json(msg_data.decode('utf-8'))

        except Exception as e:
            if self.is_running:
                self.logger.error(f"Error receiving message: {e}")
            return None

    def _recv_exactly(self, n: int) -> Optional[bytes]:
        """Receive exactly n bytes from socket"""
        data = b''
        while len(data) < n:
            chunk = self.socket.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    def _receive_loop(self):
        """Continuously receive messages from server"""
        while self.is_running and self.is_connected:
            msg = self._receive_message()

            if not msg:
                if self.is_running:
                    self.logger.warning("Connection lost")
                    self.is_connected = False
                break

            self._handle_message(msg)

    def _handle_message(self, msg: Message):
        """Handle incoming message"""
        if msg.msg_type == MessageType.SERVER_RESPONSE:
            # Log server responses
            if not msg.payload.get('success'):
                self.logger.warning(f"Server error: {msg.payload.get('message')}")

        elif msg.msg_type == MessageType.CLIENT_MESSAGE:
            # Incoming message from another client
            sender = msg.sender_id
            content = msg.payload.get('content', '')

            self.logger.info(f"Message from {sender}: {content}")

            if self.message_callback:
                try:
                    self.message_callback(sender, content)
                except Exception as e:
                    self.logger.error(f"Error in message callback: {e}")


def _join_multicast(sock: socket.socket, group: str, port: int):
    """
    Join a multicast group on all interfaces.
    Works on Windows/macOS/Linux for typical setups.
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(('', port))
    except OSError:
        # Some Windows setups require explicit localhost bind fallback
        sock.bind(('0.0.0.0', port))

    mreq = struct_pack_mreq(group)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    # Keep it local-ish
    try:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    except OSError:
        pass


def struct_pack_mreq(group: str) -> bytes:
    """
    Pack membership request for IP_ADD_MEMBERSHIP.
    """
    import struct
    return struct.pack("4sl", socket.inet_aton(group), socket.INADDR_ANY)


def discover_servers(client_id: str, timeout_s: float = DEFAULT_DISCOVERY_TIMEOUT) -> List[Dict[str, Any]]:
    """
    Discover servers via UDP multicast.
    Returns a list of server info dicts:
      { 'host': ip, 'tcp_port': int, 'server_id': str, 'is_leader': bool,
        'current_leader': str|None, 'load': int, 'capacity': int }
    """
    logger = logging.getLogger(f"Discovery-{client_id}")
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    udp.settimeout(0.2)

    servers: Dict[Tuple[str, int, str], Dict[str, Any]] = {}

    try:
        _join_multicast(udp, MULTICAST_GROUP, MULTICAST_PORT)
    except Exception as e:
        logger.error(f"Multicast join failed (discovery not possible): {e}")
        udp.close()
        return []

    # Send discovery request (to group)
    discovery = Message(
        MessageType.SERVER_DISCOVERY,
        client_id,
        {'ts': time.time()}
    )

    try:
        udp.sendto(discovery.to_json().encode('utf-8'), (MULTICAST_GROUP, MULTICAST_PORT))
    except Exception as e:
        logger.error(f"Failed to send discovery multicast: {e}")
        udp.close()
        return []

    # Collect announces until timeout
    deadline = time.monotonic() + max(0.1, timeout_s)
    while time.monotonic() < deadline:
        try:
            data, addr = udp.recvfrom(MAX_DISCOVERY_DATAGRAM)
            msg = Message.from_json(data.decode('utf-8'))

            if msg.msg_type != MessageType.SERVER_ANNOUNCE:
                continue

            payload = msg.payload or {}

            # IMPORTANT: Use sender address as host (more reliable than payload in messy networks)
            host_ip = addr[0]
            tcp_port = int(payload.get('tcp_port', 0) or 0)
            server_id = payload.get('server_id') or msg.sender_id

            if not tcp_port or not server_id:
                continue

            info = {
                'host': host_ip,
                'tcp_port': tcp_port,
                'server_id': server_id,
                'is_leader': bool(payload.get('is_leader', False)),
                'current_leader': payload.get('current_leader'),
                'load': int(payload.get('load', 0) or 0),
                'capacity': int(payload.get('capacity', 0) or 0),
            }

            key = (host_ip, tcp_port, server_id)
            servers[key] = info

        except socket.timeout:
            continue
        except Exception:
            continue

    udp.close()
    return list(servers.values())


def pick_best_server(servers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Choose best server:
    1) Prefer leader
    2) Else prefer lowest load ratio (load/capacity) where capacity > 0
    3) Else lowest load
    """
    if not servers:
        return None

    leaders = [s for s in servers if s.get('is_leader')]
    candidates = leaders if leaders else servers

    def score(s: Dict[str, Any]) -> Tuple[int, float, int]:
        # lower is better
        cap = int(s.get('capacity') or 0)
        load = int(s.get('load') or 0)
        ratio = (load / cap) if cap > 0 else 999999.0
        return (0 if s.get('is_leader') else 1, ratio, load)

    candidates.sort(key=score)
    return candidates[0]


def main():
    """Main entry point for interactive client"""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python client.py <client_id> [server_host] [server_port]")
        print("Discovery mode: python client.py <client_id>   (no host/port)")
        sys.exit(1)

    client_id = sys.argv[1]

    # If host/port provided -> classic mode
    if len(sys.argv) >= 3:
        server_host = sys.argv[2]
        server_port = int(sys.argv[3]) if len(sys.argv) > 3 else 5000
    else:
        # Discovery mode
        print(f"Discovering servers via UDP multicast {MULTICAST_GROUP}:{MULTICAST_PORT} ...")
        servers = discover_servers(client_id, DEFAULT_DISCOVERY_TIMEOUT)

        if not servers:
            print("No servers discovered. Either multicast is blocked, or no server is running.")
            print("Fallback: python client.py <client_id> <server_host> <server_port>")
            sys.exit(1)

        best = pick_best_server(servers)
        assert best is not None
        server_host = best['host']
        server_port = best['tcp_port']

        # Show what we picked (pragmatic feedback)
        leader_flag = "LEADER" if best.get('is_leader') else "FOLLOWER"
        print(f"Discovered {len(servers)} server(s). Using {best['server_id']} @ {server_host}:{server_port} ({leader_flag}, load={best.get('load')})")

    client = Client(client_id, server_host, server_port)

    # Set message callback
    def on_message(sender: str, content: str):
        print(f"\n[{sender}]: {content}")
        print(f"{client_id}> ", end='', flush=True)

    client.set_message_callback(on_message)

    # Connect to server
    print(f"Connecting to {server_host}:{server_port}...")
    if not client.connect():
        print("Failed to connect to server")
        sys.exit(1)

    print(f"Connected as {client_id}")
    print("Commands:")
    print("  send <message...>                - Broadcast")
    print("  send all|* <message...>          - Broadcast")
    print("  send to <recipient> <message...> - Private")
    print("  send @<recipient> <message...>   - Private")
    print("  quit                              - Exit")
    print()

    try:
        while client.is_connected:
            try:
                user_input = input(f"{client_id}> ")

                if not user_input.strip():
                    continue

                parts = user_input.strip().split()
                command = parts[0].lower()

                if command == 'quit':
                    break

                elif command == 'send':
                    # Supported forms:
                    # send <message...>                      -> broadcast
                    # send all <message...>                  -> broadcast
                    # send * <message...>                    -> broadcast
                    # send to <recipient> <message...>       -> private
                    # send @<recipient> <message...>         -> private
                    if len(parts) < 2:
                        print("Usage: send <message...> | send all <message...> | send to <recipient> <message...> | send @<recipient> <message...>")
                        continue

                    if parts[1].lower() in ('all', '*'):
                        msg_text = " ".join(parts[2:]).strip()
                        if not msg_text:
                            print("Usage: send all <message...>")
                            continue
                        client.send_message(msg_text)
                        continue

                    if parts[1].lower() == 'to':
                        if len(parts) < 3:
                            print("Usage: send to <recipient> <message...>")
                            continue
                        recipient = parts[2]
                        msg_text = " ".join(parts[3:]).strip()
                        if not msg_text:
                            print("Usage: send to <recipient> <message...>")
                            continue
                        client.send_message(msg_text, recipient)
                        continue

                    if parts[1].startswith('@'):
                        recipient = parts[1][1:]
                        msg_text = " ".join(parts[2:]).strip()
                        if not recipient or not msg_text:
                            print("Usage: send @<recipient> <message...>")
                            continue
                        client.send_message(msg_text, recipient)
                        continue

                    msg_text = " ".join(parts[1:]).strip()
                    client.send_message(msg_text)

                else:
                    print(f"Unknown command: {command}")

            except EOFError:
                break

    except KeyboardInterrupt:
        print("\nInterrupted")

    finally:
        client.disconnect()
        print("Disconnected")


if __name__ == '__main__':
    main()
