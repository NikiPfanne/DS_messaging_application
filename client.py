"""
Client for the distributed messaging system.
Connects to servers via TCP to send and receive messages.
"""

import socket
import threading
import time
import logging
import uuid
from typing import Optional, Callable
from protocol import Message, MessageType, ClientMessage, ServerResponse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


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

        # Client-side sequencing + acknowledgments (best-effort)
        self._seq = 0
        self._ack_events: dict[str, threading.Event] = {}
        self._ack_lock = threading.Lock()

        self.logger = logging.getLogger(f"Client-{client_id}")

    def connect(self) -> bool:
        """Connect to the server"""
        try:
            self.is_running = True
            if not self._connect_and_register():
                self.disconnect()
                return False

            # Start receive thread
            self.receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
            self.receive_thread.start()
            return True

        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            self.is_connected = False
            return False

    def _connect_and_register(self) -> bool:
        """(Re)connect socket and register client. Used for initial connect and reconnect."""
        # Close any existing socket
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.server_host, self.server_port))
        self.is_connected = True

        # Send registration message
        reg_msg = Message(MessageType.CLIENT_REGISTER, self.client_id)
        self._send_message(reg_msg)

        # Wait for registration response
        response = self._receive_message()
        if response and response.msg_type == MessageType.SERVER_RESPONSE and response.payload.get('success'):
            self.logger.info(f"Connected to server: {response.payload.get('message')}")
            return True

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

        # Best-effort: wait for MESSAGE_ACK from server and retry a few times.
        # This does NOT guarantee delivery to every recipient; it only confirms the server processed the message.
        self._seq += 1
        msg_id = str(uuid.uuid4())
        evt = threading.Event()
        with self._ack_lock:
            self._ack_events[msg_id] = evt

        try:
            msg = ClientMessage(self.client_id, content, recipient, seq=self._seq, message_id=msg_id)

            retries = 3
            timeout_s = 2.0
            for attempt in range(1, retries + 1):
                self._send_message(msg)

                if evt.wait(timeout=timeout_s):
                    return True

                self.logger.warning(
                    f"No ACK for message {msg_id} (attempt {attempt}/{retries})"
                )

            self.logger.error(f"Giving up: no ACK for message {msg_id}")
            return False

        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            return False

        finally:
            with self._ack_lock:
                self._ack_events.pop(msg_id, None)

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

                    # Best-effort reconnect (same host/port). No discovery here.
                    if self._attempt_reconnect():
                        continue
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

        elif msg.msg_type == MessageType.MESSAGE_ACK:
            acked_id = (msg.payload or {}).get('acked_message_id')
            if not acked_id:
                return
            with self._ack_lock:
                evt = self._ack_events.get(acked_id)
            if evt:
                evt.set()
                self.logger.info(f"ACK received for message {acked_id}")

    def _attempt_reconnect(self) -> bool:
        """Try to reconnect a few times. Returns True if reconnected."""
        for i in range(1, 6):
            if not self.is_running:
                return False
            try:
                time.sleep(1.0)
                if self._connect_and_register():
                    self.logger.info(f"Reconnected to {self.server_host}:{self.server_port}")
                    return True
            except Exception as e:
                self.logger.debug(f"Reconnect attempt {i} failed: {e}")
        self.logger.error("Reconnect failed")
        return False


def main():
    """Main entry point for interactive client"""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python client.py <client_id> [server_host] [server_port]")
        sys.exit(1)

    client_id = sys.argv[1]
    server_host = sys.argv[2] if len(sys.argv) > 2 else 'localhost'
    server_port = int(sys.argv[3]) if len(sys.argv) > 3 else 5000

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
    print("  send <message...>              - Broadcast")
    print("  send all|* <message...>        - Broadcast")
    print("  send to <recipient> <message...> - Private")
    print("  send @<recipient> <message...>  - Private")
    print("  quit                            - Exit")
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

                    # Explicit broadcast keywords
                    if parts[1].lower() in ('all', '*'):
                        msg_text = " ".join(parts[2:]).strip()
                        if not msg_text:
                            print("Usage: send all <message...>")
                            continue
                        client.send_message(msg_text)
                        continue

                    # Explicit private: 'to <recipient> <msg>'
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

                    # Explicit private: '@recipient <msg>'
                    if parts[1].startswith('@'):
                        recipient = parts[1][1:]
                        msg_text = " ".join(parts[2:]).strip()
                        if not recipient or not msg_text:
                            print("Usage: send @<recipient> <message...>")
                            continue
                        client.send_message(msg_text, recipient)
                        continue

                    # Default: treat as broadcast
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
