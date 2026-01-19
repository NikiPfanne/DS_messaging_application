"""
Distributed messaging client implementation.
Features:
- TCP connection to server
- Interactive CLI (send broadcast/private, quit)
- Receive messages asynchronously
- Per-client sequencing (seq) for FIFO (useful for forwarded ordering)
- Explicit MESSAGE_ACK handling (server confirms processing)
"""

import socket
import threading
import logging
import time
import uuid
from typing import Optional, Dict

from protocol import Message, MessageType, ClientMessage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class Client:
    def __init__(self, client_id: str, server_host: str, server_port: int):
        self.client_id = client_id
        self.server_host = server_host
        self.server_port = server_port

        self.sock: Optional[socket.socket] = None
        self.is_running = False

        # Sequencing for FIFO per sender
        self.seq = 0

        # Track pending messages awaiting ACK: message_id -> timestamp
        self.pending: Dict[str, float] = {}
        self.pending_lock = threading.Lock()

        self.logger = logging.getLogger(f"Client-{client_id}")

    def connect(self):
        """Connect to server and register."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.server_host, self.server_port))
        self.is_running = True

        # Register
        reg = Message(MessageType.CLIENT_REGISTER, self.client_id, {})
        self._send(reg)

        # Wait for server response (registration)
        resp = self._recv_message()
        if not resp:
            raise RuntimeError("No response from server during registration")

        # Could be ServerResponse OR other message; print something readable
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

        # Start receiver thread
        t = threading.Thread(target=self._receiver_loop, daemon=True)
        t.start()

    def close(self):
        self.is_running = False
        try:
            if self.sock:
                unreg = Message(MessageType.CLIENT_UNREGISTER, self.client_id, {})
                self._send(unreg)
        except Exception:
            pass
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        self.sock = None

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

    def _handle_send_command(self, line: str):
        parts = line.split()
        if len(parts) < 2:
            print("Usage: send <message...> | send to <recipient> <message...> | send @<recipient> <message...>")
            return

        # Cases:
        # send <msg...>  (broadcast)
        # send all <msg...> (broadcast)
        # send * <msg...> (broadcast)
        # send to bob <msg...> (private)
        # send @bob <msg...> (private)

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
            self.pending[message_id] = time.time()

        self._send(msg)

    def _receiver_loop(self):
        """Receive messages until socket closes."""
        while self.is_running:
            try:
                msg = self._recv_message()
                if not msg:
                    break

                if msg.msg_type == MessageType.MESSAGE_ACK:
                    acked_id = msg.payload.get("acked_message_id")
                    if acked_id:
                        with self.pending_lock:
                            if acked_id in self.pending:
                                self.pending.pop(acked_id, None)
                        self.logger.info(f"ACK received for message {acked_id}")
                    continue

                if msg.msg_type == MessageType.CLIENT_MESSAGE:
                    sender = msg.sender_id
                    content = msg.payload.get("content", "")
                    # print incoming message nicely
                    print(f"\n[{sender}] {content}")
                    # re-print prompt
                    print(f"{self.client_id}> ", end="", flush=True)
                    continue

                if msg.msg_type == MessageType.SERVER_RESPONSE:
                    # optional: server responses
                    message = msg.payload.get("message", "")
                    print(f"\n[server] {message}")
                    print(f"{self.client_id}> ", end="", flush=True)
                    continue

                # fallback
                print(f"\n[info] Received {msg.msg_type.value}: {msg.payload}")
                print(f"{self.client_id}> ", end="", flush=True)

            except Exception as e:
                if self.is_running:
                    self.logger.error(f"Receiver error: {e}")
                break

        self.is_running = False

    def _send(self, msg: Message):
        if not self.sock:
            raise RuntimeError("Not connected")
        data = msg.to_bytes()
        self.sock.sendall(data)

    def _recv_exactly(self, n: int) -> Optional[bytes]:
        if not self.sock:
            return None
        data = b""
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    def _recv_message(self) -> Optional[Message]:
        length_data = self._recv_exactly(4)
        if not length_data:
            return None
        length = int.from_bytes(length_data, byteorder="big")
        if length <= 0 or length > 1024 * 1024:
            return None
        payload = self._recv_exactly(length)
        if not payload:
            return None
        return Message.from_json(payload.decode("utf-8"))


def main():
    import sys

    if len(sys.argv) < 4:
        print("Usage: python client.py <client_id> <server_host> <server_port>")
        sys.exit(1)

    client_id = sys.argv[1]
    host = sys.argv[2]
    port = int(sys.argv[3])

    print(f"Connecting to {host}:{port}...")
    c = Client(client_id, host, port)
    c.connect()
    c.run_cli()


if __name__ == "__main__":
    main()
