"""
Client for the distributed messaging system.
Connects to servers via TCP to send and receive messages.
"""

import socket
import threading
import time
import logging
from typing import Optional, Callable, Tuple
from protocol import Message, MessageType, ClientMessage, ServerResponse
import config

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
        
        self.logger = logging.getLogger(f"Client-{client_id}")
    
    def connect(self) -> bool:
        """Connect to the server and register."""
        try:
            # Ensure any old connection is closed before starting a new one
            if self.socket:
                self._close_connection()

            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.logger.info(f"Connecting to {self.server_host}:{self.server_port}...")
            self.socket.connect((self.server_host, self.server_port))
            
            # Send registration message
            reg_msg = Message(MessageType.CLIENT_REGISTER, self.client_id)
            self._send_message(reg_msg)
            
            # Wait for registration response
            response = self._receive_message()
            if response and response.msg_type == MessageType.SERVER_RESPONSE:
                if response.payload.get('success'):
                    self.logger.info(f"Connected to server: {response.payload.get('message')}")
                    self.is_connected = True
                    return True
            
            # If registration fails, clean up
            self._close_connection()
            return False
        
        except Exception as e:
            self.logger.error(f"Connection attempt failed: {e}")
            self._close_connection()
            return False

    def start(self):
        """Start the client's background processing loop."""
        if not self.receive_thread or not self.receive_thread.is_alive():
            self.is_running = True
            self.receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
            self.receive_thread.start()

    
    def _close_connection(self):
        """Internal method to close the socket and reset connection state."""
        if self.socket:
            try:
                self.socket.close()
            except (socket.error, OSError):
                pass
        self.socket = None
        self.is_connected = False

    def disconnect(self):
        """Disconnect from the server and stop running."""
        self.is_running = False
        
        if self.is_connected and self.socket:
            try:
                # Send unregister message if we can
                unreg_msg = Message(MessageType.CLIENT_UNREGISTER, self.client_id)
                self._send_message(unreg_msg)
            except (socket.error, OSError):
                pass
        
        self._close_connection()
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

    @staticmethod
    def discover_server(client_id: str) -> Optional[Tuple[str, int]]:
        """Discover a server using UDP multicast."""
        logger = logging.getLogger(f"Client-{client_id}-Discovery")
        logger.info(f"Attempting to discover a server on {config.DEFAULT_MULTICAST_GROUP}:{config.DEFAULT_MULTICAST_PORT}...")

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
                sock.settimeout(config.UDP_TIMEOUT * 3)  # Give more time for discovery

                # Send discovery message
                discover_msg = Message(MessageType.SERVER_DISCOVERY, client_id)
                sock.sendto(
                    discover_msg.to_json().encode('utf-8'),
                    (config.DEFAULT_MULTICAST_GROUP, config.DEFAULT_MULTICAST_PORT)
                )
                logger.info("Discovery message sent. Waiting for response...")

                # Wait for a response
                while True:
                    try:
                        data, addr = sock.recvfrom(4096)
                        response = Message.from_json(data.decode('utf-8'))

                        if response.msg_type == MessageType.SERVER_ANNOUNCE:
                            payload = response.payload
                            host = payload.get('host')
                            port = payload.get('port')
                            if host and port:
                                logger.info(f"Discovered server {payload.get('server_id')} at {host}:{port}")
                                return host, port
                    except socket.timeout:
                        logger.warning("Server discovery timed out.")
                        return None
                    except Exception:
                        # Ignore malformed messages and continue listening until timeout
                        continue
        except Exception as e:
            logger.error(f"Error during server discovery: {e}")
            return None

    
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
        """Continuously receive messages from server, with reconnect logic."""
        while self.is_running:
            if not self.is_connected:
                # If we have a server address, try to connect
                if self.server_host and self.server_port:
                    self.connect()

                # If connection failed or we had no address, discover a new one
                if not self.is_connected and self.is_running:
                    self.logger.info("Attempting to discover a server...")
                    discovered = self.discover_server(self.client_id)
                    if discovered and self.is_running:
                        self.server_host, self.server_port = discovered
                        self.connect()

                # If we're still not connected, wait and retry
                if not self.is_connected and self.is_running:
                    self.logger.warning("Failed to establish a connection. Retrying in 5 seconds...")
                    time.sleep(5)
                
                continue # Restart the loop to check connection status

            # At this point, we assume we are connected. Try receiving.
            msg = self._receive_message()
            
            if not msg:
                # Connection lost
                if self.is_running:
                    self.logger.warning("Connection lost.")
                    self._close_connection()
                # The loop will attempt to reconnect on the next iteration
            else:
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


def main():
    """Main entry point for interactive client"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python client.py <client_id> [server_host] [server_port]")
        sys.exit(1)
    
    client_id = sys.argv[1]
    # Optional host/port for initial connection attempt
    server_host = sys.argv[2] if len(sys.argv) > 2 else None
    server_port = int(sys.argv[3]) if len(sys.argv) > 3 else None
    
    client = Client(client_id, server_host, server_port)
    
    # Set message callback
    def on_message(sender: str, content: str):
        print(f"\n[{sender}]: {content}")
        print(f"{client_id}> ", end='', flush=True)
    
    client.set_message_callback(on_message)
    
    # Start the client. It will handle connection and reconnection automatically.
    client.start()
    
    # Wait a moment for the initial connection attempt
    print("Client starting... waiting for connection.")
    time.sleep(1) # Give receive_loop a moment to start
    connect_timeout = time.time() + 15 # Wait up to 15s for first connection
    while not client.is_connected and time.time() < connect_timeout and client.is_running:
        time.sleep(0.5)

    if not client.is_connected:
        print("\nCould not connect to a server. Please check the network and servers.")
        client.disconnect()
        sys.exit(1)

    print(f"Connected as {client_id}. Type 'quit' to exit.")
    print("Commands:")
    print("  send <message...>              - Broadcast")
    print("  send all|* <message...>        - Broadcast")
    print("  send to <recipient> <message...> - Private")
    print("  send @<recipient> <message...>  - Private")
    print("  quit                            - Exit")
    print()
    
    try:
        while True:
            # We no longer check client.is_connected here. The input loop runs
            # independently. Sending will fail if disconnected.
            try:
                user_input = input(f"{client_id}> ")
            except EOFError:
                break # Exit on Ctrl+D

            if not user_input.strip():
                continue
            
            parts = user_input.strip().split()
            command = parts[0].lower()
            
            if command == 'quit':
                break
            
            elif command == 'send':
                if len(parts) < 2:
                    print("Usage: send <message...> | send all <message...> | send to <recipient> <message...> | send @<recipient> <message...>")
                    continue

                # Explicit broadcast keywords
                if parts[1].lower() in ('all', '*'):
                    msg_text = " ".join(parts[2:]).strip()
                    if not msg_text:
                        print("Usage: send all <message...>")
                        continue
                    if not client.send_message(msg_text):
                        print("Message failed to send. Client is likely disconnected.")
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
                    if not client.send_message(msg_text, recipient):
                        print("Message failed to send. Client is likely disconnected.")
                    continue

                # Explicit private: '@recipient <msg>'
                if parts[1].startswith('@'):
                    recipient = parts[1][1:]
                    msg_text = " ".join(parts[2:]).strip()
                    if not recipient or not msg_text:
                        print("Usage: send @<recipient> <message...>")
                        continue
                    if not client.send_message(msg_text, recipient):
                        print("Message failed to send. Client is likely disconnected.")
                    continue

                # Default: treat as broadcast to allow messages with spaces
                msg_text = " ".join(parts[1:]).strip()
                if not client.send_message(msg_text):
                    print("Message failed to send. Client is likely disconnected.")
            
            else:
                print(f"Unknown command: {command}")
    
    except KeyboardInterrupt:
        print("\nInterrupted")
    
    finally:
        client.disconnect()
        print("Disconnected")


if __name__ == '__main__':
    main()
