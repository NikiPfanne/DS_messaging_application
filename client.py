"""
Client for the distributed messaging system.
Connects to servers via TCP to send and receive messages.
"""

import socket
import threading
import time
import logging
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
            except:
                pass
        
        if self.socket:
            try:
                self.socket.close()
            except:
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
    print("  send <message>          - Send broadcast message")
    print("  send <recipient> <msg>  - Send private message")
    print("  quit                    - Exit")
    print()
    
    try:
        while client.is_connected:
            try:
                user_input = input(f"{client_id}> ")
                
                if not user_input.strip():
                    continue
                
                parts = user_input.strip().split(None, 2)
                command = parts[0].lower()
                
                if command == 'quit':
                    break
                
                elif command == 'send':
                    if len(parts) < 2:
                        print("Usage: send <message> or send <recipient> <message>")
                        continue
                    
                    if len(parts) == 2:
                        # Broadcast message
                        client.send_message(parts[1])
                    else:
                        # Private message
                        recipient = parts[1]
                        message = parts[2]
                        client.send_message(message, recipient)
                
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
