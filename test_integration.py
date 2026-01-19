"""
Integration tests for the distributed messaging system.
Tests actual server-client communication.
"""

import unittest
import time
import threading
from server import Server
from client import Client


class TestIntegration(unittest.TestCase):
    """Integration tests for client-server communication"""
    
    def setUp(self):
        """Set up test server"""
        # Using a non-standard port to avoid conflicts
        self.server = Server("test_server", 5556)
        self.server_thread = threading.Thread(target=self.server.start, daemon=True)
        self.server_thread.start()
        time.sleep(0.5)  # Give server time to start
    
    def tearDown(self):
        """Clean up test server"""
        self.server.stop()
        time.sleep(0.5)
    
    def test_client_connection(self):
        """Test that a client can connect to the server"""
        client = Client("test_client1", "localhost", 5556)
        
        # Connect to server
        self.assertTrue(client.connect(), "Client should connect successfully")
        client.start()
        
        # Wait for the client's internal loop to mark as connected
        time.sleep(0.1)
        self.assertTrue(client.is_connected, "Client should be marked as connected")
        
        # Verify server registered the client
        time.sleep(0.2)
        status = self.server.get_status()
        self.assertEqual(status['connected_clients'], 1, "Server should have 1 connected client")
        
        # Disconnect
        client.disconnect()
        time.sleep(0.2)
        
        # Verify client disconnected
        self.assertFalse(client.is_connected, "Client should be disconnected")
        status = self.server.get_status()
        self.assertEqual(status['connected_clients'], 0, "Server should have 0 connected clients")
    
    def test_multiple_clients(self):
        """Test multiple clients connecting to the same server"""
        clients = []
        
        # Connect 3 clients
        for i in range(3):
            client = Client(f"client{i}", "localhost", 5556)
            self.assertTrue(client.connect(), f"Client {i} should connect")
            client.start()
            clients.append(client)
        
        # Verify server has all clients
        time.sleep(0.2)
        status = self.server.get_status()
        self.assertEqual(status['connected_clients'], 3, "Server should have 3 clients")
        
        # Disconnect all clients
        for client in clients:
            client.disconnect()
        
        time.sleep(0.2)
        status = self.server.get_status()
        self.assertEqual(status['connected_clients'], 0, "All clients should disconnect")
    
    def test_message_sending(self):
        """Test sending messages through the server"""
        client1 = Client("alice", "localhost", 5556)
        self.assertTrue(client1.connect())
        client1.start()
        time.sleep(0.1)
        
        # Send a message
        success = client1.send_message("Hello, world!")
        self.assertTrue(success, "Message should be sent successfully")
        
        time.sleep(0.2)
        client1.disconnect()
    
    def test_message_callback(self):
        """Test receiving messages via callback"""
        client1 = Client("alice", "localhost", 5556)
        client2 = Client("bob", "localhost", 5556)
        
        received_messages = []
        
        def on_message(sender, content):
            received_messages.append((sender, content))
        
        client2.set_message_callback(on_message)
        
        self.assertTrue(client1.connect())
        self.assertTrue(client2.connect())
        client1.start()
        client2.start()
        
        time.sleep(0.2)
        
        # Send message from client1 to all (broadcast)
        client1.send_message("Hello everyone!")
        
        time.sleep(0.5)
        
        # Verify message was received by client2
        self.assertEqual(len(received_messages), 1, "Should have received one message")
        sender, content = received_messages[0]
        self.assertEqual(sender, "alice")
        self.assertEqual(content, "Hello everyone!")
        
        client1.disconnect()
        client2.disconnect()
    
    def test_server_status(self):
        """Test server status reporting"""
        status = self.server.get_status()
        
        self.assertEqual(status['server_id'], "test_server")
        self.assertEqual(status['tcp_port'], 5556)
        self.assertIn('is_leader', status)
        self.assertIn('connected_clients', status)
        self.assertIn('known_servers', status)


class TestServerIndependent(unittest.TestCase):
    """Tests that don't require a running server"""
    
    def test_server_initialization(self):
        """Test server can be initialized"""
        server = Server("test1", 5556)
        self.assertEqual(server.server_id, "test1")
        self.assertEqual(server.tcp_port, 5556)
        self.assertFalse(server.is_running)
    
    def test_client_initialization(self):
        """Test client can be initialized"""
        client = Client("client1", "localhost", 5000)
        self.assertEqual(client.client_id, "client1")
        self.assertEqual(client.server_host, "localhost")
        self.assertEqual(client.server_port, 5000)
        self.assertFalse(client.is_connected)


def run_integration_tests():
    """Run integration tests"""
    unittest.main(argv=[''], verbosity=2, exit=False)


if __name__ == '__main__':
    run_integration_tests()
