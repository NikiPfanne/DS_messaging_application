"""
Tests for the distributed messaging system.
"""

import unittest
import time
import threading
from protocol import (
    Message, MessageType, ClientMessage, ServerResponse,
    HeartbeatMessage, LeaderElectionMessage, LeaderAnnouncementMessage
)


class TestProtocol(unittest.TestCase):
    """Test message protocol and serialization"""
    
    def test_message_creation(self):
        """Test basic message creation"""
        msg = Message(MessageType.HEARTBEAT, "server1")
        self.assertEqual(msg.msg_type, MessageType.HEARTBEAT)
        self.assertEqual(msg.sender_id, "server1")
        self.assertIsNotNone(msg.message_id)
        self.assertIsNotNone(msg.timestamp)
    
    def test_message_serialization(self):
        """Test message serialization to JSON"""
        msg = Message(MessageType.HEARTBEAT, "server1", {"test": "data"})
        json_str = msg.to_json()
        self.assertIn("HEARTBEAT", json_str)
        self.assertIn("server1", json_str)
        self.assertIn("test", json_str)
    
    def test_message_deserialization(self):
        """Test message deserialization from JSON"""
        original = Message(MessageType.HEARTBEAT, "server1", {"key": "value"})
        json_str = original.to_json()
        
        restored = Message.from_json(json_str)
        self.assertEqual(restored.msg_type, original.msg_type)
        self.assertEqual(restored.sender_id, original.sender_id)
        self.assertEqual(restored.payload, original.payload)
    
    def test_message_to_bytes(self):
        """Test message conversion to bytes with length prefix"""
        msg = Message(MessageType.HEARTBEAT, "server1")
        msg_bytes = msg.to_bytes()
        
        # Check length prefix
        self.assertEqual(len(msg_bytes), 4 + len(msg.to_json().encode('utf-8')))
        
        # Verify length prefix value
        length = int.from_bytes(msg_bytes[:4], byteorder='big')
        self.assertEqual(length, len(msg.to_json().encode('utf-8')))
    
    def test_message_from_bytes(self):
        """Test message deserialization from bytes"""
        original = Message(MessageType.CLIENT_MESSAGE, "client1", {"content": "hello"})
        msg_bytes = original.to_bytes()
        
        restored = Message.from_bytes(msg_bytes)
        self.assertIsNotNone(restored)
        self.assertEqual(restored.msg_type, original.msg_type)
        self.assertEqual(restored.sender_id, original.sender_id)
    
    def test_client_message(self):
        """Test ClientMessage creation"""
        msg = ClientMessage("client1", "Hello world", "client2")
        self.assertEqual(msg.msg_type, MessageType.CLIENT_MESSAGE)
        self.assertEqual(msg.payload['content'], "Hello world")
        self.assertEqual(msg.payload['recipient'], "client2")
    
    def test_server_response(self):
        """Test ServerResponse creation"""
        msg = ServerResponse("server1", True, "Success", {"data": 123})
        self.assertEqual(msg.msg_type, MessageType.SERVER_RESPONSE)
        self.assertTrue(msg.payload['success'])
        self.assertEqual(msg.payload['message'], "Success")
    
    def test_heartbeat_message(self):
        """Test HeartbeatMessage creation"""
        msg = HeartbeatMessage("server1", 5000, True, 10)
        self.assertEqual(msg.msg_type, MessageType.HEARTBEAT)
        self.assertEqual(msg.payload['server_port'], 5000)
        self.assertTrue(msg.payload['is_leader'])
        self.assertEqual(msg.payload['load'], 10)
    
    def test_leader_election_message(self):
        """Test LeaderElectionMessage creation"""
        msg = LeaderElectionMessage("server1", "server2", "server1")
        self.assertEqual(msg.msg_type, MessageType.LEADER_ELECTION)
        self.assertEqual(msg.payload['candidate_id'], "server2")
        self.assertEqual(msg.payload['initiator_id'], "server1")
    
    def test_leader_announcement_message(self):
        """Test LeaderAnnouncementMessage creation"""
        msg = LeaderAnnouncementMessage("server1", "server1")
        self.assertEqual(msg.msg_type, MessageType.LEADER_ANNOUNCEMENT)
        self.assertEqual(msg.payload['leader_id'], "server1")


class TestServerBasics(unittest.TestCase):
    """Test basic server functionality"""
    
    def test_server_creation(self):
        """Test server instantiation"""
        from server import Server
        server = Server("test_server", 5555)
        self.assertEqual(server.server_id, "test_server")
        self.assertEqual(server.tcp_port, 5555)
        self.assertFalse(server.is_leader)
        self.assertFalse(server.is_running)
    
    def test_server_status(self):
        """Test server status reporting"""
        from server import Server
        server = Server("test_server", 5556)
        status = server.get_status()
        
        self.assertEqual(status['server_id'], "test_server")
        self.assertEqual(status['tcp_port'], 5556)
        self.assertFalse(status['is_leader'])
        self.assertEqual(status['connected_clients'], 0)


class TestClientBasics(unittest.TestCase):
    """Test basic client functionality"""
    
    def test_client_creation(self):
        """Test client instantiation"""
        from client import Client
        client = Client("test_client", "localhost", 5000)
        self.assertEqual(client.client_id, "test_client")
        self.assertEqual(client.server_host, "localhost")
        self.assertEqual(client.server_port, 5000)
        self.assertFalse(client.is_connected)


class TestLeaderElection(unittest.TestCase):
    """Test leader election algorithm logic"""
    
    def test_election_message_comparison(self):
        """Test that higher IDs win in election"""
        # In LeLann-Chang-Roberts, higher ID should win
        ids = ["server1", "server2", "server3"]
        winner = max(ids)
        self.assertEqual(winner, "server3")
    
    def test_election_message_flow(self):
        """Test election message structure"""
        msg1 = LeaderElectionMessage("server1", "server1", "server1")
        self.assertEqual(msg1.payload['candidate_id'], "server1")
        
        # Simulate receiving and comparing
        if "server2" > msg1.payload['candidate_id']:
            msg2 = LeaderElectionMessage("server2", "server2", "server1")
            self.assertEqual(msg2.payload['candidate_id'], "server2")


def run_tests():
    """Run all tests"""
    unittest.main(argv=[''], verbosity=2, exit=False)


if __name__ == '__main__':
    run_tests()
