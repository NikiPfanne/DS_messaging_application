"""
Message protocol for the distributed messaging system.
Defines message types and serialization/deserialization logic.
"""

import json
import time
from typing import Dict, Any, Optional
from enum import Enum
import uuid

class MessageType(Enum):
    """Types of messages in the system"""
    # Client-Server messages
    CLIENT_MESSAGE = "CLIENT_MESSAGE"
    SERVER_RESPONSE = "SERVER_RESPONSE"
    CLIENT_REGISTER = "CLIENT_REGISTER"
    CLIENT_UNREGISTER = "CLIENT_UNREGISTER"
    
    # Server-Server coordination messages (UDP multicast)
    HEARTBEAT = "HEARTBEAT"
    LEADER_ELECTION = "LEADER_ELECTION"
    LEADER_ANNOUNCEMENT = "LEADER_ANNOUNCEMENT"
    SERVER_DISCOVERY = "SERVER_DISCOVERY"
    SERVER_ANNOUNCE = "SERVER_ANNOUNCE"
    LOAD_INFO = "LOAD_INFO"
    
    # Message delivery
    FORWARD_MESSAGE = "FORWARD_MESSAGE"
    MESSAGE_ACK = "MESSAGE_ACK"


class Message:
    """Base message class for all communication"""
    
    def __init__(self, msg_type: MessageType, sender_id: str, payload: Dict[str, Any] = None, message_id: str = None):
        self.msg_type = msg_type
        self.sender_id = sender_id
        self.payload = payload or {}
        self.timestamp = time.time()
        self.message_id = message_id or str(uuid.uuid4())
    
    def to_json(self) -> str:
        """Serialize message to JSON string"""
        data = {
            'type': self.msg_type.value,
            'sender_id': self.sender_id,
            'payload': self.payload,
            'timestamp': self.timestamp,
            'message_id': self.message_id
        }
        return json.dumps(data)
    
    def to_bytes(self) -> bytes:
        """Convert message to bytes for network transmission"""
        json_str = self.to_json()
        # Add length prefix (4 bytes) for proper framing
        length = len(json_str.encode('utf-8'))
        return length.to_bytes(4, byteorder='big') + json_str.encode('utf-8')
    
    @staticmethod
    def from_json(json_str: str) -> 'Message':
        """Deserialize message from JSON string"""
        data = json.loads(json_str)
        msg = Message(
            MessageType(data['type']),
            data['sender_id'],
            data.get('payload', {})
        )
        msg.timestamp = data.get('timestamp', time.time())
        msg.message_id = data.get('message_id', msg.message_id)
        return msg
    
    @staticmethod
    def from_bytes(data: bytes) -> Optional['Message']:
        """Deserialize message from bytes"""
        if len(data) < 4:
            return None
        
        # Extract length and JSON data
        length = int.from_bytes(data[:4], byteorder='big')
        if len(data) < 4 + length:
            return None
        
        json_str = data[4:4+length].decode('utf-8')
        return Message.from_json(json_str)
    
    def __repr__(self):
        return f"Message(type={self.msg_type.value}, sender={self.sender_id}, id={self.message_id})"


class ClientMessage(Message):
    def __init__(self, sender_id: str, content: str, recipient: Optional[str] = None,
                 seq: Optional[int] = None, message_id: Optional[str] = None):
        payload = {'content': content, 'recipient': recipient}
        if seq is not None:
            payload['seq'] = seq
        super().__init__(MessageType.CLIENT_MESSAGE, sender_id, payload, message_id=message_id)



class ServerResponse(Message):
    """Response from server to client"""
    
    def __init__(self, sender_id: str, success: bool, message: str, data: Dict = None):
        payload = {
            'success': success,
            'message': message,
            'data': data or {}
        }
        super().__init__(MessageType.SERVER_RESPONSE, sender_id, payload)


class HeartbeatMessage(Message):
    """Heartbeat message for fault detection"""
    
    def __init__(self, sender_id: str, server_port: int, is_leader: bool, load: int):
        payload = {
            'server_port': server_port,
            'is_leader': is_leader,
            'load': load  # Number of connected clients
        }
        super().__init__(MessageType.HEARTBEAT, sender_id, payload)


class LeaderElectionMessage(Message):
    """Message for LeLann-Chang-Roberts leader election"""
    
    def __init__(self, sender_id: str, candidate_id: str, initiator_id: str):
        payload = {
            'candidate_id': candidate_id,
            'initiator_id': initiator_id
        }
        super().__init__(MessageType.LEADER_ELECTION, sender_id, payload)


class LeaderAnnouncementMessage(Message):
    """Announcement of new leader"""
    
    def __init__(self, sender_id: str, leader_id: str):
        payload = {
            'leader_id': leader_id
        }
        super().__init__(MessageType.LEADER_ANNOUNCEMENT, sender_id, payload)


class LoadInfoMessage(Message):
    """Load information for load balancing"""
    
    def __init__(self, sender_id: str, load: int, capacity: int):
        payload = {
            'load': load,
            'capacity': capacity
        }
        super().__init__(MessageType.LOAD_INFO, sender_id, payload)
