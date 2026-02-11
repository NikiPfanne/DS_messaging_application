"""
Configuration for the distributed messaging system.
"""

# Server configuration
# WICHTIG: 239.x.x.x ist "administratively scoped" und wird besser durch 
# Router/WLAN weitergeleitet als 224.0.0.x (Local Network Control Block)
DEFAULT_MULTICAST_GROUP = '239.255.255.250'
DEFAULT_MULTICAST_PORT = 5007

# Timing configuration (in seconds)
HEARTBEAT_INTERVAL = 2.0
FAILURE_TIMEOUT = 6.0  # 3 missed heartbeats
ELECTION_TIMEOUT = 5.0

# Capacity limits
MAX_CLIENTS_PER_SERVER = 100
MAX_MESSAGE_SIZE = 1024 * 1024  # 1MB

# Network configuration
TCP_TIMEOUT = 30.0
UDP_TIMEOUT = 1.0

# Example server configurations for testing
EXAMPLE_SERVERS = [
    {'id': 'server1', 'port': 5001},
    {'id': 'server2', 'port': 5002},
    {'id': 'server3', 'port': 5003},
]
