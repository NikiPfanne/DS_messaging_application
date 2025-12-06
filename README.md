# DS_messaging_application

A distributed messaging application built with a client–server architecture. Clients connect via TCP, while servers coordinate using UDP multicast. The system includes leader election (LeLann–Chang–Roberts), heartbeat-based fault detection, dynamic server discovery, and load balancing to ensure high availability and crash fault tolerance.

## Features

- ✅ **TCP Client-Server Communication**: Reliable message delivery via TCP
- ✅ **UDP Multicast Coordination**: Servers coordinate via UDP multicast
- ✅ **Leader Election**: LeLann-Chang-Roberts algorithm for distributed leader election
- ✅ **Fault Detection**: Heartbeat-based monitoring with automatic failure detection
- ✅ **Load Balancing**: Servers share load information for optimal distribution
- ✅ **High Availability**: Automatic recovery from server failures
- ✅ **Message Routing**: Both broadcast and private messaging supported

## Quick Start

### 1. Start Servers

Start multiple servers on different ports:

```bash
# Terminal 1
python server.py server1 5001

# Terminal 2
python server.py server2 5002

# Terminal 3
python server.py server3 5003
```

### 2. Connect Clients

Connect clients to any server:

```bash
# Terminal 4
python client.py alice localhost 5001

# Terminal 5
python client.py bob localhost 5002
```

### 3. Send Messages

In the client interface:
```
alice> send Hello everyone!              # Broadcast message
alice> send bob Hi Bob!                  # Private message
alice> quit                              # Disconnect
```

## Architecture

```
┌─────────┐         ┌─────────┐         ┌─────────┐
│ Client1 │◄───TCP──┤ Server1 │◄──UDP───┤ Server2 │
└─────────┘         └────┬────┘         └────┬────┘
                         │                    │
┌─────────┐         ┌────▼────┐         ┌────▼────┐
│ Client2 │◄───TCP──┤ Server3 │◄──UDP───┤ Leader  │
└─────────┘         └─────────┘         │Election │
                                        └─────────┘
```

## Components

- **`protocol.py`**: Message types and serialization
- **`server.py`**: Server with TCP listener and UDP coordination
- **`client.py`**: Client with interactive messaging interface
- **`config.py`**: System configuration
- **`test_messaging.py`**: Unit tests for protocol
- **`test_integration.py`**: Integration tests for client-server
- **`demo.py`**: Automated demo script
- **`USAGE.md`**: Comprehensive documentation

## Testing

Run tests:
```bash
python test_messaging.py        # Unit tests
python test_integration.py      # Integration tests
```

All tests pass successfully.

## Documentation

See [USAGE.md](USAGE.md) for detailed documentation including:
- Architecture details
- Leader election algorithm
- Fault detection mechanism
- Load balancing strategy
- Example scenarios
- Troubleshooting guide

## Requirements

- Python 3.6+
- No external dependencies (uses standard library only)

## Implementation Details

### Leader Election (LeLann-Chang-Roberts)
- Ring-based algorithm where highest server ID becomes leader
- Automatic re-election on leader failure
- Election messages circulate through all servers

### Heartbeat & Fault Detection
- Heartbeats sent every 2 seconds via UDP multicast
- Server marked as failed after 3 missed heartbeats (6 seconds)
- Automatic leader re-election when leader fails

### Load Balancing
- Servers broadcast load information (connected clients)
- Clients can connect to least-loaded server
- Dynamic load redistribution support

## License

Educational project for demonstrating distributed systems concepts.
