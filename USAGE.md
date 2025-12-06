# Distributed Messaging System - Usage Guide

## Overview

This is a distributed messaging application with a client-server architecture. Clients connect via TCP, while servers coordinate using UDP multicast. The system includes:

- **Leader Election**: LeLann-Chang-Roberts algorithm
- **Fault Detection**: Heartbeat-based monitoring
- **Load Balancing**: Dynamic server discovery
- **High Availability**: Crash fault tolerance

## Architecture

```
Clients (TCP) <---> Servers <---> Servers (UDP Multicast)
                      |
                   Leader Election
                   Fault Detection
                   Load Balancing
```

## Components

### 1. Protocol (`protocol.py`)
Defines message types and serialization for:
- Client-server communication (TCP)
- Server-server coordination (UDP multicast)
- Leader election messages
- Heartbeat and fault detection

### 2. Server (`server.py`)
Features:
- TCP socket for client connections
- UDP multicast for server coordination
- LeLann-Chang-Roberts leader election
- Heartbeat sender and receiver
- Failure detection (3 missed heartbeats)
- Load information broadcasting
- Message routing and delivery

### 3. Client (`client.py`)
Features:
- TCP connection to any server
- Send broadcast or private messages
- Receive messages asynchronously
- Interactive command-line interface

### 4. Configuration (`config.py`)
System-wide configuration including:
- Multicast group and port
- Timing parameters
- Capacity limits

## Quick Start

### Starting Servers

Start multiple servers on different ports:

```bash
# Terminal 1
python server.py server1 5001

# Terminal 2
python server.py server2 5002

# Terminal 3
python server.py server3 5003
```

Servers will automatically:
- Discover each other via UDP multicast
- Elect a leader using LeLann-Chang-Roberts
- Monitor each other with heartbeats
- Re-elect if the leader fails

### Starting Clients

Connect clients to any server:

```bash
# Terminal 4
python client.py alice localhost 5001

# Terminal 5
python client.py bob localhost 5002

# Terminal 6
python client.py charlie localhost 5003
```

### Sending Messages

In the client interface:

```
alice> send Hello everyone!              # Broadcast message
alice> send bob Hi Bob, how are you?     # Private message to bob
alice> quit                               # Disconnect
```

## Leader Election Algorithm

The system uses the **LeLann-Chang-Roberts** algorithm:

1. When a server starts or detects leader failure, it initiates election
2. Server sends election message with its own ID as candidate
3. Each server compares received candidate ID with its own:
   - If received ID > own ID: forward the message
   - If own ID > received ID: substitute own ID as candidate
4. When initiator receives message with its own ID, it becomes leader
5. Leader announces itself to all servers

### Election Properties
- **Ring-based**: Messages circulate through all servers
- **ID-based**: Server with highest ID becomes leader
- **Crash-tolerant**: Re-election triggered on leader failure

## Heartbeat and Fault Detection

### Heartbeat Mechanism
- Every server sends heartbeat every 2 seconds via UDP multicast
- Heartbeat contains: server_id, port, is_leader, load

### Failure Detection
- Server considered failed after 3 missed heartbeats (6 seconds)
- If leader fails: automatic re-election
- If follower fails: removed from known servers list

## Load Balancing

Servers share load information in heartbeats:
- Number of connected clients
- Current capacity
- Leader status

This allows:
- Clients to choose least-loaded server
- Dynamic redistribution of load
- Better resource utilization

## Testing

Run the test suite:

```bash
python test_messaging.py
```

Tests cover:
- Message serialization/deserialization
- Protocol correctness
- Server and client basics
- Leader election logic

## Example Scenarios

### Scenario 1: Normal Operation
1. Start 3 servers (server1, server2, server3)
2. Servers elect server3 as leader (highest ID)
3. Connect clients to different servers
4. Clients can message each other regardless of which server they're connected to

### Scenario 2: Leader Failure
1. Start 3 servers, server3 becomes leader
2. Stop server3 (Ctrl+C)
3. Other servers detect failure after 6 seconds
4. Servers re-elect server2 as new leader
5. System continues operating normally

### Scenario 3: Load Balancing
1. Start 3 servers
2. Connect 10 clients to server1
3. New clients can check server loads and connect to less-loaded servers
4. System distributes clients across servers

## Network Configuration

### TCP (Client-Server)
- Each server listens on a unique port (e.g., 5001, 5002, 5003)
- Clients connect to any available server
- Reliable, connection-oriented communication

### UDP Multicast (Server-Server)
- Multicast group: 224.0.0.1
- Multicast port: 5007
- Servers send/receive: heartbeats, election messages, announcements
- Unreliable but efficient for coordination

## Security Considerations

This is an educational implementation demonstrating distributed systems concepts. For production use, consider:

### Network Security
- **Bind to all interfaces**: The server binds to `0.0.0.0` and uses UDP multicast on all interfaces. This is intentional for a distributed system but should be restricted in production environments using firewalls or network segmentation
- **Authentication and authorization**: Implement client authentication and message authorization
- **Encrypted communications**: Use TLS/SSL for TCP connections and encrypt UDP multicast messages
- **Message integrity checks**: Add HMAC or digital signatures to prevent tampering

### Application Security
- **Rate limiting and DoS protection**: Implement connection and message rate limits
- **Input validation and sanitization**: Validate all message content and metadata
- **Resource limits**: Enforce maximum message size, client connections, and memory usage
- **Audit logging**: Log all security-relevant events for monitoring and forensics

### Deployment Recommendations
- Deploy servers in a trusted network segment
- Use firewall rules to restrict access to specific IP ranges
- Monitor for unusual traffic patterns or connection attempts
- Implement TLS/SSL certificates for production deployments
- Consider using VPN or private networks for server-to-server communication

## Troubleshooting

### Servers not discovering each other
- Check firewall settings for UDP multicast
- Ensure multicast is enabled on network interface
- Try running on same machine with localhost

### Clients cannot connect
- Verify server is running and port is correct
- Check firewall rules for TCP connections
- Ensure server is accepting connections (not at max capacity)

### Leader election not working
- Check UDP multicast is functioning
- Verify server IDs are unique and comparable
- Check logs for election messages

## Performance Characteristics

- **Latency**: Low (direct TCP connections)
- **Throughput**: Limited by network and server capacity
- **Scalability**: Supports multiple servers and clients
- **Fault Tolerance**: Automatic recovery from single server failures
- **Consistency**: Best-effort delivery, no guaranteed ordering

## Future Enhancements

Possible improvements:
- Message persistence and delivery guarantees
- Server-side message queuing
- Multi-leader support for partitioned networks
- Client failover and reconnection
- Message encryption and authentication
- Distributed consensus (e.g., Raft, Paxos)
- Monitoring and metrics dashboard

## License

This is an educational project for demonstrating distributed systems concepts.
