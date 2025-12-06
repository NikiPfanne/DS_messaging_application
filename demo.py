#!/usr/bin/env python3
"""
Demo script for the distributed messaging system.
Starts multiple servers and shows their status.
"""

import sys
import time
import subprocess
import signal
import os

def print_header(text):
    """Print a formatted header"""
    print("\n" + "=" * 60)
    print(text)
    print("=" * 60)

def main():
    print_header("Distributed Messaging System - Demo")
    
    print("""
This demo will start 3 servers that will:
1. Discover each other via UDP multicast
2. Elect a leader using LeLann-Chang-Roberts algorithm
3. Monitor each other with heartbeats
4. Demonstrate fault tolerance

Server IDs are compared lexicographically, so:
- server3 > server2 > server1
- server3 will become the leader
""")
    
    input("Press Enter to start servers...")
    
    # Server configurations
    servers = [
        ('server1', 5001),
        ('server2', 5002),
        ('server3', 5003),
    ]
    
    processes = []
    
    try:
        # Start servers
        print_header("Starting Servers")
        for server_id, port in servers:
            print(f"Starting {server_id} on port {port}...")
            proc = subprocess.Popen(
                [sys.executable, 'server.py', server_id, str(port)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            processes.append((server_id, proc))
            time.sleep(0.5)
        
        print("\nAll servers started!")
        print("Waiting for leader election (5 seconds)...")
        time.sleep(5)
        
        print_header("Instructions")
        print("""
Servers are now running. You can:

1. Open new terminals and connect clients:
   python client.py alice localhost 5001
   python client.py bob localhost 5002
   python client.py charlie localhost 5003

2. Send messages between clients:
   alice> send Hello everyone!
   bob> send alice Hi Alice!

3. Test fault tolerance:
   - Stop server3 (the leader) with Ctrl+C
   - Watch remaining servers elect a new leader
   - Clients stay connected and keep working

4. Monitor server status in this window

Press Ctrl+C to stop all servers
""")
        
        # Monitor servers
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("\n\nShutting down servers...")
    
    finally:
        # Stop all servers
        for server_id, proc in processes:
            try:
                proc.terminate()
                proc.wait(timeout=2)
                print(f"Stopped {server_id}")
            except subprocess.TimeoutExpired:
                proc.kill()
                print(f"Killed {server_id}")
            except (OSError, subprocess.SubprocessError) as e:
                print(f"Error stopping {server_id}: {e}")
        
        print("\nAll servers stopped.")
        print("Demo complete!")

if __name__ == '__main__':
    main()
