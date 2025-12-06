#!/usr/bin/env python3
"""
Simple example demonstrating the distributed messaging system.
This creates one server and two clients that exchange messages.
"""

import sys
import time
import threading
from server import Server
from client import Client


def run_server():
    """Run a test server"""
    server = Server("example_server", 5999)
    server.start()
    
    print("Server started on port 5999")
    print("Server is now accepting client connections...")
    print()
    
    try:
        while True:
            time.sleep(1)
            status = server.get_status()
            if status['connected_clients'] > 0:
                print(f"\rServer status: {status['connected_clients']} client(s) connected, "
                      f"Leader: {'YES' if status['is_leader'] else 'NO'}", end='', flush=True)
    except KeyboardInterrupt:
        print("\nStopping server...")
        server.stop()


def run_client(client_id, messages):
    """Run a test client"""
    client = Client(client_id, "localhost", 5999)
    
    print(f"[{client_id}] Connecting to server...")
    if not client.connect():
        print(f"[{client_id}] Failed to connect")
        return
    
    print(f"[{client_id}] Connected successfully!")
    
    # Set up message callback
    def on_message(sender, content):
        print(f"\n[{client_id}] Received from {sender}: {content}")
    
    client.set_message_callback(on_message)
    
    # Send messages
    time.sleep(1)
    for msg in messages:
        if isinstance(msg, tuple):
            recipient, content = msg
            print(f"[{client_id}] Sending to {recipient}: {content}")
            client.send_message(content, recipient)
        else:
            print(f"[{client_id}] Broadcasting: {msg}")
            client.send_message(msg)
        time.sleep(1)
    
    # Wait a bit before disconnecting
    time.sleep(2)
    
    print(f"[{client_id}] Disconnecting...")
    client.disconnect()


def main():
    """Run the example"""
    print("=" * 70)
    print("Distributed Messaging System - Simple Example")
    print("=" * 70)
    print()
    print("This example demonstrates:")
    print("  1. Server startup and client connections")
    print("  2. Private messaging between clients")
    print("  3. Broadcast messaging")
    print()
    print("=" * 70)
    print()
    
    # Start server in background
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    time.sleep(2)  # Give server time to start
    
    # Run two clients with different message patterns
    alice_messages = [
        "Hello everyone!",
        ("bob", "Hi Bob, how are you?"),
    ]
    
    bob_messages = [
        ("alice", "Hi Alice! I'm doing great!"),
        "Everyone, the server is working perfectly!",
    ]
    
    # Start clients
    alice_thread = threading.Thread(target=run_client, args=("alice", alice_messages))
    bob_thread = threading.Thread(target=run_client, args=("bob", bob_messages))
    
    alice_thread.start()
    time.sleep(0.5)  # Stagger client starts
    bob_thread.start()
    
    # Wait for clients to finish
    alice_thread.join()
    bob_thread.join()
    
    print()
    print("=" * 70)
    print("Example complete!")
    print()
    print("To try it yourself:")
    print("  1. Run: python server.py myserver 5001")
    print("  2. Run: python client.py alice localhost 5001")
    print("  3. Type: send Hello world!")
    print("=" * 70)


if __name__ == '__main__':
    main()
