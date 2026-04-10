#!/usr/bin/env python
"""
Wrapper script to run FastAPI backend server
Fixes Python path issues and handles port conflicts
"""
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import socket
import subprocess
import time

# Load environment variables from .env file
load_dotenv(Path(__file__).parent / ".env")

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

def find_process_on_port(port):
    """Find and kill any process using the specified port"""
    try:
        # Try to connect to the port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('127.0.0.1', port))
        sock.close()
        
        if result == 0:  # Port is in use
            print(f"⚠️  Port {port} is already in use. Attempting to clear it...")
            try:
                # Kill any Python process that might be holding the port
                if os.name == 'nt':  # Windows
                    os.system(f'powershell -Command "Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force"')
                else:  # Unix/Linux
                    os.system(f"lsof -ti:{port} | xargs kill -9 2>/dev/null || true")
                time.sleep(2)
                print(f"✓ Cleared port {port}")
            except Exception as e:
                print(f"Could not clear port: {e}")
                raise
    except Exception as e:
        print(f"Error checking port: {e}")

# Import and run main
from src.api.main import app
import uvicorn

if __name__ == "__main__":
    port = int(os.getenv("API_PORT", 8000))
    
    # Clear the port before starting
    find_process_on_port(port)
    
    print(f"✓ Starting backend on port {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)
