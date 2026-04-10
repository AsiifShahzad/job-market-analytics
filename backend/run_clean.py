#!/usr/bin/env python
"""
Clean Backend Runner - No Database Dependency
Uses simplified RESTful API with clear pipeline stages
"""
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(Path(__file__).parent / ".env")

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

# Import and run main
from src.api.main_new import app
import uvicorn

if __name__ == "__main__":
    port = int(os.getenv("API_PORT", 8000))
    print(f"\n{'='*70}")
    print(f"🚀 JobPulse AI Backend Starting...")
    print(f"{'='*70}")
    print(f"🌐 API URL: http://0.0.0.0:{port}")
    print(f"📚 Docs: http://localhost:{port}/api/docs")
    print(f"✅ Status: No database required - Using mock data")
    print(f"{'='*70}\n")
    
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
