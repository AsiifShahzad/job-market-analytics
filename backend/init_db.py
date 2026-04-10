#!/usr/bin/env python
"""
Initialize database on Neon (Render)
Creates all required tables and indexes
Run this on Render after deploying to ensure database is ready
"""

import asyncio
import os
import sys
from pathlib import Path

# Add backend directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import structlog
from sqlalchemy import text

from src.db.session import async_engine
from src.db.models import Base

logger = structlog.get_logger(__name__)


async def init_database():
    """Initialize all database tables"""
    
    logger.info("Starting database initialization...")
    
    try:
        # Test connection
        logger.info("Testing database connection...")
        async with async_engine.begin() as conn:
            result = await conn.execute(text("SELECT NOW()"))
            timestamp = result.scalar()
            logger.info(f"Database connection successful! Server time: {timestamp}")
        
        # Create all tables
        logger.info("Creating database tables...")
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        logger.info("Database tables created successfully!")
        
        # Verify tables exist
        logger.info("Verifying tables...")
        async with async_engine.begin() as conn:
            result = await conn.execute(
                text("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    ORDER BY table_name
                """)
            )
            tables = result.fetchall()
            
            if tables:
                logger.info(f"Found {len(tables)} tables:")
                for table in tables:
                    logger.info(f"  - {table[0]}")
            else:
                logger.warning("No tables found immediately after creation")
        
        logger.info("✓ Database initialization complete!")
        return True
        
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}", exc_info=True)
        return False
    finally:
        # Dispose of engine
        await async_engine.dispose()


if __name__ == "__main__":
    success = asyncio.run(init_database())
    sys.exit(0 if success else 1)
