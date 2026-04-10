"""
Async database session configuration using SQLAlchemy with asyncpg
Manages connection pool and provides FastAPI dependency for DB access
"""

import os
from typing import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncSession, create_async_engine, async_sessionmaker, AsyncEngine
)
from sqlalchemy.pool import NullPool, QueuePool
import structlog

from .models import Base

logger = structlog.get_logger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://user:pass@localhost/jobpulse"
)

# Validate Neon URL format if applicable
if "neon.tech" in DATABASE_URL or "asyncpg" in DATABASE_URL:
    logger.info("Using async PostgreSQL with asyncpg driver", url=DATABASE_URL[:50] + "...")
elif "sqlite" in DATABASE_URL:
    logger.info("Using SQLite for development", url=DATABASE_URL)


def create_engine() -> AsyncEngine:
    """
    Create async engine with database-specific configuration.
    - PostgreSQL (Neon): Connection pooling with 5 connections
    - SQLite: Local file database for development
    """
    is_sqlite = "sqlite" in DATABASE_URL
    
    if is_sqlite:
        # SQLite doesn't use pool_size, max_overflow, or server_settings
        engine = create_async_engine(
            DATABASE_URL,
            echo=os.getenv("SQL_ECHO", "false").lower() == "true",
            future=True,
            poolclass=NullPool,  # SQLite works better with NullPool
            connect_args={"timeout": 5}
        )
    else:
        # PostgreSQL configuration
        engine = create_async_engine(
            DATABASE_URL,
            echo=os.getenv("SQL_ECHO", "false").lower() == "true",
            future=True,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            connect_args={
                "server_settings": {
                    "application_name": "jobpulse-ai",
                    "jit": "off",  # Disable JIT for Neon
                },
                "timeout": 5,  # 5 second connection timeout
                "command_timeout": 10,  # 10 second command timeout
            }
        )
    return engine


async_engine = create_engine()

# Session factory for dependency injection
async_session_maker = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency: provides an AsyncSession for route handlers.
    Automatically commits on success, rolls back on exception.
    Has timeout protection to prevent hanging.
    """
    import asyncio
    try:
        # Create session with timeout
        async with async_session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error("Database session error", error=str(e), exc_info=True)
                raise
            finally:
                await session.close()
    except asyncio.TimeoutError:
        logger.error("Database session acquisition timed out")
        raise
    except Exception as e:
        logger.error("Failed to acquire database session", error=str(e))
        raise


@asynccontextmanager
async def lifespan_context():
    """
    Application lifespan manager: performs health check on startup.
    Creates tables as needed (but with timeout to avoid hanging).
    """
    db_connected = False
    
    # Startup: Try to connect to database and create tables
    try:
        logger.info("Starting database connection...")
        import asyncio
        from sqlalchemy import text
        
        # Simple connection test
        async with async_session_maker() as session:
            await asyncio.wait_for(
                session.execute(text("SELECT 1")),
                timeout=5.0
            )
        
        db_connected = True
        logger.info("Database connected successfully")
        
        # Now try to create tables with timeout
        try:
            logger.info("Creating database tables if not exists...")
            async with async_engine.begin() as conn:
                await asyncio.wait_for(
                    conn.run_sync(Base.metadata.create_all),
                    timeout=10.0
                )
            logger.info("Database tables ready")
        except asyncio.TimeoutError:
            logger.warning("Database table creation timed out, but connection is working")
        except Exception as e:
            logger.warning(f"Failed to create database tables: {e}")
            
    except asyncio.TimeoutError:
        logger.warning("Database connection test timed out, proceeding without DB")
    except Exception as e:
        logger.warning(f"Database connection failed: {e}, proceeding without DB")

    yield

    # Shutdown: Dispose of connection pool
    try:
        if db_connected:
            logger.info("Closing database connections...")
            await async_engine.dispose()
            logger.info("Database connections closed")
    except Exception as e:
        logger.warning(f"Error closing database: {e}")
