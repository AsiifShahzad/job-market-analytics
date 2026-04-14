"""
Async SQLAlchemy session management.
Cloud-only: connects exclusively to Neon (PostgreSQL).
Crashes on startup if DATABASE_URL is not set — no silent localhost fallback.
"""

import os
import asyncio
from typing import AsyncGenerator
from contextlib import asynccontextmanager

from dotenv import load_dotenv
load_dotenv()  # load .env before reading DATABASE_URL

from sqlalchemy.ext.asyncio import (
    AsyncSession, create_async_engine, async_sessionmaker, AsyncEngine
)
from sqlalchemy.pool import NullPool
import structlog

from .models import Base

logger = structlog.get_logger(__name__)

# ── Database URL — Neon only, no localhost fallback ───────────────────────────

DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable is not set. "
        "Add it to your .env file (locally) or Render environment variables (production). "
        "It should be your Neon connection string."
    )

# Normalize postgres:// / postgresql:// → postgresql+asyncpg://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Reject any accidental localhost / SQLite URL
_lower = DATABASE_URL.lower()
if "localhost" in _lower or "127.0.0.1" in _lower or "sqlite" in _lower:
    raise RuntimeError(
        f"DATABASE_URL points to a local database — this app only supports Neon (cloud Postgres). "
        f"Check your .env file. URL starts with: {DATABASE_URL[:60]}"
    )

logger.info("Using Neon database", url_prefix=DATABASE_URL[:40] + "...")


# ── Engine — NullPool for Neon serverless ─────────────────────────────────────
# NullPool opens a fresh connection per request instead of keeping a pool.
# Neon serverless drops idle connections after ~5 min; a standard pool
# thinks those connections are alive and gets "SSL connection closed" errors.
# NullPool avoids this entirely.

def _build_engine() -> AsyncEngine:
    return create_async_engine(
        DATABASE_URL,
        echo=os.getenv("SQL_ECHO", "false").lower() == "true",
        future=True,
        poolclass=NullPool,
        connect_args={
            "server_settings": {
                "application_name": "jobpulse-ai",
                "jit": "off",
            },
            "timeout": 60,        # Neon free tier can take 20-30s to cold-start
            "command_timeout": 30,
            "ssl": "require",     # Neon requires SSL — explicit is safer than implicit
        },
    )


async_engine = _build_engine()

async_session_maker = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


# ── Session dependency ────────────────────────────────────────────────────────

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency — yields a DB session.
    Retries once on connection error to handle Neon cold-starts gracefully.
    """
    last_exc = None

    for attempt in range(2):
        try:
            async with async_session_maker() as session:
                try:
                    yield session
                    await session.commit()
                    return
                except Exception:
                    await session.rollback()
                    raise
                finally:
                    await session.close()

        except Exception as exc:
            last_exc = exc
            is_connection_error = any(
                phrase in str(exc).lower()
                for phrase in [
                    "connection", "timeout", "ssl",
                    "could not connect", "server closed the connection",
                ]
            )
            if attempt == 0 and is_connection_error:
                logger.warning(
                    "Neon connection failed, retrying in 3s",
                    attempt=attempt,
                    error=str(exc)[:200],
                )
                await asyncio.sleep(3)
                continue
            raise

    raise last_exc


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan_db():
    """Creates tables on startup, disposes engine on shutdown."""
    try:
        async with async_engine.begin() as conn:
            await asyncio.wait_for(
                conn.run_sync(Base.metadata.create_all),
                timeout=60.0,
            )
        logger.info("Neon database tables ready")

    except asyncio.TimeoutError:
        logger.error("Neon DB table creation timed out after 60s")
    except Exception as e:
        logger.error(
            "Neon DB init failed",
            error=str(e),
            error_type=type(e).__name__,
        )

    yield

    try:
        await async_engine.dispose()
        logger.info("Neon database connections closed")
    except Exception as e:
        logger.warning("Error closing Neon connections", error=str(e))