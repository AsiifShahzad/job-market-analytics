"""
FastAPI application — JobPulse AI backend.

Mounts all route modules and manages DB lifecycle.
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import structlog

from src.db.session import lifespan_db

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: create tables, start schedulers. Shutdown: close connections."""
    async with lifespan_db():
        logger.info("JobPulse AI backend ready")
        
        # Start snapshot scheduler for weekly aggregation
        logger.info("Starting snapshot scheduler for weekly aggregation...")
        try:
            from src.etl.scheduler import start_snapshot_scheduler
            start_snapshot_scheduler()
        except ImportError:
            logger.warning("APScheduler not available - snapshot scheduling disabled")
        except Exception as e:
            logger.error("Failed to start snapshot scheduler", error=str(e))
        
        # Automatically trigger pipeline data fetch in the background on startup
        logger.info("Triggering automated background job fetch from Adzuna API...")
        import asyncio
        from src.api.routes.pipeline import run_pipeline_direct
        asyncio.create_task(run_pipeline_direct(pages=2))  # Fetches 100 jobs on startup
        
        yield
        
        # Shutdown: stop scheduler
        try:
            from src.etl.scheduler import stop_snapshot_scheduler
            stop_snapshot_scheduler()
        except Exception:
            pass


app = FastAPI(
    title="JobPulse AI",
    description="Job market analytics API — Adzuna ETL + skill extraction",
    version="2.0.0",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routes ────────────────────────────────────────────────────────────────────
from src.api.routes.pipeline import router as pipeline_router
from src.api.routes.jobs import router as jobs_router
from src.api.routes.skills import router as skills_router
from src.api.routes.trends import router as trends_router
from src.api.routes.snapshots import router as snapshots_router
from src.api.routes.insights import router as insights_router
from src.api.routes.analytics import router as analytics_router

app.include_router(pipeline_router)
app.include_router(jobs_router)
app.include_router(skills_router)
app.include_router(trends_router)
app.include_router(snapshots_router)
app.include_router(insights_router)
app.include_router(analytics_router)


# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {
        "status": "healthy",
        "database": "neon-postgres",
        "timestamp": datetime.now(timezone.utc),
    }
