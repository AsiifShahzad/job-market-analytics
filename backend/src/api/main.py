"""
FastAPI application for JobPulse AI backend
Integrates all routes, middleware, and lifespan management
"""

import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime

# Add backend directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import structlog
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_db, lifespan_context, async_engine, async_session_maker
from src.db.models import Base
from src.api.routes import skills, salaries, pipeline, trends, adzuna_direct, jobs, pipeline_trigger
from src.api.schemas import HealthResponse
from src.api.cache import get_cache_stats, clear_cache

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
)

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI application.
    Creates database tables on startup, disposes connections on shutdown.
    """
    logger.info("JobPulse AI Backend starting up")
    
    # Startup - skip DB init for now
    try:
        async with lifespan_context():
            yield
    except Exception as e:
        logger.warning(f"Database connection failed: {e}. Starting without DB.")
        yield
    
    # Shutdown
    logger.info("JobPulse AI Backend shutting down")


def create_app() -> FastAPI:
    """Create and configure FastAPI application"""
    
    app = FastAPI(
        title="JobPulse AI",
        description="Job Market Intelligence API",
        version="1.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json"
    )

    # CORS configuration - allow frontend on localhost and production URLs
    allowed_origins = [
        "http://localhost:5173",
        "http://localhost:3000",
        "http://localhost:8000",
        "https://job-market-analytics-p4sy.onrender.com",
    ]
    
    # Add environment-specific origins
    env = os.getenv("ENVIRONMENT", "development")
    if env == "production":
        # Add any production frontend URLs here
        pass
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Cache"],
    )

    # Include routers
    app.include_router(skills.router)
    app.include_router(salaries.router)
    app.include_router(pipeline.router)
    app.include_router(pipeline_trigger.router)  # Real ETL pipeline trigger
    app.include_router(trends.router)
    app.include_router(adzuna_direct.router)  # Direct Adzuna endpoints
    app.include_router(jobs.router)  # Jobs from database

    # Health check endpoint - NO database dependency
    @app.get("/api/health", response_model=HealthResponse)
    async def health_check() -> HealthResponse:
        """
        Health check endpoint.
        Returns service status.
        """
        # Don't check DB here - it may hang
        return HealthResponse(
            status="healthy",
            database="unknown",  # Don't require DB check
            timestamp=datetime.utcnow(),
            cache_stats=get_cache_stats()
        )

    # Utility endpoints
    @app.post("/api/cache/clear")
    async def clear_response_cache():
        """
        Manually clear the in-process response cache.
        Useful for invalidating stale data.
        """
        logger.info("Cache clear requested")
        clear_cache()
        return {"message": "Cache cleared successfully"}

    @app.get("/api/cache/stats")
    async def cache_statistics():
        """Get current cache statistics and utilization"""
        return get_cache_stats()

    # Root endpoint
    @app.get("/")
    async def root():
        """Root endpoint - redirects to API docs"""
        return {
            "message": "JobPulse AI Backend",
            "docs": "/api/docs",
            "version": "1.0.0"
        }

    logger.info("FastAPI application created successfully")
    return app


# Create application instance
app = create_app()

# Set lifespan
app.router.lifespan_context = app_lifespan


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("API_PORT", 8000))
    debug = os.getenv("DEBUG", "false").lower() == "true"
    
    logger.info("Starting Uvicorn server", port=port, debug=debug)
    
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=port,
        reload=debug,
        log_config=None,  # Use structlog
    )
