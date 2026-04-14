"""
Production-grade Adzuna ETL ingestion pipeline.

GET  /api/pipeline/runs   — list recent pipeline executions
POST /api/pipeline/run    — trigger a new ETL run
"""

import os
import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc
import structlog

from src.db.session import get_db
from src.db.models import PipelineRun
from src.api.schemas import (
    PipelineRunsResponse, PipelineRunItem,
    PipelineRunResponse, PipelineRunStats,
)
from src.etl.fetcher import run_multi_keyword_fetch

logger = structlog.get_logger(__name__)
router = APIRouter(tags=["pipeline"])

# ══════════════════════════════════════════════════════════════════════════════
# GET /api/pipeline/runs — list recent pipeline executions
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/api/pipeline/runs", response_model=PipelineRunsResponse)
async def get_pipeline_runs(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(20, ge=1, le=100),
) -> PipelineRunsResponse:
    try:
        q = select(PipelineRun).order_by(desc(PipelineRun.started_at)).limit(limit)
        runs = (await db.execute(q)).scalars().all()
        total = (await db.execute(select(func.count(PipelineRun.id)))).scalar() or 0

        items = [
            PipelineRunItem(
                id=r.id,
                status=r.status,
                jobs_fetched=r.jobs_fetched,
                jobs_inserted=r.jobs_inserted,
                jobs_skipped=r.jobs_skipped,
                unique_skills=r.unique_skills,
                started_at=r.started_at,
                completed_at=r.finished_at,
                duration_seconds=(
                    (r.finished_at - r.started_at).total_seconds()
                    if r.finished_at else None
                ),
                error_message=r.error_message,
            )
            for r in runs
        ]
        return PipelineRunsResponse(runs=items, total_count=total)

    except Exception as e:
        logger.error("pipeline runs error", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch pipeline runs")


# ══════════════════════════════════════════════════════════════════════════════
# POST /api/pipeline/run — trigger a new ETL run
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/api/pipeline/run", response_model=PipelineRunResponse)
async def run_pipeline(
    db: AsyncSession = Depends(get_db),
    max_keywords: int = Query(5, ge=1, le=20),
) -> PipelineRunResponse:

    logger.info("═══ PIPELINE STARTED ═══", max_keywords=max_keywords)

    run = PipelineRun(started_at=datetime.now(timezone.utc), status="running")
    db.add(run)
    await db.flush()
    
    try:
        result = await run_multi_keyword_fetch(db, run, max_keywords)
        
        return PipelineRunResponse(
            status="success",
            message="Pipeline completed successfully",
            run_id=run.id,
            statistics=PipelineRunStats(**result["statistics"]),
            timestamp=datetime.now(timezone.utc),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Background startup entry point
# ══════════════════════════════════════════════════════════════════════════════

async def run_pipeline_direct(pages: int = 0) -> dict:
    """Called by the FastAPI lifespan directly to populate data on app start."""
    from src.db.session import async_session_maker

    async with async_session_maker() as db:
        run = PipelineRun(started_at=datetime.now(timezone.utc), status="running")
        db.add(run)
        await db.flush()
        
        # Limit to fetching just 3 groups in the background so it doesn't take 10 minutes
        # Note: the `pages` param is ignored here since max_keywords dictates size
        return await run_multi_keyword_fetch(db, run, max_keywords=3)

