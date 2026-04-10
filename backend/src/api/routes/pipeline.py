"""
Pipeline endpoints: manage and monitor job ingestion pipeline runs
POST /api/pipeline/trigger - start a new pipeline run (async background task)
GET /api/pipeline/runs - list recent pipeline runs
GET /api/pipeline/{run_id}/status - poll for run status and progress
"""

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, func
from datetime import datetime
from typing import List
import structlog
import asyncio

from src.db.models import PipelineRun, Job, Skill, JobSkill
from src.db.session import get_db
from src.api.schemas import (
    PipelineRunsResponse, PipelineRunSummary, PipelineStatus, PipelineTriggerResponse
)

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

# Store for in-flight runs (run_id -> status dict)
_active_runs = {}


async def real_pipeline_task(run_id: int, db: AsyncSession) -> None:
    """
    Background task that executes the real end-to-end pipeline.
    Fetches jobs → Extracts skills → Scores → Updates snapshots.
    """
    logger.info("Real pipeline task started", run_id=run_id)
    
    try:
        # Import the actual pipeline
        from src.flows.pipeline import run_full_pipeline
        
        # Execute the pipeline
        result = await run_full_pipeline(
            db=db,
            locations=["London", "Manchester", "Amsterdam", "Berlin"],
            countries=["gb", "nl", "de"],
            keywords_list=["python developer", "javascript developer", "data engineer"],
        )
        
        logger.info(
            "Pipeline task completed successfully",
            run_id=run_id,
            result=result,
        )
        
    except Exception as e:
        logger.error("Pipeline task failed", run_id=run_id, error=str(e), exc_info=True)
        
        # Update run as failed
        try:
            run_query = select(PipelineRun).where(PipelineRun.id == run_id)
            run_result = await db.execute(run_query)
            run = run_result.scalar_one_or_none()
            
            if run:
                run.status = "failed"
                run.finished_at = datetime.utcnow()
                run.error_message = str(e)[:500]
                await db.commit()
        except:
            logger.error("Failed to update run status in database")


@router.post("/trigger", response_model=PipelineTriggerResponse)
async def trigger_pipeline(
    db: AsyncSession = Depends(get_db),
    background_tasks: BackgroundTasks = None,
) -> PipelineTriggerResponse:
    """
    Trigger a new pipeline run.
    Returns immediately with run_id; pipeline runs in background.
    Poll /api/pipeline/{run_id}/status to check progress.
    """
    logger.info("Pipeline trigger requested")

    try:
        # Create new pipeline run record
        new_run = PipelineRun(
            started_at=datetime.utcnow(),
            status="running",
            jobs_fetched=0,
            jobs_inserted=0,
            jobs_skipped=0
        )
        db.add(new_run)
        await db.commit()
        await db.refresh(new_run)

        run_id = new_run.id
        logger.info("Pipeline run created", run_id=run_id)

        # Schedule background task
        if background_tasks:
            background_tasks.add_task(real_pipeline_task, run_id, db)
        else:
            logger.warning("No background tasks available, run will not execute")

        return PipelineTriggerResponse(
            run_id=run_id,
            status="running",
            message=f"Pipeline run {run_id} started. Poll /api/pipeline/{run_id}/status for progress."
        )

    except Exception as e:
        logger.error("Error triggering pipeline", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to trigger pipeline")


@router.get("/runs", response_model=PipelineRunsResponse)
async def get_pipeline_runs(
    db: AsyncSession = Depends(get_db),
    limit: int = 20,
) -> PipelineRunsResponse:
    """
    Get recent pipeline runs (last 20 by default).
    Useful for monitoring pipeline health and history.
    """
    logger.info("Getting pipeline runs", limit=limit)

    try:
        # Get recent runs
        query = (
            select(PipelineRun)
            .order_by(desc(PipelineRun.started_at))
            .limit(limit)
        )
        result = await db.execute(query)
        runs = result.scalars().all()

        # Get total count
        count_query = select(func.count(PipelineRun.id))
        count_result = await db.execute(count_query)
        total_count = count_result.scalar() or 0

        run_summaries = [
            PipelineRunSummary(
                id=run.id,
                started_at=run.started_at,
                finished_at=run.finished_at,
                status=run.status,
                jobs_fetched=run.jobs_fetched,
                jobs_inserted=run.jobs_inserted,
                jobs_skipped=run.jobs_skipped,
                error_message=run.error_message
            )
            for run in runs
        ]

        logger.info("Retrieved pipeline runs", count=len(run_summaries), total=total_count)

        return PipelineRunsResponse(
            runs=run_summaries,
            total_count=total_count,
            cache_status="MISS"
        )

    except Exception as e:
        logger.error("Error fetching pipeline runs", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch pipeline runs")


@router.get("/{run_id}/status", response_model=PipelineStatus)
async def get_pipeline_status(
    run_id: int,
    db: AsyncSession = Depends(get_db),
) -> PipelineStatus:
    """
    Poll for pipeline run status and progress.
    Returns current counts of fetched, inserted, skipped jobs.
    """
    logger.info("Getting pipeline status", run_id=run_id)

    try:
        query = select(PipelineRun).where(PipelineRun.id == run_id)
        result = await db.execute(query)
        run = result.scalar_one_or_none()

        if not run:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

        duration_seconds = None
        if run.finished_at:
            duration_seconds = (run.finished_at - run.started_at).total_seconds()

        logger.info(
            "Retrieved pipeline status",
            run_id=run_id,
            status=run.status,
            duration=duration_seconds
        )

        return PipelineStatus(
            run_id=run.id,
            status=run.status,
            started_at=run.started_at,
            finished_at=run.finished_at,
            jobs_fetched=run.jobs_fetched,
            jobs_inserted=run.jobs_inserted,
            jobs_skipped=run.jobs_skipped,
            error_message=run.error_message,
            duration_seconds=duration_seconds,
            cache_status="MISS"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching pipeline status", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch pipeline status")


# Import func for the query above
from sqlalchemy import func
