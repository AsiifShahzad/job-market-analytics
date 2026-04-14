"""
GET  /api/snapshots/latest  — get latest snapshot date
POST /api/snapshots/build   — trigger snapshot build
GET  /api/snapshots/skill/{skill_id}  — get recent snapshots for skill
GET  /api/snapshots/growth/{skill_id}  — get growth rate for skill
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_db
from src.etl.snapshots import (
    build_skill_snapshots,
    get_latest_snapshot_date,
    get_skill_snapshots,
    calculate_skill_growth,
    cleanup_old_snapshots,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["snapshots"])


# ══════════════════════════════════════════════════════════════════════════════
# Models
# ══════════════════════════════════════════════════════════════════════════════

from pydantic import BaseModel


class SnapshotBuildResponse(BaseModel):
    """Response from snapshot build endpoint."""
    status: str  # "success" or "error"
    snapshot_date: datetime
    snapshots_created: int
    snapshots_updated: int
    skills_processed: int
    locations_processed: int
    duration_seconds: float
    errors: int
    error_messages: list = []


class SnapshotItem(BaseModel):
    """Single snapshot record."""
    snapshot_date: datetime
    job_count: int
    avg_salary_mid: Optional[float]
    city: Optional[str]
    country: Optional[str]


class SkillGrowthResponse(BaseModel):
    """Skill growth metrics."""
    skill_id: int
    growth_rate: Optional[float]
    period_days: int
    trend: str  # "growing", "declining", "stable", "insufficient_data"


# ══════════════════════════════════════════════════════════════════════════════
# Endpoints
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/api/snapshots/latest")
async def get_latest_snapshot(
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Returns the date of the most recent snapshot.
    
    Useful for checking if today's snapshot has been built.
    """
    try:
        latest_date = await get_latest_snapshot_date(db)
        
        if not latest_date:
            return {
                "status": "no_snapshots",
                "message": "No snapshots have been built yet"
            }
        
        return {
            "status": "success",
            "latest_snapshot_date": latest_date,
            "days_old": (datetime.now(timezone.utc) - latest_date).days,
        }
    
    except Exception as e:
        logger.error("Failed to get latest snapshot", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/snapshots/build", response_model=SnapshotBuildResponse)
async def trigger_snapshot_build(
    db: AsyncSession = Depends(get_db),
    snapshot_date: Optional[str] = Query(None, description="ISO date string (default: today)"),
    retention_days: int = Query(365, description="Cleanup snapshots older than N days"),
) -> SnapshotBuildResponse:
    """
    Triggers snapshot building for today (or specified date).
    
    Process:
    1. Aggregates skill demand from job_skill + job tables
    2. Groups by skill_id, city, country
    3. Upserts into skill_snapshot table
    4. Optionally cleans up old snapshots
    
    Args:
        snapshot_date: ISO format date (e.g., "2026-04-14"). Default: today
        retention_days: Keep snapshots from last N days. Set to 0 to skip cleanup
    
    Returns:
        Metrics and status of snapshot build
    
    Example:
        POST /api/snapshots/build
        POST /api/snapshots/build?snapshot_date=2026-04-14&retention_days=365
    """
    try:
        # Parse snapshot date if provided
        parsed_date = None
        if snapshot_date:
            try:
                parsed_date = datetime.fromisoformat(snapshot_date).date()
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid date format. Use ISO format: YYYY-MM-DD"
                )
        
        logger.info(
            "Snapshot build triggered",
            snapshot_date=snapshot_date,
            retention_days=retention_days,
        )
        
        # Build snapshots
        report = await build_skill_snapshots(db, snapshot_date=parsed_date)
        
        # Cleanup old snapshots if requested
        if retention_days > 0:
            deleted = await cleanup_old_snapshots(db, retention_days=retention_days)
            logger.info("Cleaned up old snapshots", deleted=deleted)
        
        return SnapshotBuildResponse(
            status="success",
            snapshot_date=report.snapshot_date,
            snapshots_created=report.total_snapshots_created,
            snapshots_updated=report.total_snapshots_updated,
            skills_processed=report.skills_processed,
            locations_processed=report.locations_processed,
            duration_seconds=report.duration_seconds,
            errors=report.errors,
            error_messages=report.error_messages,
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Snapshot build failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/snapshots/skill/{skill_id}", response_model=dict)
async def get_skill_snapshot_history(
    skill_id: int,
    db: AsyncSession = Depends(get_db),
    days: int = Query(30, ge=1, le=365, description="How many days back"),
) -> dict:
    """
    Retrieves recent snapshots for a specific skill.
    
    Useful for building trend charts and historical comparisons.
    
    Args:
        skill_id: Skill database ID
        days: Number of days back to retrieve (1-365)
    
    Returns:
        List of snapshots with job counts and avg salaries by location
    """
    try:
        snapshots = await get_skill_snapshots(db, skill_id=skill_id, days=days)
        
        if not snapshots:
            return {
                "status": "no_data",
                "skill_id": skill_id,
                "message": f"No snapshots found for skill in last {days} days"
            }
        
        return {
            "status": "success",
            "skill_id": skill_id,
            "days_retrieved": days,
            "snapshot_count": len(snapshots),
            "snapshots": snapshots,
        }
    
    except Exception as e:
        logger.error("Failed to get skill snapshots", skill_id=skill_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/snapshots/growth/{skill_id}", response_model=SkillGrowthResponse)
async def get_skill_growth(
    skill_id: int,
    db: AsyncSession = Depends(get_db),
    period_days: int = Query(7, ge=1, le=90, description="Period for growth calculation"),
) -> SkillGrowthResponse:
    """
    Calculates period-over-period growth rate for a skill.
    
    Formula: (current_period_count - previous_period_count) / previous_period_count
    
    Args:
        skill_id: Skill database ID
        period_days: Days per period (e.g., 7 for week-over-week)
    
    Returns:
        Growth rate and trend classification
    """
    try:
        growth_rate = await calculate_skill_growth(db, skill_id=skill_id, days=period_days)
        
        if growth_rate is None:
            trend = "insufficient_data"
        elif growth_rate > 0.05:
            trend = "growing"
        elif growth_rate < -0.05:
            trend = "declining"
        else:
            trend = "stable"
        
        return SkillGrowthResponse(
            skill_id=skill_id,
            growth_rate=round(growth_rate, 4) if growth_rate is not None else None,
            period_days=period_days,
            trend=trend,
        )
    
    except Exception as e:
        logger.error("Failed to calculate skill growth", skill_id=skill_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
