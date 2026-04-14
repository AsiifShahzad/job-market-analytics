"""
Snapshot Scheduler — Automated weekly snapshot building.

Runs skill_snapshot aggregation on a schedule using APScheduler.

Usage:
    from src.etl.scheduler import start_snapshot_scheduler
    
    # In FastAPI startup:
    @app.on_event("startup")
    async def startup():
        start_snapshot_scheduler()

Configuration:
    - Default: Weekly on Sunday at 2:00 AM UTC
    - Configurable via environment variables
"""

import logging
import os
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from src.db.models import Base
from src.etl.snapshots import build_skill_snapshots, cleanup_old_snapshots

logger = logging.getLogger(__name__)

# Global scheduler instance
_scheduler: AsyncIOScheduler = None


# ══════════════════════════════════════════════════════════════════════════════
# Scheduled Task
# ══════════════════════════════════════════════════════════════════════════════

async def scheduled_snapshot_build() -> None:
    """
    Scheduled task to build skill snapshots.
    
    Runs independently with its own database session.
    """
    logger.info("╔═══ SCHEDULED SNAPSHOT BUILD STARTED ═══╗")
    
    try:
        # Create database session for this task
        from src.db.session import async_engine, async_session_maker
        
        async with async_session_maker() as db:
            # Build snapshots for today
            report = await build_skill_snapshots(
                db,
                snapshot_date=None,  # Today
                include_global=True
            )
            
            # Cleanup snapshots older than 1 year
            retention_days = int(os.getenv("SNAPSHOT_RETENTION_DAYS", "365"))
            if retention_days > 0:
                deleted = await cleanup_old_snapshots(db, retention_days=retention_days)
                logger.info(
                    "Snapshot cleanup complete",
                    deleted_count=deleted,
                    retention_days=retention_days,
                )
            
            logger.info(
                "╔═══ SCHEDULED SNAPSHOT BUILD COMPLETE ═══╗",
                snapshots_created=report.total_snapshots_created,
                snapshots_updated=report.total_snapshots_updated,
                skills_processed=report.skills_processed,
                duration_seconds=round(report.duration_seconds, 2),
                errors=report.errors,
            )
            
    except Exception as e:
        logger.error(
            "╔═══ SCHEDULED SNAPSHOT BUILD FAILED ═══╗",
            error=str(e),
        )


# ══════════════════════════════════════════════════════════════════════════════
# Scheduler Control
# ══════════════════════════════════════════════════════════════════════════════

def start_snapshot_scheduler() -> None:
    """
    Starts the APScheduler instance for snapshot automation.
    
    Configuration:
    - SNAPSHOT_SCHEDULE: Cron expression (default: "0 2 * * 0" = Sunday 2 AM UTC)
    - SNAPSHOT_ENABLED: Set to "false" to disable (default: "true")
    """
    global _scheduler
    
    # Check if scheduler is enabled
    enabled = os.getenv("SNAPSHOT_ENABLED", "true").lower() == "true"
    if not enabled:
        logger.info("Snapshot scheduler is disabled")
        return
    
    if _scheduler is not None and _scheduler.running:
        logger.warning("Scheduler already running")
        return
    
    try:
        # Get schedule from env (cron format)
        schedule = os.getenv("SNAPSHOT_SCHEDULE", "0 2 * * 0")  # Sunday 2 AM UTC
        
        logger.info(
            "Starting snapshot scheduler",
            schedule=schedule,
            timezone="UTC",
        )
        
        # Create scheduler
        _scheduler = AsyncIOScheduler(timezone="UTC")
        
        # Add job
        _scheduler.add_job(
            scheduled_snapshot_build,
            trigger=CronTrigger.from_crontab(schedule, timezone="UTC"),
            id="snapshot_builder",
            name="Skill Snapshot Builder",
            misfire_grace_time=600,  # 10 min grace period
            replace_existing=True,
        )
        
        # Start scheduler
        _scheduler.start()
        
        logger.info(
            "✓ Snapshot scheduler started successfully",
            schedule=schedule,
            next_run_time=_scheduler.get_job("snapshot_builder").next_run_time,
        )
        
    except Exception as e:
        logger.error(
            "Failed to start snapshot scheduler",
            error=str(e),
        )
        raise


def stop_snapshot_scheduler() -> None:
    """Stops the scheduler gracefully."""
    global _scheduler
    
    if _scheduler is None or not _scheduler.running:
        return
    
    try:
        logger.info("Stopping snapshot scheduler")
        _scheduler.shutdown(wait=True)
        _scheduler = None
        logger.info("✓ Snapshot scheduler stopped")
    
    except Exception as e:
        logger.error("Failed to stop snapshot scheduler", error=str(e))


def get_next_snapshot_run() -> datetime:
    """Returns the next scheduled snapshot run time."""
    global _scheduler
    
    if _scheduler is None or not _scheduler.running:
        return None
    
    job = _scheduler.get_job("snapshot_builder")
    return job.next_run_time if job else None


# ══════════════════════════════════════════════════════════════════════════════
# Manual Trigger (for testing)
# ══════════════════════════════════════════════════════════════════════════════

async def run_snapshot_now() -> dict:
    """
    Manually trigger a snapshot build immediately.
    
    Useful for testing or manual runs outside the schedule.
    
    Returns:
        Execution report
    """
    from src.db.session import async_session_maker
    
    logger.info("Manual snapshot build triggered")
    
    async with async_session_maker() as db:
        report = await build_skill_snapshots(db)
        
        return {
            "status": "success",
            "snapshots_created": report.total_snapshots_created,
            "snapshots_updated": report.total_snapshots_updated,
            "skills_processed": report.skills_processed,
            "duration_seconds": report.duration_seconds,
            "errors": report.errors,
        }
