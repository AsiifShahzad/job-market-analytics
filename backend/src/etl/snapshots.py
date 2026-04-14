"""
Skill Snapshot Builder — Data aggregation for trend analysis.

Builds weekly/daily snapshots of skill demand by location.

Features:
- Aggregates job counts per skill
- Computes average salary by skill
- Groups by: skill_id, city, country
- Avoids duplicate snapshots (upsert behavior)
- Handles NULL locations (global snapshots)
- Scheduled for weekly runs
- Full audit logging

Author: Data Engineer
Standards: PEP 8, async-first, production-ready
"""

import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Optional, List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, text
from sqlalchemy.dialects.postgresql import insert

from src.db.models import Job, Skill, JobSkill, SkillSnapshot

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Data Structures
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class SnapshotMetrics:
    """Aggregated snapshot for a single skill+location."""
    skill_id: int
    snapshot_date: datetime
    job_count: int
    avg_salary_mid: Optional[float]
    city: Optional[str]
    country: Optional[str]


@dataclass
class SnapshotRunReport:
    """Execution report for snapshot building."""
    snapshot_date: datetime
    total_snapshots_created: int
    total_snapshots_updated: int
    skills_processed: int
    locations_processed: int
    duration_seconds: float
    errors: int
    error_messages: list


# ══════════════════════════════════════════════════════════════════════════════
# Main Snapshot Builder
# ══════════════════════════════════════════════════════════════════════════════

async def build_skill_snapshots(
    db: AsyncSession,
    snapshot_date: Optional[datetime] = None,
    include_global: bool = True,
) -> SnapshotRunReport:
    """
    Builds skill demand snapshots grouped by skill, city, and country.
    
    Process:
    1. Compute aggregates from job_skill + job tables
    2. Group by skill_id, city, country
    3. Check for existing snapshots (avoid duplicates)
    4. Upsert into skill_snapshot table
    5. Return execution metrics
    
    Args:
        db: AsyncSession for database operations
        snapshot_date: Date for snapshot (default: today UTC)
        include_global: Whether to include global snapshots (NULL city/country)
    
    Returns:
        SnapshotRunReport with execution metrics
    
    Notes:
        - Uses upsert pattern to handle existing snapshots gracefully
        - Skips skill+location combinations with 0 jobs
        - Handles NULL locations (global aggregates)
        - Logs detailed metrics
    """
    start_time = datetime.now(timezone.utc)
    snapshot_date = snapshot_date or datetime.now(timezone.utc).date()
    
    # Convert date to datetime at midnight UTC
    if not isinstance(snapshot_date, datetime):
        snapshot_date = datetime.combine(snapshot_date, datetime.min.time()).replace(
            tzinfo=timezone.utc
        )
    else:
        snapshot_date = snapshot_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    logger.info(
        "╔══ SNAPSHOT BUILDER STARTED ══╗",
        snapshot_date=snapshot_date.isoformat(),
    )
    
    report = SnapshotRunReport(
        snapshot_date=snapshot_date,
        total_snapshots_created=0,
        total_snapshots_updated=0,
        skills_processed=0,
        locations_processed=0,
        duration_seconds=0.0,
        errors=0,
        error_messages=[],
    )
    
    try:
        # ────────────────────────────────────────────────────────────────────
        # STEP 1: Build query for skill aggregates with location grouping
        # ────────────────────────────────────────────────────────────────────
        
        logger.info("STEP 1/4 — Building aggregation query")
        
        query = (
            select(
                Skill.id.label("skill_id"),
                Job.city.label("city"),
                Job.country.label("country"),
                func.count(distinct(JobSkill.job_id)).label("job_count"),
                func.avg(Job.salary_mid).label("avg_salary_mid"),
            )
            .select_from(JobSkill)
            .join(Skill, JobSkill.skill_id == Skill.id)
            .join(Job, JobSkill.job_id == Job.id)
            .group_by(Skill.id, Job.city, Job.country)
        )
        
        logger.info("STEP 1/4 — Executing aggregation query")
        rows = (await db.execute(query)).fetchall()
        logger.info("STEP 1/4 — Complete", total_rows=len(rows))
        
        if not rows:
            logger.warning("No job data found for snapshot")
            report.duration_seconds = (
                datetime.now(timezone.utc) - start_time
            ).total_seconds()
            return report
        
        # ────────────────────────────────────────────────────────────────────
        # STEP 2: Filter and prepare records for insertion
        # ────────────────────────────────────────────────────────────────────
        
        logger.info("STEP 2/4 — Preparing records for upsert")
        
        records_to_insert = []
        skills_set = set()
        locations_set = set()
        
        for row in rows:
            # Skip if job_count is 0
            if row.job_count == 0:
                continue
            
            # Skip NULL locations unless include_global is True
            city = row.city
            country = row.country
            if not include_global and city is None and country is None:
                continue
            
            skills_set.add(row.skill_id)
            locations_set.add((city, country))
            
            records_to_insert.append({
                "skill_id": row.skill_id,
                "snapshot_date": snapshot_date,
                "job_count": int(row.job_count),
                "avg_salary_mid": float(row.avg_salary_mid) if row.avg_salary_mid else None,
                "city": city,
                "country": country,
            })
        
        logger.info(
            "STEP 2/4 — Records prepared",
            total_records=len(records_to_insert),
            unique_skills=len(skills_set),
            unique_locations=len(locations_set),
        )
        
        if not records_to_insert:
            logger.warning("No records to insert after filtering")
            report.duration_seconds = (
                datetime.now(timezone.utc) - start_time
            ).total_seconds()
            return report
        
        # ────────────────────────────────────────────────────────────────────
        # STEP 3: Check for existing snapshots
        # ────────────────────────────────────────────────────────────────────
        
        logger.info("STEP 3/4 — Checking for existing snapshots")
        
        skill_ids = list(skills_set)
        existing_count = (
            await db.execute(
                select(func.count(SkillSnapshot.id)).where(
                    and_(
                        SkillSnapshot.skill_id.in_(skill_ids),
                        SkillSnapshot.snapshot_date == snapshot_date,
                    )
                )
            )
        ).scalar() or 0
        
        logger.info(
            "STEP 3/4 — Complete",
            existing_snapshots=existing_count,
            new_snapshots=len(records_to_insert) - existing_count,
        )
        
        # ────────────────────────────────────────────────────────────────────
        # STEP 4: Upsert into database
        # ────────────────────────────────────────────────────────────────────
        
        logger.info("STEP 4/4 — Upserting records")
        
        inserted_count = 0
        updated_count = 0
        
        for record in records_to_insert:
            try:
                # Check if snapshot exists
                exists = (
                    await db.execute(
                        select(SkillSnapshot.id).where(
                            and_(
                                SkillSnapshot.skill_id == record["skill_id"],
                                SkillSnapshot.snapshot_date == record["snapshot_date"],
                                SkillSnapshot.city == record["city"],
                                SkillSnapshot.country == record["country"],
                            )
                        )
                    )
                ).scalar_one_or_none()
                
                if exists:
                    # Update existing
                    await db.execute(
                        text("""
                            UPDATE skill_snapshot
                            SET job_count = :job_count,
                                avg_salary_mid = :avg_salary_mid
                            WHERE skill_id = :skill_id
                              AND snapshot_date = :snapshot_date
                              AND city IS NOT DISTINCT FROM :city
                              AND country IS NOT DISTINCT FROM :country
                        """),
                        {
                            "skill_id": record["skill_id"],
                            "snapshot_date": record["snapshot_date"],
                            "job_count": record["job_count"],
                            "avg_salary_mid": record["avg_salary_mid"],
                            "city": record["city"],
                            "country": record["country"],
                        }
                    )
                    updated_count += 1
                else:
                    # Insert new
                    db.add(SkillSnapshot(**record))
                    inserted_count += 1
                    
            except Exception as e:
                logger.error(
                    "Failed to upsert snapshot",
                    skill_id=record["skill_id"],
                    city=record["city"],
                    country=record["country"],
                    error=str(e),
                )
                report.errors += 1
                report.error_messages.append(str(e))
                continue
        
        # Flush all inserts
        await db.flush()
        
        logger.info(
            "STEP 4/4 — Upsert complete",
            inserted=inserted_count,
            updated=updated_count,
            errors=report.errors,
        )
        
        # ────────────────────────────────────────────────────────────────────
        # STEP 5: Commit and generate report
        # ────────────────────────────────────────────────────────────────────
        
        await db.commit()
        
        report.total_snapshots_created = inserted_count
        report.total_snapshots_updated = updated_count
        report.skills_processed = len(skills_set)
        report.locations_processed = len(locations_set)
        report.duration_seconds = (
            datetime.now(timezone.utc) - start_time
        ).total_seconds()
        
        logger.info(
            "╔══ SNAPSHOT BUILDER COMPLETE ══╗",
            snapshot_date=snapshot_date.isoformat(),
            snapshots_created=inserted_count,
            snapshots_updated=updated_count,
            skills_processed=len(skills_set),
            locations_processed=len(locations_set),
            duration_seconds=round(report.duration_seconds, 2),
            total_errors=report.errors,
        )
        
        return report
        
    except Exception as e:
        logger.error(
            "╔══ SNAPSHOT BUILDER FAILED ══╗",
            error=str(e),
            snapshot_date=snapshot_date.isoformat(),
        )
        report.errors += 1
        report.error_messages.append(str(e))
        await db.rollback()
        raise


# ══════════════════════════════════════════════════════════════════════════════
# Utility Functions
# ══════════════════════════════════════════════════════════════════════════════

async def cleanup_old_snapshots(
    db: AsyncSession,
    retention_days: int = 365,
) -> int:
    """
    Removes snapshots older than retention period.
    
    Args:
        db: AsyncSession
        retention_days: Keep snapshots from last N days (default: 1 year)
    
    Returns:
        Number of snapshots deleted
    """
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
    
    logger.info(
        "Cleaning up old snapshots",
        retention_days=retention_days,
        cutoff_date=cutoff_date.isoformat(),
    )
    
    result = await db.execute(
        "DELETE FROM skill_snapshot WHERE snapshot_date < :cutoff_date",
        {"cutoff_date": cutoff_date}
    )
    
    deleted_count = result.rowcount or 0
    await db.commit()
    
    logger.info("Snapshot cleanup complete", deleted=deleted_count)
    return deleted_count


async def get_latest_snapshot_date(db: AsyncSession) -> Optional[datetime]:
    """
    Returns the most recent snapshot date in database.
    
    Args:
        db: AsyncSession
    
    Returns:
        datetime of latest snapshot, or None if no snapshots exist
    """
    result = (
        await db.execute(
            select(func.max(SkillSnapshot.snapshot_date))
        )
    ).scalar()
    
    return result


async def get_skill_snapshots(
    db: AsyncSession,
    skill_id: int,
    days: int = 30,
) -> List[dict]:
    """
    Retrieves recent snapshots for a specific skill.
    
    Args:
        db: AsyncSession
        skill_id: Skill ID to fetch
        days: How many days back to retrieve
    
    Returns:
        List of snapshot records
    """
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    
    rows = (
        await db.execute(
            select(SkillSnapshot).where(
                and_(
                    SkillSnapshot.skill_id == skill_id,
                    SkillSnapshot.snapshot_date >= cutoff_date,
                )
            ).order_by(SkillSnapshot.snapshot_date.desc())
        )
    ).scalars().all()
    
    return [
        {
            "snapshot_date": row.snapshot_date,
            "job_count": row.job_count,
            "avg_salary_mid": row.avg_salary_mid,
            "city": row.city,
            "country": row.country,
        }
        for row in rows
    ]


async def calculate_skill_growth(
    db: AsyncSession,
    skill_id: int,
    days: int = 7,
) -> Optional[float]:
    """
    Calculates week-over-week growth rate for a skill.
    
    Formula: (current - previous) / previous
    
    Args:
        db: AsyncSession
        skill_id: Skill ID
        days: Period for growth calculation (default: 7 days)
    
    Returns:
        Growth rate as decimal (e.g. 0.25 = 25% growth), or None if insufficient data
    """
    now = datetime.now(timezone.utc)
    period_start = now - timedelta(days=days)
    period_end = now - timedelta(days=days * 2)
    
    # Current period
    current = await db.execute(
        select(func.sum(SkillSnapshot.job_count)).where(
            and_(
                SkillSnapshot.skill_id == skill_id,
                SkillSnapshot.snapshot_date >= period_start,
            )
        )
    )
    current_count = current.scalar() or 0
    
    # Previous period
    previous = await db.execute(
        select(func.sum(SkillSnapshot.job_count)).where(
            and_(
                SkillSnapshot.skill_id == skill_id,
                SkillSnapshot.snapshot_date >= period_end,
                SkillSnapshot.snapshot_date < period_start,
            )
        )
    )
    previous_count = previous.scalar() or 0
    
    if previous_count == 0:
        return None
    
    growth = (current_count - previous_count) / previous_count
    return growth
