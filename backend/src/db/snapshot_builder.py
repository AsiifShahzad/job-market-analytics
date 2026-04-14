"""
Skill Snapshot Builder — Weekly aggregation module.

Populates skill_snapshot table with:
- Job count per skill (city, country)
- Average salary per skill (city, country)
- Date of snapshot

Features:
- City/country grouping
- Duplicate detection (uq_skill_snapshot)
- Batch insertion
- Error handling

Author: Data Engineer
Standards: PEP 8, production-ready
"""

import logging
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from sqlalchemy.dialects.postgresql import insert

from src.db.models import Skill, JobSkill, Job, SkillSnapshot

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Snapshot Builder
# ══════════════════════════════════════════════════════════════════════════════

async def build_skill_snapshots(db: AsyncSession, snapshot_date: datetime = None) -> dict:
    """
    Aggregates skill demand across city/country for the given date.
    
    Process:
    1. For each unique (skill, city, country) combination
    2. Count distinct jobs mentioning the skill
    3. Calculate average salary_mid
    4. Upsert into skill_snapshot (avoid duplicates)
    
    Args:
        db: AsyncSession for database operations
        snapshot_date: Date for snapshot (default: today UTC)
    
    Returns:
        dict with stats: {
            "snapshots_created": int,
            "skills_processed": int,
            "locations_covered": int,
            "duration_seconds": float
        }
    """
    start_time = datetime.now(timezone.utc)
    
    if snapshot_date is None:
        snapshot_date = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    
    logger.info(
        "🔄 Starting skill snapshot build",
        snapshot_date=snapshot_date.isoformat(),
    )
    
    try:
        # ────────────────────────────────────────────────────────────────────
        # STEP 1: Query skill demand by (skill_id, city, country)
        # ────────────────────────────────────────────────────────────────────
        
        query = (
            select(
                Skill.id.label("skill_id"),
                Job.city,
                Job.country,
                func.count(func.distinct(JobSkill.job_id)).label("job_count"),
                func.avg(Job.salary_mid).label("avg_salary"),
            )
            .select_from(JobSkill)
            .join(Skill, JobSkill.skill_id == Skill.id)
            .join(Job, JobSkill.job_id == Job.id)
            .where(Job.salary_mid.isnot(None))  # Only count jobs with salary
            .group_by(Skill.id, Job.city, Job.country)
        )
        
        rows = (await db.execute(query)).fetchall()
        logger.info(f"Found {len(rows)} unique (skill, city, country) combinations")
        
        # ────────────────────────────────────────────────────────────────────
        # STEP 2: Check for existing snapshots (avoid duplicates)
        # ────────────────────────────────────────────────────────────────────
        
        existing_snapshots = await _get_existing_snapshots(db, snapshot_date, rows)
        logger.info(f"Found {len(existing_snapshots)} existing snapshots for today")
        
        # ────────────────────────────────────────────────────────────────────
        # STEP 3: Build new snapshot records
        # ────────────────────────────────────────────────────────────────────
        
        snapshots_to_insert = []
        unique_skills = set()
        unique_locations = set()
        
        for row in rows:
            # Create composite key
            key = (row.skill_id, row.city, row.country)
            
            # Skip if already exists
            if key in existing_snapshots:
                continue
            
            snapshot = SkillSnapshot(
                skill_id=row.skill_id,
                snapshot_date=snapshot_date,
                job_count=int(row.job_count),
                avg_salary_mid=float(row.avg_salary) if row.avg_salary else None,
                city=row.city,
                country=row.country,
            )
            snapshots_to_insert.append(snapshot)
            unique_skills.add(row.skill_id)
            unique_locations.add((row.city, row.country))
        
        logger.info(
            f"Prepared {len(snapshots_to_insert)} new snapshots",
            unique_skills=len(unique_skills),
            unique_locations=len(unique_locations),
        )
        
        # ────────────────────────────────────────────────────────────────────
        # STEP 4: Batch insert
        # ────────────────────────────────────────────────────────────────────
        
        if snapshots_to_insert:
            db.add_all(snapshots_to_insert)
            await db.flush()
            logger.info(f"✓ Inserted {len(snapshots_to_insert)} snapshots")
        
        # ────────────────────────────────────────────────────────────────────
        # STEP 5: Commit and return stats
        # ────────────────────────────────────────────────────────────────────
        
        await db.commit()
        
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        
        stats = {
            "snapshots_created": len(snapshots_to_insert),
            "skills_processed": len(unique_skills),
            "locations_covered": len(unique_locations),
            "duration_seconds": round(duration, 2),
        }
        
        logger.info(
            "✓ Skill snapshot build complete",
            **stats,
        )
        
        return stats
        
    except Exception as e:
        logger.error(
            "❌ Skill snapshot build failed",
            error=str(e),
        )
        await db.rollback()
        raise


# ══════════════════════════════════════════════════════════════════════════════
# Helper Functions
# ══════════════════════════════════════════════════════════════════════════════

async def _get_existing_snapshots(
    db: AsyncSession,
    snapshot_date: datetime,
    candidate_rows: list,
) -> set:
    """
    Retrieves existing snapshots for today to avoid duplicates.
    
    Returns:
        set of tuples: {(skill_id, city, country), ...}
    """
    if not candidate_rows:
        return set()
    
    # Extract unique (skill_id, city, country) keys
    candidate_keys = {
        (row.skill_id, row.city, row.country)
        for row in candidate_rows
    }
    
    skill_ids = {key[0] for key in candidate_keys}
    cities = {key[1] for key in candidate_keys if key[1]}
    countries = {key[2] for key in candidate_keys if key[2]}
    
    # Query existing snapshots
    query = (
        select(
            SkillSnapshot.skill_id,
            SkillSnapshot.city,
            SkillSnapshot.country,
        )
        .where(
            and_(
                SkillSnapshot.snapshot_date == snapshot_date,
                SkillSnapshot.skill_id.in_(skill_ids),
            )
        )
    )
    
    rows = (await db.execute(query)).fetchall()
    
    # Build set of existing keys
    existing = {(row.skill_id, row.city, row.country) for row in rows}
    
    return existing


async def build_skill_snapshots_global(db: AsyncSession, snapshot_date: datetime = None) -> dict:
    """
    Alternative: Global snapshots (without city/country grouping).
    Builds one snapshot per skill across all jobs.
    
    Use case: Trend analysis at global level only
    """
    if snapshot_date is None:
        snapshot_date = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    
    logger.info("🔄 Building global skill snapshots (no city/country)")
    
    try:
        # Aggregate without location grouping
        query = (
            select(
                Skill.id.label("skill_id"),
                func.count(func.distinct(JobSkill.job_id)).label("job_count"),
                func.avg(Job.salary_mid).label("avg_salary"),
            )
            .select_from(JobSkill)
            .join(Skill, JobSkill.skill_id == Skill.id)
            .join(Job, JobSkill.job_id == Job.id)
            .where(Job.salary_mid.isnot(None))
            .group_by(Skill.id)
        )
        
        rows = (await db.execute(query)).fetchall()
        
        # Check for existing global snapshots
        existing_query = (
            select(SkillSnapshot.skill_id)
            .where(
                and_(
                    SkillSnapshot.snapshot_date == snapshot_date,
                    SkillSnapshot.city.is_(None),
                    SkillSnapshot.country.is_(None),
                )
            )
        )
        
        existing_ids = {
            row.skill_id
            for row in (await db.execute(existing_query)).fetchall()
        }
        
        # Insert new snapshots
        snapshots = []
        for row in rows:
            if row.skill_id not in existing_ids:
                snapshots.append(
                    SkillSnapshot(
                        skill_id=row.skill_id,
                        snapshot_date=snapshot_date,
                        job_count=int(row.job_count),
                        avg_salary_mid=float(row.avg_salary) if row.avg_salary else None,
                        city=None,
                        country=None,
                    )
                )
        
        if snapshots:
            db.add_all(snapshots)
            await db.commit()
            logger.info(f"✓ Inserted {len(snapshots)} global snapshots")
        
        return {
            "snapshots_created": len(snapshots),
            "skills_processed": len(rows),
        }
        
    except Exception as e:
        logger.error("❌ Global snapshot build failed", error=str(e))
        await db.rollback()
        raise
