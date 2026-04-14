"""
GET /api/trends/emerging
Week-over-week growth from SkillSnapshot table.
Falls back to ranking by job count if snapshot data is insufficient.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from datetime import datetime, timedelta, timezone
from typing import Optional
import structlog

from src.db.session import get_db
from src.db.models import Skill, JobSkill, SkillSnapshot, Job
from src.api.schemas import EmergingTrendsResponse, EmergingSkillItem

logger = structlog.get_logger(__name__)
router = APIRouter(tags=["trends"])


@router.get("/api/trends/emerging", response_model=EmergingTrendsResponse)
async def get_emerging_skills(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(20, ge=1, le=100),
    period: str = Query("week-over-week"),
    min_job_count: int = Query(5, ge=1),
) -> EmergingTrendsResponse:
    """
    Skills with fastest week-over-week growth.
    growth_rate is expressed as 0.0–1.0 (e.g. 0.45 = 45% growth).
    """
    try:
        now = datetime.now(timezone.utc)
        week_ago = now - timedelta(days=7)
        two_weeks_ago = now - timedelta(days=14)

        # Current week counts per skill
        current_q = (
            select(
                Skill.name,
                Skill.category,
                func.sum(SkillSnapshot.job_count).label("cur_count"),
            )
            .select_from(SkillSnapshot)
            .join(Skill, SkillSnapshot.skill_id == Skill.id)
            .where(SkillSnapshot.snapshot_date >= week_ago)
            .group_by(Skill.id, Skill.name, Skill.category)
            .having(func.sum(SkillSnapshot.job_count) >= min_job_count)
        )
        current_rows = (await db.execute(current_q)).fetchall()

        # Previous week counts per skill
        prev_q = (
            select(
                Skill.name,
                func.sum(SkillSnapshot.job_count).label("prev_count"),
            )
            .select_from(SkillSnapshot)
            .join(Skill, SkillSnapshot.skill_id == Skill.id)
            .where(
                and_(
                    SkillSnapshot.snapshot_date >= two_weeks_ago,
                    SkillSnapshot.snapshot_date < week_ago,
                )
            )
            .group_by(Skill.name)
        )
        prev_map = {r.name: r.prev_count for r in (await db.execute(prev_q)).fetchall()}

        skills: list[EmergingSkillItem] = []

        if current_rows:
            for row in current_rows:
                prev = prev_map.get(row.name, 0)
                if prev > 0:
                    growth = (row.cur_count - prev) / prev
                else:
                    growth = 1.0 if row.cur_count > 0 else 0.0

                skills.append(
                    EmergingSkillItem(
                        name=row.name,
                        growth_rate=round(growth, 4),
                        job_count=int(row.cur_count),
                        trending=growth > 0.05,
                    )
                )

            skills.sort(key=lambda x: x.growth_rate, reverse=True)
            skills = skills[:limit]

        else:
            # Fallback: no snapshots yet — calculate growth from recent vs older jobs
            logger.warning("No snapshot data — calculating growth from job data")
            
            now = datetime.now(timezone.utc)
            week_ago = now - timedelta(days=7)
            two_weeks_ago = now - timedelta(days=14)
            
            # Recent week job counts per skill
            recent_q = (
                select(
                    Skill.id,
                    Skill.name,
                    Skill.category,
                    func.count(JobSkill.job_id).label("recent_count"),
                )
                .select_from(Skill)
                .join(JobSkill, Skill.id == JobSkill.skill_id)
                .join(Job, JobSkill.job_id == Job.id)
                .where(Job.fetched_at >= week_ago)
                .group_by(Skill.id, Skill.name, Skill.category)
            )
            recent_rows = (await db.execute(recent_q)).fetchall()
            
            # Older week job counts per skill (for comparison)
            older_q = (
                select(
                    Skill.id,
                    func.count(JobSkill.job_id).label("older_count"),
                )
                .select_from(Skill)
                .join(JobSkill, Skill.id == JobSkill.skill_id)
                .join(Job, JobSkill.job_id == Job.id)
                .where(
                    and_(
                        Job.fetched_at >= two_weeks_ago,
                        Job.fetched_at < week_ago,
                    )
                )
                .group_by(Skill.id)
            )
            older_map = {r.id: r.older_count for r in (await db.execute(older_q)).fetchall()}
            
            # Calculate growth rates dynamically
            for row in recent_rows:
                recent_count = row.recent_count
                older_count = older_map.get(row.id, 0)
                
                if older_count > 0:
                    growth = (recent_count - older_count) / older_count
                else:
                    # New skill this week
                    growth = 1.0 if recent_count > 0 else 0.0
                
                if recent_count >= min_job_count:
                    skills.append(
                        EmergingSkillItem(
                            name=row.name,
                            growth_rate=round(growth, 4),
                            job_count=int(recent_count),
                            trending=growth > 0.05,
                        )
                    )
            
            # Sort by growth rate and limit
            skills.sort(key=lambda x: x.growth_rate, reverse=True)
            skills = skills[:limit]

        logger.info("emerging skills fetched", count=len(skills))
        return EmergingTrendsResponse(emerging_skills=skills, limit=limit, period=period)

    except Exception as e:
        logger.error("trends error", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch trends")
