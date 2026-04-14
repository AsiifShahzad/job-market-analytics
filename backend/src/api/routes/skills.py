"""
GET /api/skills — top skills ranked by job count with average salary.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc
from typing import Optional
import structlog

from src.db.session import get_db
from src.db.models import Skill, JobSkill, Job
from src.api.schemas import SkillsResponse, SkillItem

logger = structlog.get_logger(__name__)
router = APIRouter(tags=["skills"])


@router.get("/api/skills", response_model=SkillsResponse)
async def get_skills(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    category: Optional[str] = Query(None, description="Filter by category"),
) -> SkillsResponse:
    """Top skills ranked by how many jobs mention them."""
    try:
        q = (
            select(
                Skill.name,
                Skill.category,
                func.count(JobSkill.job_id).label("job_count"),
                func.avg(Job.salary_mid).label("avg_salary"),
            )
            .select_from(Skill)
            .join(JobSkill, Skill.id == JobSkill.skill_id)
            .join(Job, JobSkill.job_id == Job.id)
            .group_by(Skill.id, Skill.name, Skill.category)
            .order_by(desc("job_count"))
        )

        if category:
            q = q.where(Skill.category == category)

        # Total count (before pagination)
        count_q = (
            select(func.count())
            .select_from(
                select(Skill.id)
                .join(JobSkill, Skill.id == JobSkill.skill_id)
                .group_by(Skill.id)
                .subquery()
            )
        )
        total = (await db.execute(count_q)).scalar() or 0

        q = q.offset(offset).limit(limit)
        rows = (await db.execute(q)).fetchall()

        items = [
            SkillItem(
                name=row.name,
                category=row.category,
                frequency=row.job_count,
                job_count=row.job_count,
                average_salary=round(float(row.avg_salary), 2) if row.avg_salary else None,
            )
            for row in rows
        ]

        logger.info("skills fetched", count=len(items), total=total)
        return SkillsResponse(
            skills=items,
            total_count=total,
            limit=limit,
            offset=offset,
        )

    except Exception as e:
        logger.error("skills error", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch skills")
