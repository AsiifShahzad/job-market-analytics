"""
Trends endpoints: emerging skills and skill-city heatmaps
GET /api/trends/emerging - fastest growing skills by week-over-week growth
GET /api/trends/heatmap - skill-city job count matrix
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, desc
from datetime import datetime, timedelta
from typing import List, Optional
import structlog

from src.db.models import SkillSnapshot, Skill, Job, JobSkill
from src.db.session import get_db
from src.api.schemas import EmergingTrendsResponse, TrendPoint, HeatmapResponse, HeatmapCell

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/trends", tags=["trends"])


@router.get("/emerging", response_model=EmergingTrendsResponse)
async def get_emerging_skills(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(20, ge=1, le=100, description="Top N emerging skills"),
    min_job_count: int = Query(10, ge=1, description="Minimum job postings to qualify"),
) -> EmergingTrendsResponse:
    """
    Get fastest-growing skills by week-over-week job count growth.
    Requires minimum job postings to filter out noise.
    """
    logger.info("Getting emerging skills", limit=limit, min_job_count=min_job_count)

    try:
        # Get latest two weeks of data
        latest_date = datetime.utcnow()
        week_ago = latest_date - timedelta(days=7)

        # Get snapshots for current and previous week
        current_query = (
            select(
                Skill.id,
                Skill.name,
                Skill.category,
                func.sum(SkillSnapshot.job_count).label("total_jobs"),
                func.avg(SkillSnapshot.avg_salary_mid).label("avg_salary")
            )
            .select_from(SkillSnapshot)
            .join(Skill, SkillSnapshot.skill_id == Skill.id)
            .where(SkillSnapshot.snapshot_date >= week_ago)
            .group_by(Skill.id, Skill.name, Skill.category)
            .having(func.sum(SkillSnapshot.job_count) >= min_job_count)
        )

        current_result = await db.execute(current_query)
        current_weeks = {row[1]: (row[2], row[3], row[4]) for row in current_result.fetchall()}

        # Get previous week data
        two_weeks_ago = latest_date - timedelta(days=14)
        previous_query = (
            select(
                Skill.name,
                func.sum(SkillSnapshot.job_count).label("total_jobs")
            )
            .select_from(SkillSnapshot)
            .join(Skill, SkillSnapshot.skill_id == Skill.id)
            .where(
                and_(
                    SkillSnapshot.snapshot_date >= two_weeks_ago,
                    SkillSnapshot.snapshot_date < week_ago
                )
            )
            .group_by(Skill.name)
        )

        previous_result = await db.execute(previous_query)
        previous_weeks = {row[0]: row[1] for row in previous_result.fetchall()}

        # Calculate week-over-week growth
        trending_skills = []

        for skill_name, (category, current_count, avg_salary) in current_weeks.items():
            prev_count = previous_weeks.get(skill_name, 0)
            
            if prev_count == 0:
                # New skill this week
                wow_growth = 100.0 if current_count > 0 else 0
            else:
                wow_growth = ((current_count - prev_count) / prev_count) * 100

            if current_count >= min_job_count:
                trending_skills.append(
                    TrendPoint(
                        skill_name=skill_name,
                        category=category,
                        current_job_count=current_count,
                        week_over_week_growth=wow_growth,
                        week_over_week_growth_percent=f"{wow_growth:+.1f}%",
                        avg_salary_mid=avg_salary
                    )
                )

        # Sort by growth and limit
        trending_skills.sort(key=lambda x: x.week_over_week_growth, reverse=True)
        trending_skills = trending_skills[:limit]

        logger.info("Retrieved emerging skills", count=len(trending_skills))

        return EmergingTrendsResponse(
            emerging_skills=trending_skills,
            limit=limit,
            period="week-over-week",
            cache_status="MISS"
        )

    except Exception as e:
        logger.error("Error fetching emerging skills", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch emerging skills")


@router.get("/heatmap", response_model=HeatmapResponse)
async def get_skill_heatmap(
    db: AsyncSession = Depends(get_db),
    top_n_skills: int = Query(10, ge=1, le=50, description="Top N skills to include"),
    top_n_cities: int = Query(10, ge=1, le=50, description="Top N cities to include"),
) -> HeatmapResponse:
    """
    Get skill-city demand heatmap (job count matrix).
    Returns all combinations of top skills and top cities.
    """
    logger.info("Getting skill heatmap", top_n_skills=top_n_skills, top_n_cities=top_n_cities)

    try:
        # Get top skills by total job count
        top_skills_query = (
            select(Skill.name)
            .select_from(Skill)
            .join(JobSkill, Skill.id == JobSkill.skill_id)
            .join(Job, JobSkill.job_id == Job.id)
            .group_by(Skill.id, Skill.name)
            .order_by(func.count(JobSkill.job_id).desc())
            .limit(top_n_skills)
        )

        top_skills_result = await db.execute(top_skills_query)
        top_skills = [row[0] for row in top_skills_result.fetchall()]

        # Get top cities by total jobs
        top_cities_query = (
            select(Job.city)
            .where(Job.city.isnot(None))
            .group_by(Job.city)
            .order_by(func.count(Job.id).desc())
            .limit(top_n_cities)
        )

        top_cities_result = await db.execute(top_cities_query)
        top_cities = [row[0] for row in top_cities_result.fetchall() if row[0]]

        # Build heatmap
        heatmap_data = []

        for skill in top_skills:
            for city in top_cities:
                query = (
                    select(
                        func.count(JobSkill.job_id).label("job_count"),
                        func.avg(Job.salary_mid).label("avg_salary")
                    )
                    .select_from(JobSkill)
                    .join(Job, JobSkill.job_id == Job.id)
                    .join(Skill, JobSkill.skill_id == Skill.id)
                    .where(
                        and_(
                            Skill.name == skill,
                            Job.city == city
                        )
                    )
                )

                result = await db.execute(query)
                row = result.fetchone()

                if row and row[0] > 0:
                    heatmap_data.append(
                        HeatmapCell(
                            skill_name=skill,
                            city=city,
                            job_count=row[0],
                            avg_salary_mid=row[1]
                        )
                    )

        logger.info("Retrieved heatmap", cells=len(heatmap_data), skills=len(top_skills), cities=len(top_cities))

        return HeatmapResponse(
            heatmap_data=heatmap_data,
            top_n_skills=len(top_skills),
            top_n_cities=len(top_cities),
            cache_status="MISS"
        )

    except Exception as e:
        logger.error("Error fetching heatmap", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch heatmap data")
