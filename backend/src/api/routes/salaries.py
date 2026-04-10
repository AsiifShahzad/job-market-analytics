"""
Salary endpoints: salary band analysis and skill premium calculations
GET /api/salaries - salary percentiles (p25/p50/p75)
GET /api/salaries/skill-premium - per-skill salary uplift vs baseline
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from typing import Optional, List
import structlog

from src.db.models import Job, Skill, JobSkill
from src.db.session import get_db
from src.api.schemas import SalaryResponse, SalaryBand, SalaryPremiumResponse, SkillPremium

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/salaries", tags=["salaries"])


def _percentile_expr(column, percentile):
    """Generate PERCENTILE_CONT expression for PostgreSQL"""
    from sqlalchemy import literal_column
    return literal_column(f"PERCENTILE_CONT({percentile}) WITHIN GROUP (ORDER BY {column.name})")


@router.get("", response_model=SalaryResponse)
async def get_salaries(
    db: AsyncSession = Depends(get_db),
    title: Optional[str] = Query(None, description="Filter by job title (partial match)"),
    city: Optional[str] = Query(None, description="Filter by city"),
    skill: Optional[str] = Query(None, description="Filter by skill requirement"),
    seniority: Optional[str] = Query(None, description="Filter by seniority level"),
) -> SalaryResponse:
    """
    Get salary percentile bands (p25, p50, p75) with optional filters.
    Uses median salary where available.
    """
    logger.info(
        "Getting salary bands",
        title=title,
        city=city,
        skill=skill,
        seniority=seniority
    )

    try:
        # Build filter conditions
        filters = []
        
        if title:
            filters.append(Job.title.ilike(f"%{title}%"))
        if city:
            filters.append(Job.city == city)
        if seniority:
            filters.append(Job.seniority == seniority)
        
        # Must have salary data
        filters.append(Job.salary_mid.isnot(None))

        # Base query
        query = select(Job.salary_mid).where(and_(*filters))

        # If skill filter, join with skills
        if skill:
            query = (
                query
                .select_from(Job)
                .join(JobSkill, Job.id == JobSkill.job_id)
                .join(Skill, JobSkill.skill_id == Skill.id)
                .where(Skill.name == skill)
            )

        result = await db.execute(query)
        salaries = [row[0] for row in result.fetchall()]

        if not salaries:
            logger.warning("No salary data found with filters")
            return SalaryResponse(
                salary_band=SalaryBand(p25=None, p50=None, p75=None, count=0),
                title_filter=title,
                city_filter=city,
                skill_filter=skill,
                seniority_filter=seniority,
                cache_status="MISS"
            )

        # Calculate percentiles
        salaries.sort()
        n = len(salaries)
        
        p25_idx = int(n * 0.25)
        p50_idx = int(n * 0.50)
        p75_idx = int(n * 0.75)

        salary_band = SalaryBand(
            p25=float(salaries[p25_idx]) if p25_idx < n else None,
            p50=float(salaries[p50_idx]) if p50_idx < n else None,
            p75=float(salaries[p75_idx]) if p75_idx < n else None,
            count=n
        )

        logger.info(
            "Retrieved salary bands",
            count=n,
            p50=salary_band.p50
        )

        return SalaryResponse(
            salary_band=salary_band,
            title_filter=title,
            city_filter=city,
            skill_filter=skill,
            seniority_filter=seniority,
            cache_status="MISS"
        )

    except Exception as e:
        logger.error("Error fetching salaries", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch salary data")


@router.get("/skill-premium", response_model=SalaryPremiumResponse)
async def get_skill_premium(
    db: AsyncSession = Depends(get_db),
    city: Optional[str] = Query(None, description="Filter by city"),
    title: Optional[str] = Query(None, description="Filter by job title"),
    top_n: int = Query(20, ge=1, le=100, description="Top N skills by premium"),
) -> SalaryPremiumResponse:
    """
    Calculate salary premium (uplift) for each skill.
    Premium = avg(salary WITH skill) - avg(salary WITHOUT skill)
    """
    logger.info("Getting skill premiums", city=city, title=title, top_n=top_n)

    try:
        # Build filters
        filters = [Job.salary_mid.isnot(None)]
        if city:
            filters.append(Job.city == city)
        if title:
            filters.append(Job.title.ilike(f"%{title}%"))

        # Get all skills with their base statistics
        skill_query = (
            select(
                Skill.id,
                Skill.name,
                func.count(JobSkill.job_id).label("with_skill_count"),
                func.avg(Job.salary_mid).label("with_skill_avg")
            )
            .select_from(Skill)
            .join(JobSkill, Skill.id == JobSkill.skill_id)
            .join(Job, JobSkill.job_id == Job.id)
            .where(and_(*filters))
            .group_by(Skill.id, Skill.name)
            .having(func.count(JobSkill.job_id) >= 10)  # Minimum 10 jobs
            .order_by(func.count(JobSkill.skill_id).desc())
            .limit(top_n * 2)  # Get extra to filter later
        )

        skill_result = await db.execute(skill_query)
        skills = skill_result.fetchall()

        # Get baseline salary (all jobs with salary data)
        baseline_query = select(func.avg(Job.salary_mid)).where(and_(*filters))
        baseline_result = await db.execute(baseline_query)
        baseline_salary = baseline_result.scalar() or 0

        premiums = []

        for skill in skills:
            skill_id, skill_name, with_skill_count, with_skill_avg = skill

            absolute_premium = with_skill_avg - baseline_salary if baseline_salary > 0 else 0
            percent_premium = (absolute_premium / baseline_salary * 100) if baseline_salary > 0 else 0

            premiums.append(
                SkillPremium(
                    skill_name=skill_name,
                    baseline_salary=baseline_salary,
                    with_skill_salary=with_skill_avg,
                    absolute_premium=absolute_premium,
                    percent_premium=percent_premium,
                    job_count_with_skill=with_skill_count
                )
            )

        # Sort by premium and limit
        premiums.sort(key=lambda x: x.percent_premium or 0, reverse=True)
        premiums = premiums[:top_n]

        logger.info("Retrieved skill premiums", count=len(premiums))

        return SalaryPremiumResponse(
            premiums=premiums,
            city_filter=city,
            title_filter=title,
            cache_status="MISS"
        )

    except Exception as e:
        logger.error("Error fetching skill premiums", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch skill premium data")
