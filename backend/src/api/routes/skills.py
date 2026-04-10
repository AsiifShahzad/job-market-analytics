"""
Skills endpoints: skill demand analysis, trends, and co-occurrence patterns
GET /api/skills - top skills with filters
GET /api/skills/{skill_name}/trend - weekly trend time series
GET /api/skills/cooccurrence - top co-occurring skill pairs
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_
from datetime import datetime, timedelta
from typing import Optional, List
import structlog

from src.db.models import Job, Skill, JobSkill, SkillSnapshot
from src.db.session import get_db
from src.api.schemas import SkillSummary, SkillTrendResponse, SkillTrendPoint, CooccurrencePair, SkillsListResponse
from src.api.cache import cache_response

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/skills", tags=["skills"])


@router.get("", response_model=SkillsListResponse)
async def get_skills(
    db: AsyncSession = Depends(get_db),
    city: Optional[str] = Query(None, description="Filter by city"),
    country: Optional[str] = Query(None, description="Filter by country"),
    seniority: Optional[str] = Query(None, description="Filter by seniority level"),
    category: Optional[str] = Query(None, description="Filter by skill category"),
    limit: int = Query(50, ge=1, le=500, description="Number of skills to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
) -> SkillsListResponse:
    """
    Get top skills by job frequency with optional filters.
    Returns skills ranked by how many jobs require them.
    """
    logger.info(
        "Getting skills",
        city=city,
        country=country,
        seniority=seniority,
        category=category,
        limit=limit,
        offset=offset
    )

    try:
        # Build job filter based on location and seniority
        job_filters = []
        if city:
            job_filters.append(Job.city == city)
        if country:
            job_filters.append(Job.country == country)
        if seniority:
            job_filters.append(Job.seniority == seniority)

        # Build skill query
        skill_filters = []
        if category:
            skill_filters.append(Skill.category == category)

        # Main query: count jobs per skill
        query = (
            select(
                Skill.name,
                Skill.category,
                func.count(JobSkill.job_id).label("job_count"),
                func.avg(Job.salary_mid).label("avg_salary_mid")
            )
            .select_from(Skill)
            .join(JobSkill, Skill.id == JobSkill.skill_id)
            .join(Job, JobSkill.job_id == Job.id)
        )

        # Apply filters
        if job_filters:
            query = query.where(and_(*job_filters))
        if skill_filters:
            query = query.where(and_(*skill_filters))

        # Group and sort
        query = (
            query
            .group_by(Skill.id, Skill.name, Skill.category)
            .order_by(func.count(JobSkill.job_id).desc())
            .limit(limit)
            .offset(offset)
        )

        result = await db.execute(query)
        rows = result.fetchall()

        # Count total without limit
        total_query = (
            select(func.count(Skill.id.distinct()))
            .select_from(Skill)
            .join(JobSkill, Skill.id == JobSkill.skill_id)
            .join(Job, JobSkill.job_id == Job.id)
        )
        if job_filters:
            total_query = total_query.where(and_(*job_filters))
        if skill_filters:
            total_query = total_query.where(and_(*skill_filters))

        total_result = await db.execute(total_query)
        total_count = total_result.scalar() or 0

        skills = [
            SkillSummary(
                name=row.name,
                category=row.category,
                job_count=row.job_count,
                avg_salary_mid=row.avg_salary_mid,
                city=city,
                country=country
            )
            for row in rows
        ]

        logger.info("Retrieved skills", count=len(skills), total=total_count)

        return SkillsListResponse(
            skills=skills,
            total_count=total_count,
            limit=limit,
            offset=offset,
            cache_status="MISS"
        )

    except Exception as e:
        logger.error("Error fetching skills", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch skills")


@router.get("/{skill_name}/trend", response_model=SkillTrendResponse)
async def get_skill_trend(
    skill_name: str,
    db: AsyncSession = Depends(get_db),
    city: Optional[str] = Query(None, description="Filter by city"),
    country: Optional[str] = Query(None, description="Filter by country"),
    weeks: int = Query(12, ge=1, le=52, description="Number of weeks of history"),
) -> SkillTrendResponse:
    """
    Get weekly trend time series for a specific skill.
    Returns job count and average salary over time.
    """
    logger.info("Getting skill trend", skill_name=skill_name, city=city, country=country, weeks=weeks)

    try:
        # Get skill
        skill_query = select(Skill).where(Skill.name == skill_name)
        skill_result = await db.execute(skill_query)
        skill = skill_result.scalar_one_or_none()

        if not skill:
            raise HTTPException(status_code=404, detail=f"Skill '{skill_name}' not found")

        # Get snapshots
        cutoff_date = datetime.utcnow() - timedelta(weeks=weeks)
        snapshot_query = (
            select(SkillSnapshot)
            .where(
                and_(
                    SkillSnapshot.skill_id == skill.id,
                    SkillSnapshot.snapshot_date >= cutoff_date
                )
            )
        )

        if city:
            snapshot_query = snapshot_query.where(SkillSnapshot.city == city)
        if country:
            snapshot_query = snapshot_query.where(SkillSnapshot.country == country)

        snapshot_query = snapshot_query.order_by(SkillSnapshot.snapshot_date)
        snapshot_result = await db.execute(snapshot_query)
        snapshots = snapshot_result.scalars().all()

        # Compute week-over-week growth
        timepoints = []
        for i, snapshot in enumerate(snapshots):
            wow_growth = None
            if i > 0:
                prev_count = snapshots[i - 1].job_count
                if prev_count > 0:
                    wow_growth = ((snapshot.job_count - prev_count) / prev_count) * 100

            timepoints.append(
                SkillTrendPoint(
                    date=snapshot.snapshot_date,
                    job_count=snapshot.job_count,
                    avg_salary_mid=snapshot.avg_salary_mid,
                    week_over_week_growth=wow_growth
                )
            )

        logger.info("Retrieved skill trend", skill_name=skill_name, timepoints_count=len(timepoints))

        return SkillTrendResponse(
            skill_name=skill.name,
            category=skill.category,
            city=city,
            country=country,
            timepoints=timepoints,
            total_data_points=len(timepoints)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching skill trend", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch skill trend")


@router.get("/cooccurrence", response_model=List[CooccurrencePair])
async def get_skill_cooccurrence(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(20, ge=1, le=100, description="Top N co-occurrence pairs"),
    min_confidence: float = Query(0.1, ge=0.0, le=1.0, description="Minimum confidence threshold"),
) -> List[CooccurrencePair]:
    """
    Get top co-occurring skill pairs (skills that appear together in job postings).
    Confidence = (co_occurrence_count / jobs_with_skill_1)
    """
    logger.info("Getting skill cooccurrence", limit=limit, min_confidence=min_confidence)

    try:
        # Query: find skill pairs that appear in same jobs
        query = (
            select(
                Skill.name.label("skill_1"),
                func.count(JobSkill.job_id).label("co_occurrence_count")
            )
            .select_from(JobSkill)
            .join(Skill, JobSkill.skill_id == Skill.id)
            .group_by(Skill.id, Skill.name)
        )

        result = await db.execute(query)
        rows = result.fetchall()

        # For each skill, find co-occurring skills
        pairs = []
        for row in rows[:limit]:
            skill_1_name = row.skill_1
            
            # Get jobs with this skill
            jobs_with_skill_1_query = (
                select(func.count(JobSkill.job_id.distinct()))
                .where(JobSkill.skill_id == (
                    select(Skill.id).where(Skill.name == skill_1_name).scalar_subquery()
                ))
            )
            jobs_count_result = await db.execute(jobs_with_skill_1_query)
            jobs_with_skill_1 = jobs_count_result.scalar() or 1

            # Find co-occurring skills
            cooccur_query = (
                select(
                    Skill.name.label("skill_2"),
                    func.count(JobSkill.job_id).label("co_occurrence_count")
                )
                .select_from(JobSkill)
                .join(Skill, JobSkill.skill_id == Skill.id)
                .where(
                    JobSkill.job_id.in_(
                        select(js.job_id)
                        .select_from(JobSkill.as_("js"))
                        .join(Skill, Skill.id == JobSkill.skill_id)
                        .where(Skill.name == skill_1_name)
                    )
                )
                .where(Skill.name != skill_1_name)
                .group_by(Skill.id, Skill.name)
                .order_by(func.count(JobSkill.job_id).desc())
                .limit(5)
            )

            cooccur_result = await db.execute(cooccur_query)
            cooccur_rows = cooccur_result.fetchall()

            for cooccur_row in cooccur_rows:
                confidence = cooccur_row.co_occurrence_count / jobs_with_skill_1
                if confidence >= min_confidence:
                    pairs.append(
                        CooccurrencePair(
                            skill_1=skill_1_name,
                            skill_2=cooccur_row.skill_2,
                            co_occurrence_count=cooccur_row.co_occurrence_count,
                            confidence=confidence
                        )
                    )

        pairs.sort(key=lambda x: x.confidence, reverse=True)
        pairs = pairs[:limit]

        logger.info("Retrieved cooccurrence pairs", count=len(pairs))
        return pairs

    except Exception as e:
        logger.error("Error fetching cooccurrence", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch skill cooccurrence")
