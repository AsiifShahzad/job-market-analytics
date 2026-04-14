"""
GET /api/jobs/search
Paginated job search with filters.
Returns all fields the frontend job cards need.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, or_, and_
from typing import Optional, List
import structlog

from src.db.session import get_db
from src.db.models import Job, Skill, JobSkill
from src.api.schemas import JobsSearchResponse, JobItem, PaginationMeta

logger = structlog.get_logger(__name__)
router = APIRouter(tags=["jobs"])


@router.get("/api/jobs/search", response_model=JobsSearchResponse)
async def search_jobs(
    db: AsyncSession = Depends(get_db),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    search: Optional[str] = Query(None, description="Keyword in title or description"),
    city: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    seniority: Optional[str] = Query(None, description="junior | mid | senior | lead | unspecified"),
    min_salary: Optional[float] = Query(None),
    max_salary: Optional[float] = Query(None),
    remote: Optional[bool] = Query(None),
    skills: Optional[str] = Query(None, description="Comma separated list of skills"),
    skill_match_type: str = Query("any", description="any | all"),
    sort_by: str = Query("date", description="date | salary_min | salary_max | salary_mid"),
) -> JobsSearchResponse:
    """
    Search jobs with optional filters and pagination.
    Returns job cards with extracted skills list attached.
    """
    try:
        filters = []

        if search:
            term = f"%{search}%"
            filters.append(
                or_(Job.title.ilike(term), Job.description.ilike(term), Job.company.ilike(term))
            )
        if city:
            filters.append(Job.city.ilike(f"%{city}%"))
        if country:
            filters.append(Job.country.ilike(f"%{country}%"))
        if seniority:
            filters.append(Job.seniority == seniority)
        if min_salary is not None:
            filters.append(Job.salary_mid >= min_salary)
        if max_salary is not None:
            filters.append(Job.salary_mid <= max_salary)
        if remote is not None:
            filters.append(Job.remote == remote)
            
        if skills:
            parsed_skills = [s.strip().lower() for s in skills.split(",")]
            if parsed_skills:
                if skill_match_type == "all":
                    for skill_name in parsed_skills:
                        skill_subq = (
                            select(JobSkill.job_id)
                            .join(Skill, JobSkill.skill_id == Skill.id)
                            .where(func.lower(Skill.name) == skill_name)
                            .scalar_subquery()
                        )
                        filters.append(Job.id.in_(skill_subq))
                else:
                    skill_subq = (
                        select(JobSkill.job_id)
                        .join(Skill, JobSkill.skill_id == Skill.id)
                        .where(func.lower(Skill.name).in_(parsed_skills))
                        .scalar_subquery()
                    )
                    filters.append(Job.id.in_(skill_subq))

        # Count total matching rows
        count_q = select(func.count(Job.id))
        if filters:
            count_q = count_q.where(and_(*filters))
        total = (await db.execute(count_q)).scalar() or 0

        # Build main query
        query = select(Job)
        if filters:
            query = query.where(and_(*filters))

        # Sort
        sort_col = {
            "salary_min": Job.salary_min,
            "salary_max": Job.salary_max,
            "salary_mid": Job.salary_mid,
        }.get(sort_by, Job.posted_at)

        query = query.order_by(desc(sort_col).nullslast())

        # Paginate
        offset = (page - 1) * limit
        query = query.offset(offset).limit(limit)

        jobs = (await db.execute(query)).scalars().all()

        # Fetch skills for each job in one query
        job_ids = [j.id for j in jobs]
        skills_q = (
            select(JobSkill.job_id, Skill.name)
            .join(Skill, JobSkill.skill_id == Skill.id)
            .where(JobSkill.job_id.in_(job_ids))
        )
        skill_rows = (await db.execute(skills_q)).fetchall()

        # Build job_id → [skill_name] map
        skills_map: dict[str, list[str]] = {}
        for job_id, skill_name in skill_rows:
            skills_map.setdefault(job_id, []).append(skill_name)

        # Format location for display
        def fmt_location(job: Job) -> str:
            if job.city and job.country:
                return f"{job.city}, {job.country}"
            return job.location_raw or ""

        items = [
            JobItem(
                id=job.id,
                title=job.title,
                company=job.company,
                location=fmt_location(job),
                description=job.description,
                salary_min=job.salary_min,
                salary_max=job.salary_max,
                salary_mid=job.salary_mid,
                salary_currency="USD",
                created=job.posted_at,
                url=job.url,
                remote=job.remote,
                skills=skills_map.get(job.id, []),
            )
            for job in jobs
        ]

        pages = max(1, -(-total // limit))  # ceiling division

        logger.info("jobs search", total=total, page=page, returned=len(items))

        return JobsSearchResponse(
            data=items,
            pagination=PaginationMeta(page=page, limit=limit, total=total, pages=pages),
        )

    except Exception as e:
        logger.error("jobs search error", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to search jobs")
