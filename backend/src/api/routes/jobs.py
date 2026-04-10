"""
Jobs endpoints - fetch real job data from database
"""

import structlog
from fastapi import APIRouter, Query, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc
from typing import Optional, List

from src.db.session import get_db
from src.db.models import Job, JobSkill, Skill

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/jobs", tags=["jobs"])


@router.get("/search", response_model=dict)
async def search_jobs(
    keyword: str = Query("", description="Search keyword in title/description"),
    location: Optional[str] = Query(None, description="Filter by city or country"),
    salary_min: Optional[float] = Query(None, description="Minimum salary filter"),
    salary_max: Optional[float] = Query(None, description="Maximum salary filter"),
    salary_currency: str = Query("USD", description="Salary currency"),
    skill: Optional[List[str]] = Query(None, description="Filter by skills"),
    sort_by: str = Query("relevance", description="Sort by: relevance, salary, date"),
    sort_order: str = Query("desc", description="Sort order: asc or desc"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Results per page"),
    db: AsyncSession = Depends(get_db),
):
    """
    Search jobs from database with optional filters.
    Returns paginated results with skill associations.
    """
    try:
        # Build query
        query = select(Job)
        
        # Apply keyword search
        if keyword:
            search_term = f"%{keyword}%"
            from sqlalchemy import or_
            query = query.where(
                or_(
                    Job.title.ilike(search_term),
                    Job.description.ilike(search_term),
                    Job.company.ilike(search_term)
                )
            )
        
        # Apply location filter (search in city or location_raw)
        if location:
            from sqlalchemy import or_
            location_term = f"%{location}%"
            query = query.where(
                or_(
                    Job.city.ilike(location_term),
                    Job.location_raw.ilike(location_term),
                    Job.country.ilike(location_term)
                )
            )
        
        # Apply salary filters
        if salary_min is not None:
            query = query.where(Job.salary_max >= salary_min)
        if salary_max is not None:
            query = query.where(Job.salary_min <= salary_max)
        
        # Apply skill filter if provided
        if skill:
            skill_names = [s.lower() for s in skill]
            # Join with job_skill and skill tables
            query = query.join(JobSkill).join(Skill).where(
                Skill.name.in_(skill_names)
            ).distinct()
        
        # Count total before limiting
        count_query = select(func.count()).select_from(Job)
        
        # Apply same filters to count query
        if keyword:
            search_term = f"%{keyword}%"
            from sqlalchemy import or_
            count_query = count_query.where(
                or_(
                    Job.title.ilike(search_term),
                    Job.description.ilike(search_term),
                    Job.company.ilike(search_term)
                )
            )
        if location:
            from sqlalchemy import or_
            location_term = f"%{location}%"
            count_query = count_query.where(
                or_(
                    Job.city.ilike(location_term),
                    Job.location_raw.ilike(location_term),
                    Job.country.ilike(location_term)
                )
            )
        if salary_min is not None:
            count_query = count_query.where(Job.salary_max >= salary_min)
        if salary_max is not None:
            count_query = count_query.where(Job.salary_min <= salary_max)
        
        total_result = await db.execute(count_query)
        total = total_result.scalar_one()
        
        # Apply sorting
        if sort_by == "salary":
            query = query.order_by(
                desc(Job.salary_max) if sort_order == "desc" else Job.salary_max
            )
        elif sort_by == "date":
            query = query.order_by(
                desc(Job.created_at) if sort_order == "desc" else Job.created_at
            )
        else:  # relevance (default)
            query = query.order_by(desc(Job.id))
        
        # Apply pagination
        offset = (page - 1) * limit
        query = query.offset(offset).limit(limit)
        
        # Execute query
        result = await db.execute(query)
        jobs = result.scalars().all()
        
        # Format response
        jobs_data = []
        for job in jobs:
            desc_short = job.description[:200] + "..." if job.description and len(job.description) > 200 else job.description
            location_display = f"{job.city}, {job.country}" if job.city else job.location_raw
            job_dict = {
                "id": job.id,
                "title": job.title,
                "company": job.company,
                "location": location_display,
                "description": desc_short,
                "salary_min": job.salary_min,
                "salary_max": job.salary_max,
                "salary_mid": job.salary_mid,
                "salary_currency": salary_currency,
                "created": job.created_at.isoformat() if job.created_at else None,
            }
            jobs_data.append(job_dict)
        
        logger.info(
            "Job search executed",
            keyword=keyword,
            total_results=total,
            returned=len(jobs_data),
            page=page
        )
        
        return {
            "data": jobs_data,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total,
                "pages": (total + limit - 1) // limit,
            }
        }
    
    except Exception as e:
        logger.error("Job search failed", error=str(e))
        raise


@router.get("/{job_id}", response_model=dict)
async def get_job_detail(
    job_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed information about a specific job including skills.
    """
    try:
        # Fetch job
        result = await db.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one_or_none()
        
        if not job:
            return {"error": "Job not found"}, 404
        
        # Fetch associated skills
        skills_result = await db.execute(
            select(Skill).join(JobSkill).where(JobSkill.job_id == job_id)
        )
        skills = skills_result.scalars().all()
        
        location_display = f"{job.city}, {job.country}" if job.city else job.location_raw
        
        return {
            "data": {
                "id": job.id,
                "title": job.title,
                "company": job.company,
                "location": location_display,
                "description": job.description,
                "salary_min": job.salary_min,
                "salary_max": job.salary_max,
                "salary_mid": job.salary_mid,
                "created": job.created_at.isoformat() if job.created_at else None,
                "skills": [{"name": s.name, "category": s.category} for s in skills],
            }
        }
    
    except Exception as e:
        logger.error("Failed to fetch job detail", job_id=job_id, error=str(e))
        raise
