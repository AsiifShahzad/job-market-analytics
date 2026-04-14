"""
FastAPI routes for insights endpoints.
Exposes computed insights via REST API for frontend consumption.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import structlog

from src.db.session import get_db
from src.insights.compute import (
    compute_all_insights,
    get_top_skills,
    get_fastest_growing_skills,
    get_average_salary_per_skill,
    get_salary_by_seniority,
    get_remote_salary_difference,
    get_top_hiring_locations,
    get_remote_job_percentage,
    get_jobs_posted_trend,
    get_seniority_distribution,
    get_top_skills_in_senior_roles,
    get_salary_by_search_keyword,
)

logger = structlog.get_logger(__name__)
router = APIRouter(tags=["insights"])

# ── Pydantic Models ──────────────────────────────────────────────────────────

class SkillInsight(BaseModel):
    skill: str
    demand: Optional[int] = None
    avg_salary: Optional[int] = None
    growth_rate: Optional[float] = None
    job_count: Optional[int] = None
    category: Optional[str] = None
    label: Optional[str] = None
    cooccurrence_count: Optional[int] = None


class SalaryInsight(BaseModel):
    avg_salary: int
    min_salary: Optional[int] = None
    max_salary: Optional[int] = None
    job_count: int


class LocationInsight(BaseModel):
    city: Optional[str]
    country: Optional[str]
    job_count: int


class TrendPoint(BaseModel):
    date: str
    jobs_posted: int


class InsightsSummaryResponse(BaseModel):
    summary: Dict[str, Any]
    top_skills: List[SkillInsight]
    trending_skills: List[SkillInsight]
    salary_insights: Dict[str, Any]
    market_insights: Dict[str, Any]
    skill_insights: Dict[str, Any]
    actionable_insights: List[str]


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/api/insights/summary", response_model=InsightsSummaryResponse)
async def get_insights_summary(
    db: AsyncSession = Depends(get_db),
) -> InsightsSummaryResponse:
    """
    Complete insights dashboard data.
    Returns all high-impact insights in a single call.
    """
    try:
        logger.info("Computing insights summary...")
        insights = await compute_all_insights(db)
        return InsightsSummaryResponse(**insights)
    except Exception as e:
        logger.error("insights summary error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to compute insights: {e}")


@router.get("/api/insights/skills", response_model=Dict[str, Any])
async def get_insights_skills(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(10, ge=1, le=50),
) -> Dict[str, Any]:
    """Skill-focused insights: demand, categories, trends."""
    try:
        top_skills = await get_top_skills(db, limit=limit)
        trending = await get_fastest_growing_skills(db, limit=5)
        
        return {
            "top_skills": top_skills,
            "trending_skills": trending,
            "insight": "Skills are ranked by demand. Most in-demand skills shown first.",
        }
    except Exception as e:
        logger.error("skills insights error", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch skill insights")


@router.get("/api/insights/salary", response_model=Dict[str, Any])
async def get_insights_salary(
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """Salary insights: by skill, seniority, remote status."""
    try:
        salary_per_skill = await get_average_salary_per_skill(db)
        salary_by_seniority = await get_salary_by_seniority(db)
        remote_diff = await get_remote_salary_difference(db)
        
        return {
            "top_paying_skills": salary_per_skill,
            "by_seniority": salary_by_seniority,
            "remote_comparison": remote_diff,
            "insight": "Salary data shows high-paying skills and roles. Remote work comparison included.",
        }
    except Exception as e:
        logger.error("salary insights error", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch salary insights")


@router.get("/api/insights/market", response_model=Dict[str, Any])
async def get_insights_market(
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """Market insights: locations, remote %, job trends, seniority distribution."""
    try:
        locations = await get_top_hiring_locations(db)
        remote_pct = await get_remote_job_percentage(db)
        jobs_trend = await get_jobs_posted_trend(db)
        seniority_dist = await get_seniority_distribution(db)
        
        return {
            "top_locations": locations,
            "remote_percentage": remote_pct,
            "jobs_trend": jobs_trend,
            "seniority_distribution": seniority_dist,
            "insight": "Market overview shows job distribution by location, remote status, and experience level.",
        }
    except Exception as e:
        logger.error("market insights error", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch market insights")


@router.get("/api/insights/keywords", response_model=Dict[str, Any])
async def get_insights_keywords(
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """Keyword intelligence: which search keywords bring high-value jobs."""
    try:
        keyword_salary = await get_salary_by_search_keyword(db)
        
        return {
            "keywords_by_salary": keyword_salary,
            "insight": "Keywords ranked by average salary of results. Higher-paying keywords shown first.",
        }
    except Exception as e:
        logger.error("keywords insights error", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch keyword insights")


@router.get("/api/insights/seniority", response_model=Dict[str, Any])
async def get_insights_seniority(
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """Seniority insights: distribution and skills valued in senior roles."""
    try:
        seniority_dist = await get_seniority_distribution(db)
        senior_skills = await get_top_skills_in_senior_roles(db)
        
        return {
            "distribution": seniority_dist,
            "top_skills_in_senior_roles": senior_skills,
            "insight": "Shows job market composition by seniority level and skills valued in senior positions.",
        }
    except Exception as e:
        logger.error("seniority insights error", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch seniority insights")


# ── Health Check ────────────────────────────────────────────────────────────

@router.get("/api/insights/health")
async def health_check():
    """Health check for insights service."""
    return {"status": "ok", "service": "insights"}
