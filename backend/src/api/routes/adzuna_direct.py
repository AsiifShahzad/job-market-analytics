"""
Direct Adzuna API wrapper endpoints - fetch data without database dependency
Useful for frontend dev/testing when database is unavailable
"""

import os
import structlog
from fastapi import APIRouter, Query
from typing import Optional, List
from datetime import datetime

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/adzuna", tags=["adzuna"])

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_API_KEY = os.getenv("ADZUNA_API_KEY")


@router.get("/jobs", response_model=dict)
async def get_adzuna_jobs(
    location: str = Query("GB", description="Job location"),
    limit: int = Query(10, ge=1, le=50, description="Number of jobs to return"),
):
    """
    Get jobs data (mock version for quick testing).
    """
    mock_jobs = [
        {
            "title": "Senior Python Developer",
            "company": "Tech Corp",
            "location": "London, UK",
            "description": "We are looking for a Senior Python Developer with expertise in FastAPI, Docker, and AWS.",
            "salary": "£80,000 - £120,000",
        },
        {
            "title": "React Frontend Engineer",
            "company": "Web Solutions",
            "location": "Manchester, UK",
            "description": "Join our team to build modern React applications with TypeScript and GraphQL experience required.",
            "salary": "£60,000 - £90,000",
        },
        {
            "title": "DevOps Engineer",
            "company": "Cloud Systems",
            "location": "Bristol, UK",
            "description": "DevOps professional needed with Kubernetes, Terraform, and CI/CD experience.",
            "salary": "£70,000 - £110,000",
        },
        {
            "title": "Data Scientist",
            "company": "Analytics Inc",
            "location": "Birmingham, UK",
            "description": "Seeking Data Scientist proficient in Python, Machine Learning, TensorFlow, and Pandas.",
            "salary": "£65,000 - £100,000",
        },
        {
            "title": "Full Stack Developer",
            "company": "StartUp Ltd",
            "location": "London, UK",
            "description": "Full stack developer with Node.js backend and React frontend expertise needed.",
            "salary": "£55,000 - £85,000",
        },
    ]
    
    return {
        "location": location,
        "jobs_count": len(mock_jobs[:limit]),
        "jobs": mock_jobs[:limit]
    }


@router.get("/skills-summary", response_model=dict)
async def get_skills_summary(
    location: str = Query("GB", description="Job location"),
    limit: int = Query(20, ge=1, le=50, description="Number of top skills to return"),
):
    """
    Get aggregated top skills from job data.
    """
    # Mock data: skills and their frequency
    skills_data = [
        {"name": "Python", "job_count": 145, "percentage": 23.5},
        {"name": "JavaScript", "job_count": 132, "percentage": 21.4},
        {"name": "SQL", "job_count": 128, "percentage": 20.8},
        {"name": "React", "job_count": 98, "percentage": 15.9},
        {"name": "Docker", "job_count": 87, "percentage": 14.1},
        {"name": "AWS", "job_count": 76, "percentage": 12.3},
        {"name": "TypeScript", "job_count": 65, "percentage": 10.5},
        {"name": "Node.js", "job_count": 62, "percentage": 10.1},
        {"name": "Kubernetes", "job_count": 54, "percentage": 8.8},
        {"name": "FastAPI", "job_count": 43, "percentage": 7.0},
        {"name": "Machine Learning", "job_count": 39, "percentage": 6.3},
        {"name": "TensorFlow", "job_count": 31, "percentage": 5.0},
        {"name": "Git", "job_count": 92, "percentage": 14.9},
        {"name": "GraphQL", "job_count": 28, "percentage": 4.5},
        {"name": "PostgreSQL", "job_count": 71, "percentage": 11.5},
    ]
    
    return {
        "location": location,
        "timestamp": datetime.utcnow().isoformat(),
        "skills": sorted(skills_data, key=lambda x: x["job_count"], reverse=True)[:limit]
    }


@router.get("/trending-skills", response_model=dict)
async def get_trending_skills(
    period_days: int = Query(30, ge=1, le=365, description="Period in days"),
    limit: int = Query(10, ge=1, le=50, description="Number of trending skills to return"),
):
    """
    Get emerging/trending skills.
    """
    trending = [
        {"name": "AI/LLM", "growth_percent": 85, "new_jobs_this_period": 156},
        {"name": "Prompt Engineering", "growth_percent": 120, "new_jobs_this_period": 89},
        {"name": "Vector Databases", "growth_percent": 95, "new_jobs_this_period": 42},
        {"name": "Rust", "growth_percent": 45, "new_jobs_this_period": 78},
        {"name": "Go", "growth_percent": 38, "new_jobs_this_period": 65},
    ]
    
    return {
        "period_days": period_days,
        "timestamp": datetime.utcnow().isoformat(),
        "trending_skills": trending[:limit]
    }


@router.get("/salary-insights", response_model=dict)  
async def get_salary_insights(
    skill: Optional[str] = Query(None, description="Filter by skill"),
    location: str = Query("GB", description="Job location"),
):
    """
    Get salary insights for skills.
    """
    salary_data = {
        "Python": {"average": 85000, "min": 45000, "max": 180000, "percentile_90": 140000},
        "JavaScript": {"average": 72000, "min": 35000, "max": 160000, "percentile_90": 125000},
        "React": {"average": 78000, "min": 40000, "max": 170000, "percentile_90": 135000},
        "AWS": {"average": 95000, "min": 55000, "max": 200000, "percentile_90": 165000},
        "Machine Learning": {"average": 105000, "min": 60000, "max": 220000, "percentile_90": 180000},
    }
    
    if skill and skill in salary_data:
        return {
            "skill": skill,
            "location": location,
            "salary_insights": salary_data[skill]
        }
    
    return {
        "location": location,
        "all_skills_salary": salary_data
    }
