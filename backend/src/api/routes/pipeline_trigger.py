"""
Real-time pipeline endpoint - fetches from Adzuna, cleans, and saves to database
Triggered on-demand through API
"""

import os
import structlog
import requests
import time
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, insert
from datetime import datetime
from typing import Dict, List

from src.db.session import get_db
from src.db.models import Job, Skill, JobSkill, PipelineRun, Base
from src.nlp.skill_extractor import extract_skills

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_API_KEY = os.getenv("ADZUNA_API_KEY")


@router.post("/run")
async def run_pipeline(
    pages: int = Query(3, ge=1, le=10, description="Number of pages to fetch from Adzuna"),
    db: AsyncSession = Depends(get_db),
):
    """
    Execute full ETL pipeline:
    1. Fetch jobs from Adzuna API
    2. Clean and normalize data
    3. Extract skills using NLP
    4. Save to database
    5. Return statistics
    """
    try:
        logger.info("Pipeline started", pages=pages)
        
        # Step 1: Fetch from Adzuna
        logger.info("Step 1: Fetching jobs from Adzuna API")
        raw_jobs = await fetch_adzuna_jobs(pages)
        logger.info(f"Fetched {len(raw_jobs)} jobs from Adzuna")
        
        if not raw_jobs:
            return {
                "status": "error",
                "message": "No jobs fetched from Adzuna API",
                "jobs_count": 0,
            }
        
        # Step 2: Clean data
        logger.info("Step 2: Cleaning job data")
        cleaned_jobs = clean_jobs_data(raw_jobs)
        logger.info(f"Cleaned {len(cleaned_jobs)} jobs")
        
        # Step 3: Extract skills and save to database
        logger.info("Step 3: Extracting skills and saving to database")
        stats = await save_jobs_to_db(db, cleaned_jobs)
        
        logger.info("Pipeline completed successfully", **stats)
        
        return {
            "status": "success",
            "message": "Pipeline completed successfully",
            "statistics": stats,
            "timestamp": datetime.now().isoformat(),
        }
        
    except Exception as e:
        logger.error("Pipeline failed", error=str(e))
        return {
            "status": "error",
            "message": f"Pipeline failed: {str(e)}",
            "error": str(e),
        }


async def fetch_adzuna_jobs(pages: int) -> List[Dict]:
    """Fetch jobs from Adzuna API with pagination"""
    
    if not ADZUNA_APP_ID or not ADZUNA_API_KEY:
        raise ValueError("Adzuna credentials not set in environment")
    
    all_jobs = []
    base_url = "https://api.adzuna.com/v1/api/jobs"
    country = "us"
    
    for page in range(1, pages + 1):
        try:
            logger.debug(f"Fetching page {page}")
            
            url = f"{base_url}/{country}/search/{page}"
            params = {
                "app_id": ADZUNA_APP_ID,
                "app_key": ADZUNA_API_KEY,
                "results_per_page": 50,
                "what": "python javascript java developer",
                "content-type": "application/json",
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                logger.warning(f"API status {response.status_code} on page {page}")
                if page == 1:  # Fail only if first page fails
                    raise Exception(f"Adzuna API error: {response.status_code}")
                break
            
            data = response.json()
            results = data.get("results", [])
            
            if not results:
                logger.info(f"No more results at page {page}")
                break
            
            all_jobs.extend(results)
            logger.info(f"Page {page}: fetched {len(results)} jobs (total: {len(all_jobs)})")
            
            # Rate limiting - be nice to the API
            time.sleep(1)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error on page {page}: {str(e)}")
            if page == 1:
                raise
            break
    
    return all_jobs


def clean_jobs_data(raw_jobs: List[Dict]) -> List[Dict]:
    """Clean and normalize job data from Adzuna"""
    
    cleaned = []
    
    for job in raw_jobs:
        try:
            # Extract fields from Adzuna response
            cleaned_job = {
                "id": str(job.get("id", "")),
                "title": job.get("title", "").strip(),
                "company": job.get("company", {}).get("display_name", "").strip() if isinstance(job.get("company"), dict) else "",
                "location": job.get("location", {}).get("display_name", "").strip() if isinstance(job.get("location"), dict) else "",
                "description": job.get("description", "").strip(),
                "salary_min": job.get("salary_min"),
                "salary_max": job.get("salary_max"),
                "source": "adzuna",
                "fetched_at": datetime.now(),
                "created": job.get("created"),
            }
            
            # Skip if missing critical fields
            if not cleaned_job["title"] or not cleaned_job["description"]:
                continue
            
            # Compute salary_mid
            if cleaned_job["salary_min"] and cleaned_job["salary_max"]:
                cleaned_job["salary_mid"] = (cleaned_job["salary_min"] + cleaned_job["salary_max"]) / 2
            else:
                cleaned_job["salary_mid"] = None
            
            # Parse city and country from location
            location_parts = cleaned_job["location"].split(",")
            cleaned_job["city"] = location_parts[0].strip() if location_parts else None
            cleaned_job["country"] = location_parts[-1].strip() if len(location_parts) > 1 else "US"
            
            cleaned.append(cleaned_job)
            
        except Exception as e:
            logger.warning(f"Error cleaning job {job.get('id')}: {str(e)}")
            continue
    
    return cleaned


async def save_jobs_to_db(db: AsyncSession, jobs: List[Dict]) -> Dict:
    """Save cleaned jobs and extract skills to database"""
    
    stats = {
        "jobs_inserted": 0,
        "duplicates_skipped": 0,
        "skills_extracted": 0,
        "errors": 0,
    }
    
    for job_data in jobs:
        try:
            job_id = job_data["id"]
            
            # Check if job already exists
            existing = await db.execute(
                select(Job).where(Job.id == job_id)
            )
            if existing.scalar_one_or_none():
                stats["duplicates_skipped"] += 1
                continue
            
            # Create Job record
            job = Job(
                id=job_id,
                title=job_data["title"],
                company=job_data["company"],
                location_raw=job_data["location"],
                city=job_data["city"],
                country=job_data["country"],
                salary_min=job_data.get("salary_min"),
                salary_max=job_data.get("salary_max"),
                salary_mid=job_data.get("salary_mid"),
                description=job_data["description"],
                source="adzuna",
                fetched_at=job_data["fetched_at"],
                remote=False,  # Default
                seniority=None,  # Could parse from title later
            )
            
            db.add(job)
            await db.flush()
            
            # Extract and link skills
            try:
                skills_found = extract_skills(job_data["description"], job_data["title"])
                
                for skill_name in skills_found:
                    # Get or create skill
                    skill_result = await db.execute(
                        select(Skill).where(Skill.name == skill_name)
                    )
                    skill = skill_result.scalar_one_or_none()
                    
                    if not skill:
                        skill = Skill(name=skill_name, category="technical", active=True)
                        db.add(skill)
                        await db.flush()
                    
                    # Link skill to job
                    job_skill = JobSkill(job_id=job_id, skill_id=skill.id)
                    db.add(job_skill)
                
                stats["skills_extracted"] += len(skills_found)
                
            except Exception as e:
                logger.warning(f"Skill extraction error for job {job_id}: {str(e)}")
            
            stats["jobs_inserted"] += 1
            
        except Exception as e:
            logger.error(f"Error saving job {job_data.get('id')}: {str(e)}")
            stats["errors"] += 1
            continue
    
    # Commit all changes
    await db.commit()
    
    logger.info("Jobs saved to database", **stats)
    return stats


@router.get("/status")
async def get_pipeline_status(db: AsyncSession = Depends(get_db)):
    """Get database statistics - how much data we have"""
    
    try:
        # Count jobs
        jobs_result = await db.execute(select(func.count(Job.id)))
        jobs_count = jobs_result.scalar() or 0
        
        # Count skills
        skills_result = await db.execute(select(func.count(Skill.id)))
        skills_count = skills_result.scalar() or 0
        
        # Count job-skill relationships
        job_skills_result = await db.execute(select(func.count(JobSkill.id)))
        job_skills_count = job_skills_result.scalar() or 0
        
        return {
            "status": "ok",
            "database_statistics": {
                "total_jobs": jobs_count,
                "total_skills": skills_count,
                "total_job_skill_links": job_skills_count,
            },
            "timestamp": datetime.now().isoformat(),
        }
        
    except Exception as e:
        logger.error(f"Error getting pipeline status: {str(e)}")
        return {
            "status": "error",
            "message": str(e),
        }
