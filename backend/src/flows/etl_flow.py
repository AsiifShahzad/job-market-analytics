"""
Prefect ETL Flow - Automated job data ingestion and processing
Fetches from Adzuna API, cleans, extracts skills, and saves to Neon PostgreSQL
Schedule: Every 6 hours
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List
import asyncio

from prefect import flow, task, get_run_logger
from prefect.schedules import CronSchedule
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select, delete
import httpx

# Import backend modules
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.models import Job, Skill, JobSkill, SkillSnapshot, PipelineRun, Base
from src.nlp.skill_extractor import extract_skills
import structlog

logger = structlog.get_logger(__name__)


@task(name="fetch_adzuna_jobs", retries=2, retry_delay_seconds=60)
async def fetch_adzuna_jobs_task(pages: int = 5) -> List[Dict]:
    """Task: Fetch jobs from Adzuna API with pagination"""
    
    logger = get_run_logger()
    logger.info(f"Fetching {pages} pages from Adzuna API")
    
    ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
    ADZUNA_API_KEY = os.getenv("ADZUNA_API_KEY")
    
    if not ADZUNA_APP_ID or not ADZUNA_API_KEY:
        raise ValueError("Adzuna credentials not set")
    
    all_jobs = []
    base_url = "https://api.adzuna.com/v1/api/jobs"
    country = "us"
    
    async with httpx.AsyncClient(timeout=30) as client:
        for page in range(1, pages + 1):
            try:
                logger.info(f"Fetching page {page}/{pages}")
                
                url = f"{base_url}/{country}/search/{page}"
                params = {
                    "app_id": ADZUNA_APP_ID,
                    "app_key": ADZUNA_API_KEY,
                    "results_per_page": 50,
                    "what": "python javascript java developer engineer",
                    "content-type": "application/json",
                }
                
                response = await client.get(url, params=params)
                
                if response.status_code != 200:
                    logger.warning(f"API status {response.status_code} on page {page}")
                    if page == 1:
                        raise Exception(f"Adzuna API error: {response.status_code}")
                    break
                
                data = response.json()
                results = data.get("results", [])
                
                if not results:
                    logger.info(f"No more results at page {page}")
                    break
                
                all_jobs.extend(results)
                logger.info(f"Page {page}: fetched {len(results)} jobs (total: {len(all_jobs)})")
                
                # Rate limiting
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error on page {page}: {str(e)}")
                if page == 1:
                    raise
                break
    
    logger.info(f"Total jobs fetched: {len(all_jobs)}")
    return all_jobs


@task(name="clean_jobs_data")
def clean_jobs_data_task(raw_jobs: List[Dict]) -> List[Dict]:
    """Task: Clean and normalize job data"""
    
    logger = get_run_logger()
    logger.info(f"Cleaning {len(raw_jobs)} jobs")
    
    cleaned = []
    
    for job in raw_jobs:
        try:
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
            
            # Parse location
            location_parts = cleaned_job["location"].split(",")
            cleaned_job["city"] = location_parts[0].strip() if location_parts else None
            cleaned_job["country"] = location_parts[-1].strip() if len(location_parts) > 1 else "US"
            
            cleaned.append(cleaned_job)
            
        except Exception as e:
            logger.warning(f"Error cleaning job {job.get('id')}: {str(e)}")
            continue
    
    logger.info(f"Cleaned {len(cleaned)} jobs")
    return cleaned


@task(name="save_to_database", retries=2)
async def save_to_database_task(cleaned_jobs: List[Dict]) -> Dict:
    """Task: Save cleaned jobs to PostgreSQL database"""
    
    logger = get_run_logger()
    logger.info(f"Saving {len(cleaned_jobs)} jobs to database")
    
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set")
    
    # Create async engine
    engine = create_async_engine(DATABASE_URL, echo=False, future=True)
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    stats = {
        "jobs_inserted": 0,
        "duplicates_skipped": 0,
        "skills_extracted": 0,
        "errors": 0,
    }
    
    async with async_session() as db:
        for job_data in cleaned_jobs:
            try:
                job_id = job_data["id"]
                
                # Check if job exists
                existing = await db.execute(select(Job).where(Job.id == job_id))
                if existing.scalar_one_or_none():
                    stats["duplicates_skipped"] += 1
                    continue
                
                # Create job
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
                    remote=False,
                    seniority=None,
                )
                
                db.add(job)
                await db.flush()
                
                # Extract skills
                try:
                    skills_found = extract_skills(job_data["description"], job_data["title"])
                    
                    for skill_name in skills_found:
                        skill_result = await db.execute(select(Skill).where(Skill.name == skill_name))
                        skill = skill_result.scalar_one_or_none()
                        
                        if not skill:
                            skill = Skill(name=skill_name, category="technical", active=True)
                            db.add(skill)
                            await db.flush()
                        
                        job_skill = JobSkill(job_id=job_id, skill_id=skill.id)
                        db.add(job_skill)
                    
                    stats["skills_extracted"] += len(skills_found)
                    
                except Exception as e:
                    logger.warning(f"Skill extraction error for {job_id}: {str(e)}")
                
                stats["jobs_inserted"] += 1
                
            except Exception as e:
                logger.error(f"Error saving job {job_data.get('id')}: {str(e)}")
                stats["errors"] += 1
                continue
        
        await db.commit()
    
    await engine.dispose()
    
    logger.info(f"Database save completed: {stats}")
    return stats


@flow(
    name="ETL Pipeline - Adzuna to PostgreSQL",
    description="Automated job data ingestion from Adzuna API",
)
async def etl_pipeline(pages: int = 5) -> Dict:
    """
    Main ETL flow: Fetch → Clean → Save
    Run every 6 hours automatically
    """
    
    logger = get_run_logger()
    logger.info("=== ETL Pipeline Started ===")
    
    try:
        # Step 1: Fetch
        raw_jobs = await fetch_adzuna_jobs_task(pages)
        
        if not raw_jobs:
            logger.warning("No jobs fetched")
            return {"status": "error", "message": "No jobs fetched"}
        
        # Step 2: Clean
        cleaned_jobs = await clean_jobs_data_task(raw_jobs)
        
        # Step 3: Save
        stats = await save_to_database_task(cleaned_jobs)
        
        logger.info("=== ETL Pipeline Completed Successfully ===", **stats)
        
        return {
            "status": "success",
            "message": "ETL pipeline completed",
            "statistics": stats,
            "timestamp": datetime.now().isoformat(),
        }
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {str(e)}")
        return {
            "status": "error",
            "message": f"ETL pipeline failed: {str(e)}",
        }


if __name__ == "__main__":
    # Run the flow manually (useful for testing)
    result = asyncio.run(etl_pipeline(pages=3))
    print(f"\nPipeline Result: {result}")
