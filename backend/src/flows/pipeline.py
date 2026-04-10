"""
End-to-End Phase 2 Pipeline Orchestration
Fetches jobs from Adzuna → Extracts Skills → Stores in DB → Scores Skills
"""

import asyncio
import structlog
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from sqlalchemy import select, insert, func
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)


async def run_full_pipeline(
    db: AsyncSession,
    locations: List[str] = None,
    countries: List[str] = None,
    keywords_list: List[str] = None,
    incremental: bool = True,
) -> Dict[str, Any]:
    """
    Execute complete JobPulse pipeline:
    1. Fetch jobs from Adzuna API
    2. Extract skills from descriptions
    3. Insert jobs and skills into database
    4. Calculate TF-IDF scores
    5. Create skill snapshots for trending
    
    Args:
        db: AsyncSession for database operations
        locations: Job locations to search (defaults to major cities)
        countries: Country codes to search (defaults to GB, NL, DE)
        keywords_list: Search keywords (defaults to tech roles)
        incremental: If True, only fetch recent jobs
        
    Returns:
        Dict with pipeline execution results
    """
    
    locations = locations or ["London", "Manchester", "Amsterdam", "Berlin"]
    countries = countries or ["gb", "nl", "de"]
    keywords_list = keywords_list or ["python developer", "javascript developer", "data engineer"]
    
    pipeline_start = datetime.utcnow()
    run_id = None
    
    try:
        logger.info(
            "pipeline_started",
            locations=locations,
            countries=countries,
            keywords=keywords_list,
        )
        
        # ===== STAGE 0: Create Tables if Needed =====
        logger.info("stage_0_starting", stage="create_tables")
        
        from src.db.models import Base
        from src.db.session import async_engine
        
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        logger.info("tables_ensured")
        
        # ===== STAGE 1: Create Pipeline Run Record =====
        logger.info("stage_1_starting", stage="create_pipeline_run")
        
        from src.db.models import PipelineRun
        
        pipeline_run = PipelineRun(
            started_at=pipeline_start,
            finished_at=None,
            status="running",
            jobs_fetched=0,
            jobs_inserted=0,
            jobs_skipped=0,
            error_message=None,
        )
        db.add(pipeline_run)
        await db.commit()
        run_id = pipeline_run.id
        
        logger.info("pipeline_run_created", run_id=run_id)
        
        # ===== STAGE 2: Fetch Jobs from Adzuna =====
        logger.info("stage_2_starting", stage="fetch_jobs")
        
        from src.ingestion.adzuna_client import fetch_fresh_jobs
        
        all_jobs = await fetch_fresh_jobs(
            locations=locations,
            countries=countries,
            keywords_list=keywords_list,
        )
        
        logger.info("jobs_fetched", count=len(all_jobs))
        
        # ===== STAGE 3: Extract Skills & Process Jobs =====
        logger.info("stage_3_starting", stage="extract_skills")
        
        from src.nlp.skill_extractor import extract_skills
        from src.db.models import Job, Skill, JobSkill
        
        jobs_to_insert = []
        job_skills_to_insert = []
        descriptions_for_scoring = []
        
        skills_cache = {}  # skill_name -> skill_id
        
        # Preload existing skills from database
        existing_skills = await db.execute(select(Skill))
        for skill in existing_skills.scalars().all():
            skills_cache[skill.name] = skill.id
        
        all_unique_skills_to_create = set()  # Track new skills to create
        
        for idx, adzuna_job in enumerate(all_jobs):
            try:
                # Extract API fields
                job_id = adzuna_job.get("id", f"adzuna_{idx}")
                title = adzuna_job.get("title", "")
                company_name = adzuna_job.get("company", {}).get("display_name", "Unknown")
                location = adzuna_job.get("location", {}).get("display_name", "Remote")
                description = adzuna_job.get("description", "")
                salary_min = adzuna_job.get("salary_min")
                salary_max = adzuna_job.get("salary_max")
                
                # Skip if no description
                if not description or not title:
                    logger.debug("job_skipped_missing_fields", job_id=job_id)
                    continue
                
                # Extract skills using NLP
                extracted = extract_skills(
                    title=title,
                    description=description,
                    log_context={"job_id": job_id},
                )
                
                # Calculate salary mid
                salary_mid = None
                if salary_min and salary_max:
                    salary_mid = (salary_min + salary_max) / 2
                
                # Determine country from location
                country_map = {
                    "london": "GB", "manchester": "GB", "birmingham": "GB",
                    "amsterdam": "NL", "rotterdam": "NL",
                    "berlin": "DE", "munich": "DE",
                }
                country = country_map.get(location.lower(), "GB")
                
                # Create job record
                job = Job(
                    id=job_id,
                    title=title,
                    company=company_name,
                    location_raw=location,
                    city=location if location.lower() != "remote" else None,
                    country=country,
                    salary_min=salary_min,
                    salary_max=salary_max,
                    salary_mid=salary_mid,
                    description=description[:5000],  # Truncate to 5000 chars
                    seniority=extracted.seniority_level,
                    remote=extracted.is_remote,
                    fetched_at=datetime.utcnow(),
                )
                jobs_to_insert.append(job)
                descriptions_for_scoring.append(description)
                
                # Track skills to create
                for skill_name in extracted.all_skills:
                    if skill_name not in skills_cache:
                        all_unique_skills_to_create.add(skill_name)
                    
                    job_skill = JobSkill(
                        job_id=job_id,
                        skill_id=None,  # Will set after skill is committed
                        is_required=skill_name in extracted.required_skills,
                    )
                    job_skills_to_insert.append({
                        "job_skill": job_skill,
                        "skill_name": skill_name,
                    })
                
            except Exception as e:
                logger.error("job_processing_failed", error=str(e), job_id=adzuna_job.get("id"))
                continue
        
        logger.info(
            "skills_extracted",
            jobs_processed=len(jobs_to_insert),
            total_job_skills=len(job_skills_to_insert),
            new_skills_to_create=len(all_unique_skills_to_create),
        )
        
        # ===== STAGE 4: Insert Jobs & Skills into Database =====
        logger.info("stage_4_starting", stage="insert_into_database")
        
        # Create new skills first
        for skill_name in all_unique_skills_to_create:
            new_skill = Skill(
                name=skill_name,
                category="extracted",  # Mark as from Adzuna extraction
            )
            db.add(new_skill)
            logger.debug("new_skill_planned", skill_name=skill_name)
        
        await db.commit()  # Commit to generate IDs
        
        # Reload skills with IDs
        existing_skills = await db.execute(select(Skill))
        for skill in existing_skills.scalars().all():
            skills_cache[skill.name] = skill.id
        
        # Insert jobs
        for job in jobs_to_insert:
            existing_job = await db.execute(
                select(Job).where(Job.id == job.id)
            )
            if not existing_job.scalars().first():
                db.add(job)
        
        await db.commit()
        jobs_inserted = len(jobs_to_insert)
        
        logger.info("jobs_inserted", count=jobs_inserted)
        
        # Insert job-skill relationships
        for item in job_skills_to_insert:
            job_skill = item["job_skill"]
            skill_name = item["skill_name"]
            
            # Set skill_id
            if skill_name in skills_cache:
                job_skill.skill_id = skills_cache[skill_name]
                
                # Check if relationship already exists
                existing = await db.execute(
                    select(JobSkill).where(
                        (JobSkill.job_id == job_skill.job_id) &
                        (JobSkill.skill_id == job_skill.skill_id)
                    )
                )
                if not existing.scalars().first():
                    db.add(job_skill)
        
        await db.commit()
        job_skills_inserted = len(job_skills_to_insert)
        
        logger.info("job_skills_inserted", count=job_skills_inserted)
        
        # ===== STAGE 5: Calculate TF-IDF Scores =====
        logger.info("stage_5_starting", stage="tfidf_scoring")
        
        if descriptions_for_scoring:
            from src.nlp.skill_scorer import SkillScorer
            
            scorer = SkillScorer()
            scorer.add_documents(descriptions_for_scoring)
            scorer.fit()
            
            skill_scores = scorer.get_skill_importance_scores()
            top_skills = scorer.get_top_skills(k=10)
            
            logger.info(
                "tfidf_scores_computed",
                unique_skills=len(skill_scores),
                top_10=[f"{s}({score:.3f})" for s, score in top_skills],
            )
        else:
            skill_scores = {}
        
        # ===== STAGE 6: Create Skill Snapshots =====
        logger.info("stage_6_starting", stage="create_skill_snapshots")
        
        from src.db.models import SkillSnapshot
        from sqlalchemy import func as sql_func
        
        snapshot_date = datetime.utcnow().date()
        snapshots_created = 0
        
        # Get unique cities from inserted jobs
        cities_result = await db.execute(
            select(sql_func.distinct(Job.city)).where(
                Job.fetched_at >= pipeline_start
            )
        )
        cities = [c[0] for c in cities_result.fetchall() if c[0]]
        
        if not cities:
            cities = ["London", "Amsterdam", "Berlin"]
        
        # Create snapshots for each skill and city
        for skill_name, tfidf_score in skill_scores.items():
            if skill_name not in skills_cache:
                continue
            
            skill_id = skills_cache[skill_name]
            
            # Count jobs with this skill
            job_count_result = await db.execute(
                select(sql_func.count(Job.id)).distinct().join(JobSkill).where(
                    (JobSkill.skill_id == skill_id) &
                    (Job.fetched_at >= pipeline_start)
                )
            )
            job_count = job_count_result.scalar() or 0
            
            # Calculate average salary for jobs with this skill
            salary_result = await db.execute(
                select(sql_func.avg(Job.salary_mid)).join(JobSkill).where(
                    (JobSkill.skill_id == skill_id) &
                    (Job.fetched_at >= pipeline_start)
                )
            )
            avg_salary = salary_result.scalar() or 0
            
            for city in cities:
                snapshot = SkillSnapshot(
                    skill_id=skill_id,
                    snapshot_date=snapshot_date,
                    job_count=max(job_count, 1),  # At least 1
                    avg_salary_mid=int(avg_salary) if avg_salary else 0,
                    tfidf_score=round(tfidf_score, 3),
                    city=city,
                    country=country_map.get(city.lower(), "GB"),
                )
                db.add(snapshot)
                snapshots_created += 1
        
        await db.commit()
        
        logger.info("skill_snapshots_created", count=snapshots_created)
        
        # ===== Update Pipeline Run with Results =====
        pipeline_end = datetime.utcnow()
        
        pipeline_update = await db.execute(
            select(PipelineRun).where(PipelineRun.id == run_id)
        )
        pipeline_run = pipeline_update.scalars().first()
        
        if pipeline_run:
            pipeline_run.finished_at = pipeline_end
            pipeline_run.status = "success"
            pipeline_run.jobs_fetched = len(all_jobs)
            pipeline_run.jobs_inserted = jobs_inserted
            pipeline_run.jobs_skipped = len(all_jobs) - jobs_inserted
            
            await db.commit()
        
        logger.info(
            "pipeline_completed",
            run_id=run_id,
            total_jobs_fetched=len(all_jobs),
            jobs_inserted=jobs_inserted,
            job_skills_created=job_skills_inserted,
            snapshots_created=snapshots_created,
            duration_seconds=(pipeline_end - pipeline_start).total_seconds(),
        )
        
        return {
            "run_id": run_id,
            "status": "success",
            "jobs_fetched": len(all_jobs),
            "jobs_inserted": jobs_inserted,
            "jobs_skipped": len(all_jobs) - jobs_inserted,
            "job_skills_created": job_skills_inserted,
            "skill_snapshots_created": snapshots_created,
            "tfidf_skills_scored": len(skill_scores),
            "duration_seconds": (pipeline_end - pipeline_start).total_seconds(),
            "started_at": pipeline_start.isoformat(),
            "finished_at": pipeline_end.isoformat(),
        }
        
    except Exception as e:
        logger.error("pipeline_execution_failed", error=str(e), run_id=run_id)
        
        # Update pipeline run with error
        if run_id:
            try:
                pipeline_update = await db.execute(
                    select(PipelineRun).where(PipelineRun.id == run_id)
                )
                pipeline_run = pipeline_update.scalars().first()
                
                if pipeline_run:
                    pipeline_run.status = "failed"
                    pipeline_run.finished_at = datetime.utcnow()
                    pipeline_run.error_message = str(e)[:500]
                    
                    await db.commit()
            except:
                pass
        
        raise


if __name__ == "__main__":
    import asyncio
    from src.db.session import async_session_maker
    
    async def test_pipeline():
        """Test the end-to-end pipeline."""
        async with async_session_maker() as session:
            result = await run_full_pipeline(
                db=session,
                countries=["gb"],
                keywords_list=["python"],
            )
            print("\n" + "="*60)
            print("PIPELINE RESULTS")
            print("="*60)
            for key, value in result.items():
                print(f"{key:.<40} {value}")
    
    asyncio.run(test_pipeline())
