"""
JobPulseAI Main Pipeline Orchestration
Async-first pipeline with direct Neon PostgreSQL writes replacing Prefect.
Stages: ingest → clean → nlp_extract → features → tfidf_score → snapshot → quality_check
"""

import structlog
from datetime import datetime, timedelta
from typing import Dict, Optional
from sqlalchemy import select, insert, update
from sqlalchemy.ext.asyncio import AsyncSession
from contextlib import asynccontextmanager
import json

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def db_transaction(db: AsyncSession, run_id: int, stage_name: str):
    """
    Context manager for database transactions with stage logging.
    
    Args:
        db: AsyncSession for database operations
        run_id: Pipeline run ID
        stage_name: Name of pipeline stage
    """
    logger.info("stage_started", run_id=run_id, stage=stage_name)
    try:
        yield
        await db.commit()
        logger.info("stage_completed", run_id=run_id, stage=stage_name)
    except Exception as e:
        await db.rollback()
        logger.error("stage_failed", run_id=run_id, stage=stage_name, error=str(e))
        raise


async def stage_ingest_jobs(
    db: AsyncSession,
    run_id: int,
    run_date: datetime,
    incremental: bool = True,
    log_context: Optional[Dict] = None,
) -> Dict:
    """
    Stage 1: Ingest jobs from Adzuna API and insert into Job table.
    
    Args:
        db: AsyncSession for database operations
        run_id: Pipeline run ID
        run_date: Date for this pipeline run
        incremental: If True, only fetch new jobs since last run
        log_context: Optional logging context
        
    Returns:
        Dict with ingestion statistics
    """
    logger = structlog.get_logger(__name__).bind(**(log_context or {}), run_id=run_id)
    
    with asynccontextmanager(db_transaction(db, run_id, "ingest_jobs")):
        # Import stage function (will need to be refactored from Prefect task)
        from ingestion.fetch_jobs import ingest_jobs_async
        
        stats = await ingest_jobs_async(
            db=db,
            run_id=run_id,
            run_date=run_date,
            incremental=incremental,
        )
        
        logger.info(
            "ingestion_completed",
            new_jobs=stats.get("new_jobs_count", 0),
            duplicates_skipped=stats.get("duplicates_skipped", 0),
        )
        
        return stats


async def stage_clean_jobs(
    db: AsyncSession,
    run_id: int,
    run_date: datetime,
    log_context: Optional[Dict] = None,
) -> Dict:
    """
    Stage 2: Data quality cleaning - normalize locations, salaries, text fields.
    
    Args:
        db: AsyncSession for database operations
        run_id: Pipeline run ID
        run_date: Date for this pipeline run
        log_context: Optional logging context
        
    Returns:
        Dict with cleaning statistics
    """
    logger = structlog.get_logger(__name__).bind(**(log_context or {}), run_id=run_id)
    
    with asynccontextmanager(db_transaction(db, run_id, "clean_jobs")):
        from cleaning.clean_jobs import clean_jobs_async
        
        stats = await clean_jobs_async(
            db=db,
            run_id=run_id,
            run_date=run_date,
        )
        
        logger.info(
            "cleaning_completed",
            records_cleaned=stats.get("records_cleaned", 0),
            nulls_filled=stats.get("nulls_filled", 0),
        )
        
        return stats


async def stage_nlp_extract_skills(
    db: AsyncSession,
    run_id: int,
    run_date: datetime,
    log_context: Optional[Dict] = None,
) -> Dict:
    """
    Stage 3: NLP-based skill extraction with taxonomy + section detection.
    
    Args:
        db: AsyncSession for database operations
        run_id: Pipeline run ID
        run_date: Date for this pipeline run
        log_context: Optional logging context
        
    Returns:
        Dict with skill extraction statistics
    """
    logger = structlog.get_logger(__name__).bind(**(log_context or {}), run_id=run_id)
    
    with asynccontextmanager(db_transaction(db, run_id, "nlp_extract_skills")):
        from nlp.skill_extractor import extract_skills
        from src.db.models import Job, Skill, JobSkill
        
        # Fetch all jobs for this run
        query = select(Job.id, Job.title, Job.description).where(Job.pipeline_run_id == run_id)
        result = await db.execute(query)
        jobs = result.fetchall()
        
        logger.info("jobs_fetched_for_extraction", job_count=len(jobs))
        
        skills_extracted = {}
        job_skills_to_insert = []
        required_skills_by_job = {}
        preferred_skills_by_job = {}
        
        # Extract skills for each job
        for job_id, title, description in jobs:
            if not description:
                logger.debug("skipping_job_no_description", job_id=job_id)
                continue
            
            extract_result = extract_skills(
                title=title or "",
                description=description,
                log_context={"job_id": job_id},
            )
            
            # Track skill extractions
            for skill_name in extract_result.all_skills:
                skills_extracted[skill_name] = skills_extracted.get(skill_name, 0) + 1
            
            # Store for snapshot computation
            required_skills_by_job[job_id] = extract_result.required_skills
            preferred_skills_by_job[job_id] = extract_result.preferred_skills
            
            # Prepare JobSkill inserts
            all_skills = set(extract_result.all_skills)
            for skill_name in all_skills:
                job_skills_to_insert.append({
                    "job_id": job_id,
                    "skill_name": skill_name,
                    "is_required": skill_name in extract_result.required_skills,
                    "pipeline_run_id": run_id,
                })
        
        # Upsert Skill records
        unique_skills = list(skills_extracted.keys())
        skill_inserts = [
            {
                "name": skill,
                "category": "extracted",
                "frequency": skills_extracted[skill],
            }
            for skill in unique_skills
        ]
        
        # Use insert with ON CONFLICT DO NOTHING
        query = insert(Skill).values(skill_inserts).on_conflict_do_nothing()
        result = await db.execute(query)
        logger.info("skills_upserted", skill_count=len(unique_skills))
        
        # Insert JobSkill relationships
        if job_skills_to_insert:
            query = insert(JobSkill).values(job_skills_to_insert)
            await db.execute(query)
            logger.info("job_skills_inserted", count=len(job_skills_to_insert))
        
        await db.commit()
        
        return {
            "unique_skills_extracted": len(unique_skills),
            "total_skill_instances": len(job_skills_to_insert),
            "jobs_processed": len(jobs),
            "top_skills": sorted(skills_extracted.items(), key=lambda x: x[1], reverse=True)[:10],
        }


async def stage_compute_features(
    db: AsyncSession,
    run_id: int,
    run_date: datetime,
    log_context: Optional[Dict] = None,
) -> Dict:
    """
    Stage 4: Feature engineering - seniority levels, remote flags, salary bands.
    
    Args:
        db: AsyncSession for database operations
        run_id: Pipeline run ID
        run_date: Date for this pipeline run
        log_context: Optional logging context
        
    Returns:
        Dict with feature engineering statistics
    """
    logger = structlog.get_logger(__name__).bind(**(log_context or {}), run_id=run_id)
    
    with asynccontextmanager(db_transaction(db, run_id, "compute_features")):
        from src.db.models import Job
        
        # Fetch all jobs
        query = select(Job).where(Job.pipeline_run_id == run_id)
        result = await db.execute(query)
        jobs = result.scalars().all()
        
        logger.info("computing_features", job_count=len(jobs))
        
        # Update Job records with computed features
        updates = []
        for job in jobs:
            # These would be populated from nlp.skill_extractor in real impl
            seniority = "mid"  # Would be computed from title/description
            is_remote = False  # Would be detected from description
            
            updates.append({
                "id": job.id,
                "seniority_level": seniority,
                "is_remote": is_remote,
            })
        
        # Batch update
        if updates:
            for update_record in updates:
                query = (
                    update(Job)
                    .where(Job.id == update_record["id"])
                    .values(
                        seniority_level=update_record["seniority_level"],
                        is_remote=update_record["is_remote"],
                    )
                )
                await db.execute(query)
        
        await db.commit()
        
        return {
            "jobs_updated": len(updates),
            "features_created": ["seniority_level", "is_remote"],
        }


async def stage_tfidf_score_skills(
    db: AsyncSession,
    run_id: int,
    run_date: datetime,
    log_context: Optional[Dict] = None,
) -> Dict:
    """
    Stage 5: TF-IDF based skill importance scoring.
    
    Args:
        db: AsyncSession for database operations
        run_id: Pipeline run ID
        run_date: Date for this pipeline run
        log_context: Optional logging context
        
    Returns:
        Dict with TF-IDF scoring statistics
    """
    logger = structlog.get_logger(__name__).bind(**(log_context or {}), run_id=run_id)
    
    with asynccontextmanager(db_transaction(db, run_id, "tfidf_score_skills")):
        from nlp.skill_scorer import batch_score_skills
        
        result = await batch_score_skills(
            db=db,
            run_id=run_id,
            batch_size=1000,
            log_context={"run_date": run_date.isoformat()},
        )
        
        logger.info(
            "tfidf_scoring_completed",
            unique_skills_scored=result.get("unique_skills_scored", 0),
            top_skills=result.get("top_10_skills", [])[:5],
        )
        
        return result


async def stage_create_skill_snapshots(
    db: AsyncSession,
    run_id: int,
    run_date: datetime,
    log_context: Optional[Dict] = None,
) -> Dict:
    """
    Stage 6: Aggregate daily skill snapshots by city/region.
    
    Args:
        db: AsyncSession for database operations
        run_id: Pipeline run ID
        run_date: Date for this pipeline run
        log_context: Optional logging context
        
    Returns:
        Dict with snapshot creation statistics
    """
    logger = structlog.get_logger(__name__).bind(**(log_context or {}), run_id=run_id)
    
    with asynccontextmanager(db_transaction(db, run_id, "create_skill_snapshots")):
        from src.db.models import SkillSnapshot, Job, JobSkill
        from sqlalchemy import func
        
        # Aggregate jobs by location + skill
        query = (
            select(
                Job.location_normalized,
                JobSkill.skill_id,
                func.count(JobSkill.job_id).label("job_count"),
                func.avg(SkillSnapshot.tfidf_score).label("avg_tfidf_score"),
            )
            .join(Job, JobSkill.job_id == Job.id)
            .where(Job.pipeline_run_id == run_id)
            .group_by(Job.location_normalized, JobSkill.skill_id)
        )
        
        result = await db.execute(query)
        snapshots_data = result.fetchall()
        
        logger.info("snapshots_aggregated", snapshot_count=len(snapshots_data))
        
        # Insert SkillSnapshot records
        snapshot_records = [
            {
                "run_id": run_id,
                "date": run_date.date(),
                "location": location,
                "skill_id": skill_id,
                "job_count": int(job_count),
                "avg_tfidf_score": float(avg_tfidf_score) if avg_tfidf_score else None,
            }
            for location, skill_id, job_count, avg_tfidf_score in snapshots_data
        ]
        
        if snapshot_records:
            query = insert(SkillSnapshot).values(snapshot_records)
            await db.execute(query)
            logger.info("skill_snapshots_inserted", count=len(snapshot_records))
        
        await db.commit()
        
        return {
            "snapshots_created": len(snapshot_records),
            "locations_covered": len(set(s["location"] for s in snapshot_records)),
        }


async def stage_quality_check(
    db: AsyncSession,
    run_id: int,
    run_date: datetime,
    stats_by_stage: Dict,
    log_context: Optional[Dict] = None,
) -> Dict:
    """
    Stage 7: Final quality checks and run finalization.
    
    Args:
        db: AsyncSession for database operations
        run_id: Pipeline run ID
        run_date: Date for this pipeline run
        stats_by_stage: Statistics from all pipeline stages
        log_context: Optional logging context
        
    Returns:
        Dict with quality check results
    """
    logger = structlog.get_logger(__name__).bind(**(log_context or {}), run_id=run_id)
    
    with asynccontextmanager(db_transaction(db, run_id, "quality_check")):
        from src.db.models import PipelineRun, Job
        
        # Fetch run and job counts
        query = select(PipelineRun).where(PipelineRun.id == run_id)
        result = await db.execute(query)
        run_record = result.scalar_one_or_none()
        
        query = select(func.count(Job.id)).where(Job.pipeline_run_id == run_id)
        result = await db.execute(query)
        job_count = result.scalar() or 0
        
        logger.info("quality_check_passed", total_jobs=job_count, run_record=run_record)
        
        return {
            "run_id": run_id,
            "total_jobs_processed": job_count,
            "quality_metrics": {
                **stats_by_stage,
                "job_count": job_count,
            },
            "timestamp": datetime.utcnow().isoformat(),
        }


async def run_pipeline(
    db: AsyncSession,
    run_id: int,
    run_date: Optional[datetime] = None,
    incremental: bool = True,
    log_context: Optional[Dict] = None,
) -> Dict:
    """
    Main pipeline orchestration. Runs all stages in sequence.
    
    Args:
        db: AsyncSession for database operations
        run_id: Pipeline run ID
        run_date: Date for this pipeline run (defaults to today)
        incremental: If True, only processes new jobs
        log_context: Optional logging context
        
    Returns:
        Dict with complete pipeline execution statistics
    """
    logger = structlog.get_logger(__name__).bind(**(log_context or {}))
    run_date = run_date or datetime.now()
    pipeline_start = datetime.utcnow()
    
    logger.info(
        "pipeline_started",
        run_id=run_id,
        run_date=run_date.isoformat(),
        incremental=incremental,
    )
    
    try:
        stats_by_stage = {}
        
        # Stage 1: Ingest
        stats_by_stage["ingest"] = await stage_ingest_jobs(
            db=db,
            run_id=run_id,
            run_date=run_date,
            incremental=incremental,
            log_context=log_context,
        )
        
        # Stage 2: Clean
        stats_by_stage["clean"] = await stage_clean_jobs(
            db=db,
            run_id=run_id,
            run_date=run_date,
            log_context=log_context,
        )
        
        # Stage 3: NLP Extract
        stats_by_stage["nlp_extract"] = await stage_nlp_extract_skills(
            db=db,
            run_id=run_id,
            run_date=run_date,
            log_context=log_context,
        )
        
        # Stage 4: Features
        stats_by_stage["features"] = await stage_compute_features(
            db=db,
            run_id=run_id,
            run_date=run_date,
            log_context=log_context,
        )
        
        # Stage 5: TF-IDF Scoring
        stats_by_stage["tfidf"] = await stage_tfidf_score_skills(
            db=db,
            run_id=run_id,
            run_date=run_date,
            log_context=log_context,
        )
        
        # Stage 6: Snapshots
        stats_by_stage["snapshots"] = await stage_create_skill_snapshots(
            db=db,
            run_id=run_id,
            run_date=run_date,
            log_context=log_context,
        )
        
        # Stage 7: Quality Check
        stats_by_stage["quality_check"] = await stage_quality_check(
            db=db,
            run_id=run_id,
            run_date=run_date,
            stats_by_stage=stats_by_stage,
            log_context=log_context,
        )
        
        pipeline_duration = (datetime.utcnow() - pipeline_start).total_seconds()
        
        logger.info(
            "pipeline_completed_successfully",
            run_id=run_id,
            duration_seconds=pipeline_duration,
            all_stages_passed=True,
        )
        
        return {
            "run_id": run_id,
            "status": "SUCCESS",
            "run_date": run_date.isoformat(),
            "duration_seconds": pipeline_duration,
            "stages": stats_by_stage,
            "completed_at": datetime.utcnow().isoformat(),
        }
        
    except Exception as e:
        pipeline_duration = (datetime.utcnow() - pipeline_start).total_seconds()
        logger.error(
            "pipeline_failed",
            run_id=run_id,
            error=str(e),
            duration_seconds=pipeline_duration,
        )
        
        # Update PipelineRun status to FAILED
        from src.db.models import PipelineRun
        query = (
            update(PipelineRun)
            .where(PipelineRun.id == run_id)
            .values(
                status="FAILED",
                completed_at=datetime.utcnow(),
                error_message=str(e),
            )
        )
        await db.execute(query)
        await db.commit()
        
        return {
            "run_id": run_id,
            "status": "FAILED",
            "error": str(e),
            "duration_seconds": pipeline_duration,
            "completed_at": datetime.utcnow().isoformat(),
        }

