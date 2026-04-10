#!/usr/bin/env python
"""
Comprehensive local verification script
Tests database connection, runs ETL pipeline, and verifies data
"""

import asyncio
import os
import sys
from pathlib import Path

# Load .env file FIRST
from dotenv import load_dotenv
load_dotenv()  # Load from backend/.env

# Add backend directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import structlog
from sqlalchemy import text, select, func

from src.db.session import async_engine, async_session_maker
from src.db.models import Base, Job, Skill, JobSkill, PipelineRun

logger = structlog.get_logger(__name__)


async def verify_database_connection():
    """Test if database is accessible"""
    print("\n" + "="*70)
    print("1️⃣  TESTING DATABASE CONNECTION")
    print("="*70)
    
    try:
        async with async_engine.begin() as conn:
            result = await conn.execute(text("SELECT NOW()"))
            timestamp = result.scalar()
            print(f"✅ Database connected! Server time: {timestamp}")
            return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


async def verify_tables_exist():
    """Check if all required tables exist"""
    print("\n" + "="*70)
    print("2️⃣  CHECKING DATABASE TABLES")
    print("="*70)
    
    try:
        async with async_engine.begin() as conn:
            result = await conn.execute(
                text("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    ORDER BY table_name
                """)
            )
            tables = result.fetchall()
            
            if tables:
                print(f"✅ Found {len(tables)} tables:")
                for table in tables:
                    print(f"   📋 {table[0]}")
                return True
            else:
                print("❌ No tables found! Run: python backend/init_db.py")
                return False
    except Exception as e:
        print(f"❌ Error checking tables: {e}")
        return False


async def check_data_counts():
    """Count records in each table"""
    print("\n" + "="*70)
    print("3️⃣  CHECKING DATA COUNTS")
    print("="*70)
    
    try:
        async with async_session_maker() as session:
            # Count jobs
            jobs_result = await session.execute(select(func.count(Job.id)))
            jobs_count = jobs_result.scalar() or 0
            print(f"   📊 Jobs in database: {jobs_count}")
            
            # Count skills
            skills_result = await session.execute(select(func.count(Skill.id)))
            skills_count = skills_result.scalar() or 0
            print(f"   📊 Skills in database: {skills_count}")
            
            # Count job-skill relationships
            job_skills_result = await session.execute(select(func.count(JobSkill.id)))
            job_skills_count = job_skills_result.scalar() or 0
            print(f"   📊 Job-Skill relationships: {job_skills_count}")
            
            # Count pipeline runs
            runs_result = await session.execute(select(func.count(PipelineRun.id)))
            runs_count = runs_result.scalar() or 0
            print(f"   📊 Pipeline runs logged: {runs_count}")
            
            if jobs_count > 0:
                print(f"\n✅ Database has {jobs_count} jobs!")
                return True
            else:
                print(f"\n⚠️  Database is empty. Run the ETL pipeline:")
                print("      curl -X POST http://localhost:8000/api/pipeline/run?pages=2")
                return False
                
    except Exception as e:
        print(f"❌ Error counting data: {e}")
        return False


async def show_recent_pipeline_runs():
    """Display recent pipeline execution history"""
    print("\n" + "="*70)
    print("4️⃣  RECENT PIPELINE RUNS")
    print("="*70)
    
    try:
        async with async_session_maker() as session:
            # Get last 5 runs
            runs_result = await session.execute(
                select(PipelineRun)
                .order_by(PipelineRun.started_at.desc())
                .limit(5)
            )
            runs = runs_result.scalars().all()
            
            if runs:
                for run in runs:
                    duration = None
                    if run.finished_at and run.started_at:
                        duration = (run.finished_at - run.started_at).total_seconds()
                    
                    print(f"\n   Run ID: {run.id}")
                    print(f"   Status: {run.status.upper()}")
                    print(f"   Started: {run.started_at}")
                    if run.finished_at:
                        print(f"   Finished: {run.finished_at}")
                        print(f"   Duration: {duration:.1f}s" if duration else "   Duration: calculating...")
                    print(f"   Jobs Fetched: {run.jobs_fetched}")
                    print(f"   Jobs Inserted: {run.jobs_inserted}")
                    print(f"   Jobs Skipped: {run.jobs_skipped}")
                    if run.error_message:
                        print(f"   Error: {run.error_message}")
                        
                print("\n✅ Pipeline runs found!")
                return True
            else:
                print("\n⚠️  No pipeline runs recorded yet")
                return False
                
    except Exception as e:
        print(f"❌ Error retrieving pipeline runs: {e}")
        return False


async def show_sample_jobs():
    """Display sample jobs from database"""
    print("\n" + "="*70)
    print("5️⃣  SAMPLE JOBS IN DATABASE")
    print("="*70)
    
    try:
        async with async_session_maker() as session:
            # Get last 3 jobs
            jobs_result = await session.execute(
                select(Job)
                .order_by(Job.fetched_at.desc())
                .limit(3)
            )
            jobs = jobs_result.scalars().all()
            
            if jobs:
                for i, job in enumerate(jobs, 1):
                    print(f"\n   Job {i}:")
                    print(f"   Title: {job.title}")
                    print(f"   Company: {job.company}")
                    print(f"   Location: {job.location_raw}")
                    if job.salary_min and job.salary_max:
                        print(f"   Salary: ${job.salary_min:,} - ${job.salary_max:,}")
                    print(f"   Fetched: {job.fetched_at}")
                    print(f"   Description: {job.description[:100]}...")
                
                print("\n✅ Sample jobs displayed!")
                return True
            else:
                print("\n⚠️  No jobs in database")
                return False
                
    except Exception as e:
        print(f"❌ Error fetching jobs: {e}")
        return False


async def show_available_skills():
    """Show available skills"""
    print("\n" + "="*70)
    print("6️⃣  AVAILABLE SKILLS")
    print("="*70)
    
    try:
        async with async_session_maker() as session:
            # Get sample skills
            skills_result = await session.execute(
                select(Skill)
                .order_by(Skill.name)
                .limit(10)
            )
            skills = skills_result.scalars().all()
            
            if skills:
                print(f"   📋 First 10 skills:")
                for skill in skills:
                    print(f"   • {skill.name} ({skill.category})")
                
                print("\n✅ Skills found!")
                return True
            else:
                print("\n⚠️  No skills in database")
                return False
                
    except Exception as e:
        print(f"❌ Error fetching skills: {e}")
        return False


async def run_all_checks():
    """Run all verification checks"""
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║" + " "*15 + "LOCAL SETUP VERIFICATION" + " "*29 + "║")
    print("╚" + "="*68 + "╝")
    
    # Run checks
    connected = await verify_database_connection()
    
    if not connected:
        print("\n❌ Cannot proceed - database not connected")
        return False
    
    tables_exist = await verify_tables_exist()
    
    if not tables_exist:
        print("\n❌ Cannot proceed - tables don't exist")
        print("   Run: python backend/init_db.py")
        return False
    
    # Rest of the checks
    await check_data_counts()
    await show_recent_pipeline_runs()
    await show_sample_jobs()
    await show_available_skills()
    
    print("\n" + "="*70)
    print("✅ VERIFICATION COMPLETE")
    print("="*70)
    print("\n📝 Next steps:")
    print("   1. Start the backend: uvicorn src.api.main:app --reload")
    print("   2. Test the API: http://localhost:8000/api/docs")
    print("   3. Trigger pipeline: curl -X POST http://localhost:8000/api/pipeline/run?pages=2")
    print("   4. Check results: Re-run this script\n")


if __name__ == "__main__":
    try:
        asyncio.run(run_all_checks())
    except KeyboardInterrupt:
        print("\n\n⚠️  Verification interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)
