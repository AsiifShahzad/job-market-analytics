"""
PHASE 1: Seed Database Script
Inserts realistic fake data to populate the database for immediate frontend display
while the real Adzuna pipeline is being built.

Run with: python -m src.db.seed
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Tuple
import random
from pathlib import Path
from sqlalchemy import insert, select, func
from sqlalchemy.ext.asyncio import AsyncSession
import structlog
from dotenv import load_dotenv

# Load .env file
load_dotenv(Path(__file__).parent.parent.parent / ".env")

from src.db.session import async_session_maker, async_engine
from src.db.models import Job, Skill, JobSkill, SkillSnapshot, PipelineRun, Base

logger = structlog.get_logger(__name__)

# ============================================================================
# SEED DATA DEFINITIONS
# ============================================================================

SKILLS_TO_CREATE = {
    # Languages (15)
    "Python": "language",
    "JavaScript": "language",
    "TypeScript": "language",
    "Java": "language",
    "Go": "language",
    "Rust": "language",
    "SQL": "language",
    "Bash": "language",
    "C++": "language",
    "C#": "language",
    "PHP": "language",
    "Swift": "language",
    "Kotlin": "language",
    "Scala": "language",
    "R": "language",
    
    # Frameworks (10)
    "React": "framework",
    "Vue": "framework",
    "Angular": "framework",
    "FastAPI": "framework",
    "Django": "framework",
    "Flask": "framework",
    "Spring": "framework",
    "NestJS": "framework",
    "Express": "framework",
    "Tailwind CSS": "framework",
    
    # Cloud (4)
    "AWS": "cloud",
    "Google Cloud": "cloud",
    "Azure": "cloud",
    "Vercel": "cloud",
    
    # Data (8)
    "Spark": "data",
    "Kafka": "data",
    "Pandas": "data",
    "NumPy": "data",
    "TensorFlow": "data",
    "PyTorch": "data",
    "dbt": "data",
    "Airflow": "data",
    
    # Tools (10)
    "Docker": "tool",
    "Kubernetes": "tool",
    "Git": "tool",
    "Terraform": "tool",
    "Jenkins": "tool",
    "GitHub Actions": "tool",
    "Linux": "tool",
    "Ansible": "tool",
    "Grafana": "tool",
    "Prometheus": "tool",
    
    # Soft Skills (4)
    "Communication": "soft",
    "Leadership": "soft",
    "Agile": "soft",
    "Problem Solving": "soft",
}

JOBS_DATA = [
    # London
    {"title": "Senior Python Developer", "company": "TechCorp London", "location": "London", "salary_min": 80000, "salary_max": 120000, "skills": ["Python", "FastAPI", "Docker", "PostgreSQL", "AWS"]},
    {"title": "React Frontend Engineer", "company": "Digital Solutions", "location": "London", "salary_min": 70000, "salary_max": 110000, "skills": ["JavaScript", "React", "TypeScript", "Tailwind CSS", "Git"]},
    {"title": "Data Engineer", "company": "Analytics Plus", "location": "London", "salary_min": 75000, "salary_max": 115000, "skills": ["Python", "SQL", "Spark", "Airflow", "AWS"]},
    {"title": "DevOps Engineer", "company": "Cloud Nine", "location": "London", "salary_min": 85000, "salary_max": 130000, "skills": ["Docker", "Kubernetes", "Terraform", "Linux", "AWS"]},
    {"title": "Full Stack Developer", "company": "StartUp Hub", "location": "London", "salary_min": 65000, "salary_max": 105000, "skills": ["JavaScript", "React", "Node.js", "SQL", "Docker"]},
    {"title": "ML Engineer", "company": "AI Labs", "location": "London", "salary_min": 90000, "salary_max": 140000, "skills": ["Python", "TensorFlow", "PyTorch", "SQL", "AWS"]},
    {"title": "Backend Engineer", "company": "Platform Inc", "location": "London", "salary_min": 78000, "salary_max": 118000, "skills": ["Java", "Spring", "SQL", "Docker", "Kubernetes"]},
    
    # Berlin
    {"title": "Senior Frontend Developer", "company": "Berlin Tech", "location": "Berlin", "salary_min": 65000, "salary_max": 100000, "skills": ["React", "TypeScript", "JavaScript", "CSS", "Git"]},
    {"title": "Data Scientist", "company": "Data Lab Berlin", "location": "Berlin", "salary_min": 70000, "salary_max": 105000, "skills": ["Python", "Pandas", "TensorFlow", "SQL", "Jupyter"]},
    {"title": "Infrastructure Engineer", "company": "Cloud Berlin", "location": "Berlin", "salary_min": 72000, "salary_max": 110000, "skills": ["Kubernetes", "Docker", "Terraform", "Linux", "Bash"]},
    {"title": "Go Developer", "company": "Systems Berlin", "location": "Berlin", "salary_min": 68000, "salary_max": 102000, "skills": ["Go", "Docker", "SQL", "Git", "Linux"]},
    {"title": "Product Engineer", "company": "Startup Berlin", "location": "Berlin", "salary_min": 60000, "salary_max": 95000, "skills": ["JavaScript", "React", "Python", "SQL", "Docker"]},
    
    # Amsterdam
    {"title": "Senior Software Engineer", "company": "Dutch Tech", "location": "Amsterdam", "salary_min": 75000, "salary_max": 115000, "skills": ["Java", "Spring", "SQL", "Docker", "AWS"]},
    {"title": "Frontend Specialist", "company": "Amsterdam Digital", "location": "Amsterdam", "salary_min": 68000, "salary_max": 105000, "skills": ["React", "TypeScript", "Vue", "JavaScript", "CSS"]},
    {"title": "Platform Engineer", "company": "Cloud Amsterdam", "location": "Amsterdam", "salary_min": 80000, "salary_max": 125000, "skills": ["Kubernetes", "Docker", "Go", "Terraform", "Linux"]},
    {"title": "Analytics Engineer", "company": "Data Amsterdam", "location": "Amsterdam", "salary_min": 70000, "salary_max": 110000, "skills": ["SQL", "Python", "dbt", "Spark", "AWS"]},
    
    # New York
    {"title": "NYC Senior Engineer", "company": "NY Tech Corp", "location": "New York", "salary_min": 120000, "salary_max": 180000, "skills": ["Python", "Go", "Kubernetes", "AWS", "SQL"]},
    {"title": "React Engineer NYC", "company": "Digital NY", "location": "New York", "salary_min": 110000, "salary_max": 160000, "skills": ["React", "TypeScript", "JavaScript", "Node.js", "AWS"]},
    {"title": "Data Engineer NYC", "company": "Big Data Inc", "location": "New York", "salary_min": 115000, "salary_max": 170000, "skills": ["Python", "Spark", "Kafka", "SQL", "AWS"]},
    {"title": "DevOps Lead", "company": "Infra NYC", "location": "New York", "salary_min": 125000, "salary_max": 190000, "skills": ["Kubernetes", "Terraform", "Docker", "AWS", "Linux"]},
    
    # Remote
    {"title": "Remote Backend Engineer", "company": "Remote First Co", "location": "Remote", "salary_min": 85000, "salary_max": 140000, "skills": ["Python", "FastAPI", "PostgreSQL", "Docker", "AWS"], "remote": True},
    {"title": "Remote Full Stack", "company": "Global Tech", "location": "Remote", "salary_min": 80000, "salary_max": 130000, "skills": ["React", "Node.js", "Python", "SQL", "Docker"], "remote": True},
    {"title": "Remote Data Scientist", "company": "Remote Analytics", "location": "Remote", "salary_min": 95000, "salary_max": 145000, "skills": ["Python", "TensorFlow", "Spark", "SQL", "AWS"], "remote": True},
    {"title": "Remote DevOps Engineer", "company": "Cloud Services", "location": "Remote", "salary_min": 100000, "salary_max": 155000, "skills": ["Kubernetes", "Docker", "Terraform", "AWS", "Linux"], "remote": True},
]

# Add more jobs to reach 50
for i in range(50 - len(JOBS_DATA)):
    cities = ["London", "Berlin", "Amsterdam", "New York", "Remote"]
    companies = ["TechCorp", "Digital Solutions", "StartUp Hub", "Cloud Nine", "Platform Inc"]
    titles = ["Senior Developer", "Engineer", "ML Specialist", "Data Engineer", "DevOps Engineer"]
    
    JOBS_DATA.append({
        "title": f"{random.choice(titles)} {i}",
        "company": f"{random.choice(companies)} {i}",
        "location": random.choice(cities),
        "salary_min": random.randint(60000, 100000),
        "salary_max": random.randint(110000, 160000),
        "skills": random.sample(list(SKILLS_TO_CREATE.keys()), k=random.randint(3, 8)),
        "remote": random.choice([True, False])
    })

# ============================================================================
# SEED SCRIPT
# ============================================================================

async def create_tables():
    """Create all tables if they don't exist"""
    logger.info("Creating tables...")
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Tables created or already exist")


async def seed_skills(session: AsyncSession) -> dict[str, int]:
    """Seed skills table and return mapping of name -> id"""
    logger.info("Seeding skills...")
    
    skills_map = {}
    for skill_name, category in SKILLS_TO_CREATE.items():
        # Check if skill already exists
        result = await session.execute(
            select(Skill).where(Skill.name == skill_name)
        )
        existing = result.scalars().first()
        
        if not existing:
            skill = Skill(name=skill_name, category=category)
            session.add(skill)
        else:
            skills_map[skill_name] = existing.id
    
    await session.commit()
    
    # Fetch all skills to get IDs
    result = await session.execute(select(Skill))
    for skill in result.scalars().all():
        skills_map[skill.name] = skill.id
    
    logger.info("Skills seeded", count=len(skills_map))
    return skills_map


async def seed_jobs(session: AsyncSession, skills_map: dict[str, int]):
    """Seed jobs and job_skill relationships"""
    logger.info("Seeding jobs...")
    
    job_id_counter = 1000
    
    for job_data in JOBS_DATA:
        job_id = f"adzuna_{job_id_counter}"
        job_id_counter += 1
        
        salary_min = job_data["salary_min"]
        salary_max = job_data["salary_max"]
        salary_mid = (salary_min + salary_max) / 2
        
        # Determine seniority from title
        seniority = "mid"
        if "senior" in job_data["title"].lower() or "lead" in job_data["title"].lower():
            seniority = "senior"
        elif "junior" in job_data["title"].lower():
            seniority = "junior"
        
        # Check if job already exists
        existing = await session.execute(
            select(Job).where(Job.id == job_id)
        )
        if existing.scalars().first():
            continue
        
        job = Job(
            id=job_id,
            title=job_data["title"],
            company=job_data["company"],
            location_raw=job_data["location"],
            city=job_data["location"] if job_data["location"] != "Remote" else None,
            country="GB" if job_data["location"] in ["London", "Remote"] else
                   "DE" if job_data["location"] == "Berlin" else
                   "NL" if job_data["location"] == "Amsterdam" else
                   "US",
            salary_min=salary_min,
            salary_max=salary_max,
            salary_mid=salary_mid,
            description=f"Job posting for {job_data['title']} at {job_data['company']}. "
                       f"Required skills: {', '.join(job_data['skills'][:3])}. "
                       f"Preferred: {', '.join(job_data['skills'][3:])}.",
            seniority=seniority,
            remote=job_data.get("remote", False),
            fetched_at=datetime.utcnow(),
        )
        session.add(job)
        
        # Add job_skills
        for idx, skill_name in enumerate(job_data["skills"]):
            if skill_name in skills_map:
                is_required = idx < 3  # First 3 are required
                # Check if relationship already exists
                existing_skill = await session.execute(
                    select(JobSkill).where(
                        (JobSkill.job_id == job_id) & 
                        (JobSkill.skill_id == skills_map[skill_name])
                    )
                )
                if not existing_skill.scalars().first():
                    job_skill = JobSkill(
                        job_id=job_id,
                        skill_id=skills_map[skill_name],
                        is_required=is_required
                    )
                    session.add(job_skill)
    
    await session.commit()
    logger.info("Jobs seeded", count=len(JOBS_DATA))


async def seed_skill_snapshots(session: AsyncSession):
    """Seed 8 weeks of skill snapshots with realistic trending data"""
    logger.info("Seeding skill snapshots...")
    
    # Get all skills
    result = await session.execute(select(Skill))
    skills = result.scalars().all()
    
    # Get top 15 skills by job count
    top_skills_result = await session.execute(
        select(Skill.id, func.count(JobSkill.job_id).label("count"))
        .join(JobSkill)
        .group_by(Skill.id)
        .order_by(func.count(JobSkill.job_id).desc())
        .limit(15)
    )
    top_skill_ids = [row[0] for row in top_skills_result.fetchall()]
    
    # Create 8 weeks of snapshots
    base_date = datetime.utcnow().date()
    cities = ["London", "Berlin", "Amsterdam", "New York"]
    
    for week_offset in range(8):
        snapshot_date = base_date - timedelta(weeks=week_offset)
        
        for skill_id in top_skill_ids:
            for city in cities:
                # Simulate realistic trending
                base_count = random.randint(5, 50)
                # Some skills trend up, some flat, some down
                trend_factor = random.choice([1.15, 1.1, 1.05, 1.0, 0.95])
                job_count = max(5, int(base_count * (trend_factor ** week_offset)))
                
                avg_salary = random.randint(60000, 140000)
                tfidf_score = round(random.uniform(0.01, 0.95), 3)
                
                country = ("GB" if city == "London" else
                          "DE" if city == "Berlin" else
                          "NL" if city == "Amsterdam" else
                          "US")
                
                # Check if snapshot already exists
                existing = await session.execute(
                    select(SkillSnapshot).where(
                        (SkillSnapshot.skill_id == skill_id) &
                        (SkillSnapshot.snapshot_date == snapshot_date) &
                        (SkillSnapshot.city == city) &
                        (SkillSnapshot.country == country)
                    )
                )
                if existing.scalars().first():
                    continue
                
                snapshot = SkillSnapshot(
                    skill_id=skill_id,
                    snapshot_date=snapshot_date,
                    job_count=job_count,
                    avg_salary_mid=avg_salary,
                    tfidf_score=tfidf_score,
                    city=city,
                    country=country
                )
                session.add(snapshot)
    
    await session.commit()
    logger.info("Skill snapshots seeded", weeks=8, top_skills=len(top_skill_ids))


async def seed_pipeline_runs(session: AsyncSession):
    """Seed 3 pipeline run records"""
    logger.info("Seeding pipeline runs...")
    
    runs = [
        {
            "started_at": datetime.utcnow() - timedelta(days=7),
            "finished_at": datetime.utcnow() - timedelta(days=7, hours=1),
            "status": "success",
            "jobs_fetched": 245,
            "jobs_inserted": 240,
            "jobs_skipped": 5,
            "error_message": None,
        },
        {
            "started_at": datetime.utcnow() - timedelta(days=3),
            "finished_at": datetime.utcnow() - timedelta(days=3, hours=2),
            "status": "failed",
            "jobs_fetched": 180,
            "jobs_inserted": 150,
            "jobs_skipped": 30,
            "error_message": "API rate limit exceeded during fetch phase",
        },
        {
            "started_at": datetime.utcnow() - timedelta(hours=1),
            "finished_at": None,
            "status": "running",
            "jobs_fetched": 0,
            "jobs_inserted": 0,
            "jobs_skipped": 0,
            "error_message": None,
        },
    ]
    
    for run_data in runs:
        stmt = insert(PipelineRun).values(**run_data)
        await session.execute(stmt)
    
    await session.commit()
    logger.info("Pipeline runs seeded", count=len(runs))


async def verify_seed(session: AsyncSession):
    """Verify that all data was seeded correctly"""
    logger.info("Verifying seed data...")
    
    # Count jobs
    jobs_count = await session.scalar(select(func.count(Job.id)))
    logger.info(f"✅ Jobs: {jobs_count}")
    
    # Count skills
    skills_count = await session.scalar(select(func.count(Skill.id)))
    logger.info(f"✅ Skills: {skills_count}")
    
    # Count job_skills
    job_skills_count = await session.scalar(select(func.count(JobSkill.skill_id)))
    logger.info(f"✅ Job-Skill relationships: {job_skills_count}")
    
    # Count snapshots
    snapshots_count = await session.scalar(select(func.count(SkillSnapshot.id)))
    logger.info(f"✅ Skill snapshots: {snapshots_count}")
    
    # Count pipeline runs
    runs_count = await session.scalar(select(func.count(PipelineRun.id)))
    logger.info(f"✅ Pipeline runs: {runs_count}")
    
    # Show sample data
    sample_jobs = await session.execute(select(Job).limit(3))
    logger.info("Sample jobs:", count=3)
    for job in sample_jobs.scalars():
        logger.info(f"  - {job.title} @ {job.company} ({job.city}, {job.country})")
    
    logger.info("✅ Seed verification complete!")


async def main():
    """Main seeding function"""
    print("\n" + "="*70)
    print("PHASE 1: SEEDING DATABASE")
    print("="*70 + "\n")
    
    async with async_session_maker() as session:
        try:
            await create_tables()
            await seed_skills(session)
            await seed_jobs(session, await seed_skills(session))
            await seed_skill_snapshots(session)
            await seed_pipeline_runs(session)
            await verify_seed(session)
            
            print("\n" + "="*70)
            print("✅ DATABASE SEEDING COMPLETE!")
            print("="*70)
            print("\n✓ All data inserted successfully")
            print("✓ FastAPI routes should now return populated data")
            print("✓ Ready to move to PHASE 2: Build Adzuna Pipeline\n")
            
        except Exception as e:
            logger.error("Seed failed", error=str(e), exc_info=True)
            raise


if __name__ == "__main__":
    asyncio.run(main())
