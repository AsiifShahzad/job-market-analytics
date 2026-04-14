"""
Seed database with realistic fake data for local development.
Run once: python -m src.db.seed

Uses a single session for the whole operation — no double calls.
"""

import asyncio
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from src.db.session import async_session_maker, async_engine
from src.db.models import Base, Job, Skill, JobSkill, SkillSnapshot, PipelineRun

logger = structlog.get_logger(__name__)

# ── Seed data ──────────────────────────────────────────────────────────────────

SKILLS = {
    "Python": "language", "JavaScript": "language", "TypeScript": "language",
    "Java": "language", "Go": "language", "Rust": "language", "SQL": "language",
    "Bash": "language", "C++": "language", "Scala": "language",
    "React": "framework", "Vue": "framework", "Angular": "framework",
    "FastAPI": "framework", "Django": "framework", "Flask": "framework",
    "Spring": "framework", "NestJS": "framework", "Express": "framework",
    "Node.js": "framework", "Next.js": "framework", "GraphQL": "framework",
    "AWS": "cloud", "Google Cloud": "cloud", "Azure": "cloud",
    "Spark": "data", "Kafka": "data", "Pandas": "data", "NumPy": "data",
    "TensorFlow": "data", "PyTorch": "data", "dbt": "data", "Airflow": "data",
    "PostgreSQL": "data", "MongoDB": "data", "Redis": "data",
    "Docker": "tool", "Kubernetes": "tool", "Git": "tool", "Terraform": "tool",
    "GitHub Actions": "tool", "Jenkins": "tool", "Linux": "tool",
    "Ansible": "tool", "Prometheus": "tool", "Grafana": "tool",
    "Machine Learning": "data", "LLM": "data", "NLP": "data",
    "Agile": "soft", "Communication": "soft", "Leadership": "soft",
}

JOBS_TEMPLATE = [
    {"title": "Senior Python Developer",      "company": "TechCorp",       "city": "New York",    "country": "US", "sal": (110000, 160000), "skills": ["Python", "FastAPI", "Docker", "AWS", "PostgreSQL"]},
    {"title": "React Frontend Engineer",       "company": "Digital Co",     "city": "London",      "country": "GB", "sal": (70000,  110000), "skills": ["React", "TypeScript", "JavaScript", "Node.js", "GraphQL"]},
    {"title": "Data Engineer",                 "company": "Analytics Inc",  "city": "Berlin",      "country": "DE", "sal": (75000,  115000), "skills": ["Python", "Spark", "Airflow", "SQL", "AWS"]},
    {"title": "DevOps Engineer",               "company": "CloudNine",      "city": "Amsterdam",   "country": "NL", "sal": (80000,  130000), "skills": ["Docker", "Kubernetes", "Terraform", "Linux", "AWS"]},
    {"title": "ML Engineer",                   "company": "AI Labs",        "city": "San Francisco","country": "US","sal": (130000, 190000), "skills": ["Python", "TensorFlow", "PyTorch", "Machine Learning", "AWS"]},
    {"title": "Backend Engineer",              "company": "Platform Ltd",   "city": "London",      "country": "GB", "sal": (78000,  118000), "skills": ["Java", "Spring", "SQL", "Docker", "Kubernetes"]},
    {"title": "Full Stack Developer",          "company": "StartupHub",     "city": "Remote",      "country": "US", "sal": (90000,  140000), "skills": ["React", "Node.js", "Python", "SQL", "Docker"]},
    {"title": "Data Scientist",                "company": "DataLab",        "city": "New York",    "country": "US", "sal": (115000, 165000), "skills": ["Python", "Pandas", "TensorFlow", "SQL", "Machine Learning"]},
    {"title": "Platform Engineer",             "company": "InfraTeam",      "city": "Berlin",      "country": "DE", "sal": (85000,  125000), "skills": ["Kubernetes", "Docker", "Go", "Terraform", "Linux"]},
    {"title": "Senior Frontend Developer",     "company": "UXCo",           "city": "Amsterdam",   "country": "NL", "sal": (72000,  108000), "skills": ["React", "TypeScript", "Vue", "JavaScript", "GraphQL"]},
    {"title": "Analytics Engineer",            "company": "Warehouse Inc",  "city": "Remote",      "country": "US", "sal": (100000, 150000), "skills": ["SQL", "Python", "dbt", "Spark", "AWS"]},
    {"title": "NLP Engineer",                  "company": "LangAI",         "city": "San Francisco","country": "US","sal": (140000, 200000), "skills": ["Python", "NLP", "LLM", "PyTorch", "Machine Learning"]},
    {"title": "Site Reliability Engineer",     "company": "ReliableCo",     "city": "London",      "country": "GB", "sal": (90000,  135000), "skills": ["Linux", "Kubernetes", "Prometheus", "Grafana", "Python"]},
    {"title": "Go Developer",                  "company": "GopherSoft",     "city": "Berlin",      "country": "DE", "sal": (80000,  120000), "skills": ["Go", "Docker", "SQL", "Git", "Linux"]},
    {"title": "Cloud Architect",               "company": "CloudArch",      "city": "New York",    "country": "US", "sal": (150000, 210000), "skills": ["AWS", "Terraform", "Kubernetes", "Azure", "Docker"]},
]


async def create_tables():
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("tables ready")


async def seed_all():
    async with async_session_maker() as session:
        # ── skills ─────────────────────────────────────────────────────────
        skill_map: dict[str, int] = {}
        for name, cat in SKILLS.items():
            existing = (await session.execute(
                select(Skill).where(Skill.name == name)
            )).scalar_one_or_none()
            if not existing:
                s = Skill(name=name, category=cat)
                session.add(s)
                await session.flush()
                skill_map[name] = s.id
            else:
                skill_map[name] = existing.id
        await session.commit()

        # ── jobs ───────────────────────────────────────────────────────────
        now = datetime.now(timezone.utc)
        for i, tmpl in enumerate(JOBS_TEMPLATE * 4):   # ~60 jobs
            job_id = f"seed_{i:04d}"
            if (await session.execute(select(Job.id).where(Job.id == job_id))).scalar_one_or_none():
                continue

            sal_min, sal_max = tmpl["sal"]
            sal_mid = (sal_min + sal_max) / 2
            remote = tmpl["city"] == "Remote"
            city = None if remote else tmpl["city"]

            j = Job(
                id=job_id,
                title=tmpl["title"],
                company=f"{tmpl['company']} {i // len(JOBS_TEMPLATE) + 1}",
                location_raw=tmpl["city"],
                city=city,
                country=tmpl["country"],
                description=(
                    f"We are looking for a {tmpl['title']} with expertise in "
                    + ", ".join(tmpl["skills"][:3])
                    + ". Other preferred skills: "
                    + ", ".join(tmpl["skills"][3:]) + "."
                ),
                salary_min=sal_min + random.randint(-5000, 5000),
                salary_max=sal_max + random.randint(-5000, 5000),
                salary_mid=sal_mid + random.randint(-5000, 5000),
                remote=remote,
                source="seed",
                fetched_at=now,
                posted_at=now - timedelta(days=random.randint(0, 30)),
            )
            session.add(j)
            await session.flush()

            for idx, sk in enumerate(tmpl["skills"]):
                if sk in skill_map:
                    session.add(JobSkill(
                        job_id=job_id,
                        skill_id=skill_map[sk],
                        is_required=idx < 3,
                    ))

        await session.commit()
        logger.info("jobs seeded")

        # ── skill snapshots (8 weeks) ──────────────────────────────────────
        top_skill_ids = [
            r[0] for r in (await session.execute(
                select(Skill.id, func.count(JobSkill.job_id))
                .join(JobSkill, Skill.id == JobSkill.skill_id)
                .group_by(Skill.id)
                .order_by(func.count(JobSkill.job_id).desc())
                .limit(15)
            )).fetchall()
        ]

        cities = [("London", "GB"), ("Berlin", "DE"), ("Amsterdam", "NL"), ("New York", "US")]
        today = now.date()

        for week in range(8):
            snap_date = datetime(
                *(today - timedelta(weeks=week)).timetuple()[:3],
                tzinfo=timezone.utc,
            )
            for skill_id in top_skill_ids:
                for city, country in cities:
                    from sqlalchemy import and_
                    from src.db.models import SkillSnapshot
                    exists = (await session.execute(
                        select(SkillSnapshot).where(
                            and_(
                                SkillSnapshot.skill_id == skill_id,
                                SkillSnapshot.snapshot_date == snap_date,
                                SkillSnapshot.city == city,
                            )
                        )
                    )).scalar_one_or_none()
                    if exists:
                        continue
                    trend = random.choice([1.15, 1.1, 1.05, 1.0, 0.95, 0.9])
                    base = random.randint(8, 60)
                    session.add(SkillSnapshot(
                        skill_id=skill_id,
                        snapshot_date=snap_date,
                        job_count=max(5, int(base * trend ** week)),
                        avg_salary_mid=random.randint(70000, 160000),
                        city=city,
                        country=country,
                    ))

        await session.commit()
        logger.info("snapshots seeded")

        # ── pipeline runs ──────────────────────────────────────────────────
        for days_ago, status, inserted, fetched in [
            (7, "success", 240, 250),
            (3, "failed",  150, 180),
            (1, "success", 195, 200),
        ]:
            started = now - timedelta(days=days_ago)
            finished = started + timedelta(minutes=random.randint(3, 8))
            session.add(PipelineRun(
                started_at=started,
                finished_at=finished if status != "running" else None,
                status=status,
                jobs_fetched=fetched,
                jobs_inserted=inserted,
                jobs_skipped=fetched - inserted,
                unique_skills=len(skill_map),
                error_message="API rate limit" if status == "failed" else None,
            ))

        await session.commit()
        logger.info("pipeline runs seeded")

        # ── verify ─────────────────────────────────────────────────────────
        jobs_n = (await session.execute(select(func.count(Job.id)))).scalar()
        skills_n = (await session.execute(select(func.count(Skill.id)))).scalar()
        snaps_n = (await session.execute(
            select(func.count(SkillSnapshot.id))
        )).scalar()

        print(f"\n{'='*50}")
        print("SEED COMPLETE")
        print(f"  Jobs:       {jobs_n}")
        print(f"  Skills:     {skills_n}")
        print(f"  Snapshots:  {snaps_n}")
        print(f"{'='*50}\n")


async def main():
    await create_tables()
    await seed_all()


if __name__ == "__main__":
    asyncio.run(main())