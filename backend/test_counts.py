import asyncio
from sqlalchemy import select, func
from src.db.session import async_session_maker
from src.db.models import Job, Skill, JobSkill, SkillSnapshot

async def test():
    async with async_session_maker() as db:
        jobs = (await db.execute(select(func.count(Job.id)))).scalar()
        skills = (await db.execute(select(func.count(Skill.id)))).scalar()
        job_skills = (await db.execute(select(func.count(JobSkill.job_id)))).scalar()
        snapshots = (await db.execute(select(func.count(SkillSnapshot.id)))).scalar()
        print(f"Jobs: {jobs}")
        print(f"Skills: {skills}")
        print(f"JobSkills: {job_skills}")
        print(f"Snapshots: {snapshots}")

asyncio.run(test())
