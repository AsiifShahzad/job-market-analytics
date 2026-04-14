import asyncio
from src.db.session import async_session_maker
from src.db.models import Job, Skill, JobSkill
from sqlalchemy import select, func

async def test():
    async with async_session_maker() as db:
        skills = 'react'
        parsed_skills = [s.strip().lower() for s in skills.split(',')]
        skill_subq = (
            select(JobSkill.job_id)
            .join(Skill, JobSkill.skill_id == Skill.id)
            .where(func.lower(Skill.name).in_(parsed_skills))
        )
        
        q = select(Job).where(Job.id.in_(skill_subq))
        res = (await db.execute(q)).scalars().all()
        print('Jobs found:', len(res))

asyncio.run(test())
