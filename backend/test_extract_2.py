import asyncio
from src.db.session import async_session_maker
from src.db.models import Job
from sqlalchemy import select
from src.nlp.skill_extractor import extract_skills

async def test():
    async with async_session_maker() as db:
        res = await db.execute(select(Job.title, Job.description))
        jobs = res.fetchall()
        
        skills_found = 0
        names_found = set()
        
        for job in jobs:
            title, description = job.title, job.description
            ext = extract_skills(title, description)
            if ext.skill_count > 0:
                skills_found += ext.skill_count
                names_found.update(ext.all_skills)
                
        print(f"Out of {len(jobs)} jobs, found {skills_found} total skills.")
        print("Unique skills found:", list(names_found))

asyncio.run(test())
