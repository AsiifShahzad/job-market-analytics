import asyncio
from src.db.session import async_session_maker
from src.db.models import Job
from sqlalchemy import select

async def test():
    async with async_session_maker() as db:
        res = await db.execute(select(Job.title, Job.description).limit(1))
        job = res.first()
        if job:
            from src.nlp.skill_extractor import extract_skills
            print("Title:", job.title)
            print("Description len:", len(job.description))
            ext = extract_skills(job.title, job.description)
            print("Skills extracted:", ext.all_skills)
        else:
            print("No jobs found")

asyncio.run(test())
