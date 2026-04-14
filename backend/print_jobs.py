import asyncio
from src.db.session import async_session_maker
from src.db.models import Job
from sqlalchemy import select

async def test():
    async with async_session_maker() as db:
        res = await db.execute(select(Job.title, Job.description).limit(5))
        for job in res:
            print("---")
            print("Title:", job.title)
            print("Description:", job.description)

asyncio.run(test())
