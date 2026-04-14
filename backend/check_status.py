import asyncio
from src.db.session import async_session_maker
from src.db.models import Job, PipelineRun, Skill
from sqlalchemy import select, func

async def check():
    async with async_session_maker() as db:
        jobs = (await db.execute(select(func.count(Job.id)))).scalar()
        run = (await db.execute(select(PipelineRun).order_by(PipelineRun.id.desc()).limit(1))).scalar()
        skills = (await db.execute(select(func.count(Skill.id)))).scalar()
        if run:
            print(f"Latest pipeline run {run.id} status: {run.status}")
            print(f"Jobs inserted this run: {run.jobs_inserted}")
            print(f"Error (if any): {run.error_message}")
        print(f"Total jobs in DB: {jobs}")
        print(f"Total skills in DB: {skills}")

asyncio.run(check())
