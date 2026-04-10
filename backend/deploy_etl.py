"""
Prefect Deployment - Schedule ETL flow to run every 6 hours
Deploy with: python deploy_etl.py
"""

import asyncio
from datetime import datetime, timedelta
from src.flows.etl_flow import etl_pipeline
from prefect.deployments import Deployment
from prefect.schedules import CronSchedule

# Create deployment with schedule
deployment = Deployment.build(
    flow=etl_pipeline,
    name="adzuna-etl-scheduled",
    description="Automated ETL pipeline - fetches Adzuna jobs every 6 hours",
    schedule=CronSchedule(
        cron="0 */6 * * *",  # Every 6 hours: 00:00, 06:00, 12:00, 18:00
        timezone="UTC"
    ),
    parameters={
        "pages": 5  # Fetch 5 pages each run
    },
)

if __name__ == "__main__":
    print("Deploying ETL Pipeline to Prefect...")
    deployment.deploy(work_pool_name="default-agent-pool")
    print("✅ ETL Pipeline deployed!")
    print("\nTo start the Prefect agent, run:")
    print("  prefect agent start --pool 'default-agent-pool'")
    print("\nTo monitor flows, visit:")
    print("  http://localhost:4200")
