#!/usr/bin/env python
"""
Simple ETL test script - Run as: python test_etl.py
(Avoids Prefect module loading issues)
"""

import asyncio
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv(Path(__file__).parent / ".env")

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

from src.api.routes.pipeline_trigger import fetch_adzuna_jobs, clean_jobs_data, save_jobs_to_db
from src.db.session import async_session_maker
from sqlalchemy.ext.asyncio import AsyncSession

async def run_etl_test():
    """Simple ETL test - fetch, clean, save jobs"""
    
    print("\n" + "="*60)
    print("ETL PIPELINE TEST")
    print("="*60)
    
    try:
        # Step 1: Fetch
        print("\n📥 Step 1: Fetching jobs from Adzuna API...")
        raw_jobs = await fetch_adzuna_jobs(pages=2)
        print(f"✅ Fetched {len(raw_jobs)} jobs")
        
        if not raw_jobs:
            print("❌ No jobs fetched")
            return
        
        # Step 2: Clean
        print("\n🧹 Step 2: Cleaning job data...")
        cleaned_jobs = clean_jobs_data(raw_jobs)
        print(f"✅ Cleaned {len(cleaned_jobs)} jobs")
        
        # Step 3: Save
        print("\n💾 Step 3: Saving to database...")
        async with async_session_maker() as db:
            stats = await save_jobs_to_db(db, cleaned_jobs)
        
        print(f"✅ Database save completed:")
        print(f"   - Jobs inserted: {stats['jobs_inserted']}")
        print(f"   - Duplicates skipped: {stats['duplicates_skipped']}")
        print(f"   - Skills extracted: {stats['skills_extracted']}")
        print(f"   - Errors: {stats['errors']}")
        
        print("\n" + "="*60)
        print("✅ ETL TEST COMPLETED SUCCESSFULLY!")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n❌ ETL Test Failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(run_etl_test())
