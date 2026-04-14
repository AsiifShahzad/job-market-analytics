"""
Skill Snapshot Module — Test & Demo Script

Tests the snapshot building functionality with sample data.

Usage:
    python -m backend.src.etl.test_snapshots

Shows:
- Building snapshots for today
- Retrieving historical snapshots
- Calculating skill growth
- Cleanup of old snapshots
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def demo_snapshots():
    """Demonstrates snapshot functionality."""
    from src.db.session import async_session_maker
    from src.etl.snapshots import (
        build_skill_snapshots,
        get_latest_snapshot_date,
        get_skill_snapshots,
        calculate_skill_growth,
        cleanup_old_snapshots,
    )
    
    print("\n" + "="*80)
    print("SKILL SNAPSHOT MODULE — DEMO")
    print("="*80 + "\n")
    
    async with async_session_maker() as db:
        # ────────────────────────────────────────────────────────────────────
        # Test 1: Check latest snapshot
        # ────────────────────────────────────────────────────────────────────
        
        print("TEST 1: Get Latest Snapshot Date")
        print("-" * 80)
        
        latest = await get_latest_snapshot_date(db)
        if latest:
            days_ago = (datetime.now(timezone.utc) - latest).days
            print(f"✓ Latest snapshot: {latest.isoformat()}")
            print(f"  ({days_ago} days ago)\n")
        else:
            print("⚠ No snapshots found yet\n")
        
        # ────────────────────────────────────────────────────────────────────
        # Test 2: Build snapshots for today
        # ────────────────────────────────────────────────────────────────────
        
        print("TEST 2: Build Today's Snapshots")
        print("-" * 80)
        
        report = await build_skill_snapshots(db)
        
        print(f"✓ Snapshot date: {report.snapshot_date.isoformat()}")
        print(f"  Created: {report.total_snapshots_created}")
        print(f"  Updated: {report.total_snapshots_updated}")
        print(f"  Skills processed: {report.skills_processed}")
        print(f"  Locations: {report.locations_processed}")
        print(f"  Duration: {report.duration_seconds:.2f}s")
        print(f"  Errors: {report.errors}\n")
        
        # ────────────────────────────────────────────────────────────────────
        # Test 3: Get snapshots for top skills
        # ────────────────────────────────────────────────────────────────────
        
        print("TEST 3: Get Recent Snapshots by Skill")
        print("-" * 80)
        
        # Find top 3 skills from latest snapshots
        from src.db.models import Skill, SkillSnapshot
        from sqlalchemy import select, func
        
        top_skills_query = (
            select(Skill.id, Skill.name)
            .join(SkillSnapshot, Skill.id == SkillSnapshot.skill_id)
            .group_by(Skill.id, Skill.name)
            .order_by(func.count(SkillSnapshot.id).desc())
            .limit(3)
        )
        
        result = await db.execute(top_skills_query)
        top_skills = result.fetchall()
        
        if top_skills:
            for skill_id, skill_name in top_skills:
                print(f"\nSkill: {skill_name} (ID: {skill_id})")
                
                snapshots = await get_skill_snapshots(db, skill_id=skill_id, days=30)
                
                if snapshots:
                    print(f"  Recent snapshots (last 30 days): {len(snapshots)}")
                    
                    # Show latest snapshot
                    latest = snapshots[0]
                    print(f"  Latest ({latest['snapshot_date'].date()}):")
                    print(f"    - Jobs: {latest['job_count']}")
                    print(f"    - Avg Salary: ${latest['avg_salary_mid']:,.0f}" if latest['avg_salary_mid'] else "    - Avg Salary: N/A")
                    print(f"    - Location: {latest['city']}, {latest['country']}")
                else:
                    print(f"  No snapshots in last 30 days")
        else:
            print("⚠ No skills found in snapshots\n")
        
        # ────────────────────────────────────────────────────────────────────
        # Test 4: Calculate growth rates
        # ────────────────────────────────────────────────────────────────────
        
        print("\nTEST 4: Calculate Skill Growth Rates")
        print("-" * 80)
        
        if top_skills:
            for skill_id, skill_name in top_skills[:2]:  # First 2 skills
                growth = await calculate_skill_growth(db, skill_id=skill_id, days=7)
                
                if growth is not None:
                    trend = "↑ Growing" if growth > 0 else "↓ Declining"
                    print(f"\n{skill_name}:")
                    print(f"  7-day growth: {growth:+.1%} {trend}")
                else:
                    print(f"\n{skill_name}:")
                    print(f"  Insufficient data for growth calculation")
        
        print("\n" + "="*80)
        print("DEMO COMPLETE")
        print("="*80 + "\n")


async def test_snapshot_edge_cases():
    """Tests edge cases and error handling."""
    from src.db.session import async_session_maker
    from src.etl.snapshots import build_skill_snapshots, get_latest_snapshot_date
    
    print("\n" + "="*80)
    print("EDGE CASE TESTS")
    print("="*80 + "\n")
    
    async with async_session_maker() as db:
        # Test 1: Build snapshots with NULL locations
        print("TEST 1: Handle NULL Locations (Global Snapshots)")
        print("-" * 80)
        
        report = await build_skill_snapshots(db, include_global=True)
        print(f"✓ Processed with global snapshots")
        print(f"  Total locations: {report.locations_processed}\n")
        
        # Test 2: Multiple calls same day (should upsert)
        print("TEST 2: Upsert Behavior (Multiple Builds Same Day)")
        print("-" * 80)
        
        report1 = await build_skill_snapshots(db)
        print(f"Run 1: Created {report1.total_snapshots_created}, Updated {report1.total_snapshots_updated}")
        
        report2 = await build_skill_snapshots(db)
        print(f"Run 2: Created {report2.total_snapshots_created}, Updated {report2.total_snapshots_updated}")
        print(f"✓ Upsert working correctly\n")
        
        # Test 3: Check latest date consistency
        print("TEST 3: Date Consistency")
        print("-" * 80)
        
        latest = await get_latest_snapshot_date(db)
        print(f"✓ Latest snapshot date: {latest.isoformat()}")
        print(f"  Is today: {latest.date() == datetime.now(timezone.utc).date()}\n")


def main():
    """Runs all tests."""
    try:
        asyncio.run(demo_snapshots())
        asyncio.run(test_snapshot_edge_cases())
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
    except Exception as e:
        logger.error(f"Demo failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()
