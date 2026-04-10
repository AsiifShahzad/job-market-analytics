"""
Quick test of Phase 2 Pipeline Components
Tests Adzuna client → Skill extraction → Pipeline flow
"""

import asyncio
import os
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

# Load .env
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

import structlog

logger = structlog.get_logger(__name__)


async def test_adzuna_client():
    """Test Adzuna API client"""
    print("\n" + "="*60)
    print("TEST 1: Adzuna API Client")
    print("="*60)
    
    try:
        from src.ingestion.adzuna_client import AdzunaClient
        
        client = AdzunaClient()
        print("[OK] Adzuna client initialized successfully")
        
        # Test search for one page
        result = await client.search_jobs(
            country="gb",
            keywords="developer",
            location="London",
            page=1,
            per_page=5,
        )
        
        print(f"[OK] Fetched {len(result.get('results', []))} jobs from Adzuna")
        
        if result.get('results'):
            job = result['results'][0]
            print(f"   Sample job: {job.get('title')} at {job.get('company', {}).get('display_name')}")
        
        return True
    except Exception as e:
        print(f"[FAIL] Adzuna client test failed: {e}")
        return False


async def test_skill_extractor():
    """Test skill extraction"""
    print("\n" + "="*60)
    print("TEST 2: Skill Extractor")
    print("="*60)
    
    try:
        from src.nlp.skill_extractor import extract_skills
        
        test_title = "Senior Python Developer - London"
        test_description = """
        We're looking for a Senior Python Developer with experience in:
        - Python and FastAPI
        - React for frontend
        - PostgreSQL and Redis
        - AWS and Docker
        - Kubernetes for orchestration
        
        Required Skills:
        - 5+ years Python experience
        - FastAPI framework knowledge
        - SQL expertise
        
        Preferred:
        - Experience with Apache Airflow
        - Machine Learning with TensorFlow
        - Knowledge of Kafka streaming
        """
        
        result = extract_skills(title=test_title, description=test_description)
        
        print("[OK] Skill extraction successful")
        print(f"   Required skills: {result.required_skills[:3]}")
        print(f"   All skills found: {len(result.all_skills)}")
        print(f"   Seniority: {result.seniority_level}")
        print(f"   Remote: {result.is_remote}")
        
        return True
    except Exception as e:
        print(f"[FAIL] Skill extractor test failed: {e}")
        return False


async def test_skill_scorer():
    """Test TF-IDF skill scorer"""
    print("\n" + "="*60)
    print("TEST 3: Skill Scorer (TF-IDF)")
    print("="*60)
    
    try:
        from src.nlp.skill_scorer import SkillScorer
        
        # Create sample job descriptions
        descriptions = [
            "Python developer with Django and PostgreSQL",
            "Python expert in FastAPI and MySQL",
            "Senior Python architect with Flask",
            "JavaScript developer with React and Node.js",
            "TypeScript specialist in Angular",
        ]
        
        scorer = SkillScorer()
        scorer.add_documents(descriptions)
        scorer.fit()
        
        scores = scorer.get_skill_importance_scores()
        top_10 = scorer.get_top_skills(k=10)
        
        print(f"[OK] TF-IDF scoring successful")
        print(f"   Total skills scored: {len(scores)}")
        print(f"   Top 5 skills:")
        for skill, score in top_10[:5]:
            print(f"      - {skill}: {score:.4f}")
        
        return True
    except Exception as e:
        print(f"[FAIL] Skill scorer test failed: {e}")
        return False


async def test_pipeline_with_db():
    """Test full pipeline with database"""
    print("\n" + "="*60)
    print("TEST 4: Full Pipeline with Database")
    print("="*60)
    
    try:
        from src.db.session import async_session_maker
        from src.flows.pipeline import run_full_pipeline
        
        print("Starting pipeline execution...")
        
        async with async_session_maker() as session:
            result = await run_full_pipeline(
                db=session,
                countries=["gb"],  # Only GB for faster test
                keywords_list=["python"],  # Single keyword for faster test
            )
        
        print(f"\n[OK] Pipeline execution successful!")
        print(f"   Run ID: {result.get('run_id')}")
        print(f"   Status: {result.get('status')}")
        print(f"   Jobs fetched: {result.get('jobs_fetched')}")
        print(f"   Jobs inserted: {result.get('jobs_inserted')}")
        print(f"   Job skills created: {result.get('job_skills_created')}")
        print(f"   Skill snapshots created: {result.get('skill_snapshots_created')}")
        print(f"   Duration: {result.get('duration_seconds'):.2f}s")
        
        return True
    except Exception as e:
        print(f"[FAIL] Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("PHASE 2 - ADZUNA PIPELINE TEST SUITE")
    print("="*70)
    
    tests = [
        ("Adzuna Client", test_adzuna_client),
        ("Skill Extractor", test_skill_extractor),
        ("Skill Scorer", test_skill_scorer),
        ("Full Pipeline", test_pipeline_with_db),
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = await test_func()
        except Exception as e:
            print(f"\n❌ {test_name} crashed: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    for test_name, passed in results.items():
        status = "[PASS]" if passed else "[FAIL]"
        print(f"{test_name:.<50} {status}")
    
    total = len(results)
    passed = sum(results.values())
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n>> ALL TESTS PASSED - PHASE 2 IS READY!")
    else:
        print(f"\n>> {total - passed} test(s) failed - check logs above")


if __name__ == "__main__":
    asyncio.run(main())
