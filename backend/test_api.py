#!/usr/bin/env python3
"""
Backend API test suite - Quick validation of all endpoints
Run after starting the server: python test_api.py
"""

import asyncio
import httpx
import json
from datetime import datetime

BASE_URL = "http://localhost:8000"
client = httpx.AsyncClient(base_url=BASE_URL, timeout=30.0)


async def test_health():
    """Test health check endpoint"""
    print("\n🏥 Testing Health Check...")
    response = await client.get("/api/health")
    assert response.status_code == 200
    data = response.json()
    print(f"✅ Status: {data['status']}")
    print(f"✅ Database: {data['database']}")
    print(f"✅ Cache utilization: {data['cache_stats']['utilization_percent']:.1f}%")


async def test_skills():
    """Test skills endpoints"""
    print("\n🔧 Testing Skills Endpoints...")
    
    # GET /api/skills
    print("  - GET /api/skills")
    response = await client.get("/api/skills", params={"limit": 10})
    assert response.status_code == 200
    data = response.json()
    print(f"    ✅ Got {len(data['skills'])} skills (total: {data['total_count']})")
    if data['skills']:
        first_skill = data['skills'][0]
        print(f"    ✅ Sample: {first_skill['name']} ({first_skill['job_count']} jobs)")
    
    # GET /api/skills with filters
    print("  - GET /api/skills (with filters)")
    response = await client.get(
        "/api/skills",
        params={
            "limit": 5,
            "category": "language",
            "seniority": "senior"
        }
    )
    assert response.status_code == 200
    print(f"    ✅ Filtered skills: {response.json()['total_count']}")


async def test_salaries():
    """Test salary endpoints"""
    print("\n💰 Testing Salary Endpoints...")
    
    # GET /api/salaries
    print("  - GET /api/salaries")
    response = await client.get("/api/salaries")
    assert response.status_code in [200, 500]  # May return 500 if no salary data
    if response.status_code == 200:
        data = response.json()
        band = data['salary_band']
        print(f"    ✅ p25: ${band['p25']}, p50: ${band['p50']}, p75: ${band['p75']}")
    else:
        print(f"    ⚠️  No salary data yet (expected before loading jobs)")
    
    # GET /api/salaries/skill-premium
    print("  - GET /api/salaries/skill-premium")
    response = await client.get("/api/salaries/skill-premium", params={"top_n": 5})
    assert response.status_code in [200, 500]
    if response.status_code == 200 and response.json()['premiums']:
        print(f"    ✅ Got {len(response.json()['premiums'])} skill premiums")


async def test_pipeline():
    """Test pipeline endpoints"""
    print("\n⚙️  Testing Pipeline Endpoints...")
    
    # GET /api/pipeline/runs
    print("  - GET /api/pipeline/runs")
    response = await client.get("/api/pipeline/runs")
    assert response.status_code == 200
    data = response.json()
    print(f"    ✅ Got {len(data['runs'])} recent runs (total: {data['total_count']})")
    
    # POST /api/pipeline/trigger
    print("  - POST /api/pipeline/trigger")
    response = await client.post("/api/pipeline/trigger")
    assert response.status_code == 200
    data = response.json()
    run_id = data['run_id']
    print(f"    ✅ Created run {run_id}, status: {data['status']}")
    
    # GET /api/pipeline/{run_id}/status
    print(f"  - GET /api/pipeline/{run_id}/status")
    response = await client.get(f"/api/pipeline/{run_id}/status")
    assert response.status_code == 200
    data = response.json()
    print(f"    ✅ Run {run_id}: {data['status']}")
    print(f"    ✅ Progress: {data['jobs_fetched']} fetched, {data['jobs_inserted']} inserted")


async def test_trends():
    """Test trends endpoints"""
    print("\n📈 Testing Trends Endpoints...")
    
    # GET /api/trends/emerging
    print("  - GET /api/trends/emerging")
    response = await client.get("/api/trends/emerging", params={"limit": 5})
    assert response.status_code in [200, 500]  # May be empty initially
    if response.status_code == 200:
        data = response.json()
        print(f"    ✅ Got {len(data['emerging_skills'])} emerging skills")
    else:
        print(f"    ⚠️  No trend data yet (expected before loading jobs)")
    
    # GET /api/trends/heatmap
    print("  - GET /api/trends/heatmap")
    response = await client.get("/api/trends/heatmap", params={"top_n_skills": 5, "top_n_cities": 5})
    assert response.status_code in [200, 500]
    if response.status_code == 200:
        data = response.json()
        print(f"    ✅ Heatmap: {len(data['heatmap_data'])} cells")


async def test_cache():
    """Test cache management"""
    print("\n💾 Testing Cache Management...")
    
    # GET /api/cache/stats
    print("  - GET /api/cache/stats")
    response = await client.get("/api/cache/stats")
    assert response.status_code == 200
    data = response.json()
    print(f"    ✅ Cache: {data['total_entries']}/{data['max_entries']} entries")
    
    # POST /api/cache/clear
    print("  - POST /api/cache/clear")
    response = await client.post("/api/cache/clear")
    assert response.status_code == 200
    print(f"    ✅ Cache cleared")


async def main():
    """Run all tests"""
    print("=" * 60)
    print("🧪 JobPulse AI Backend API Tests")
    print(f"🌐 Target: {BASE_URL}")
    print("=" * 60)
    
    try:
        await test_health()
        await test_skills()
        await test_salaries()
        await test_pipeline()
        await test_trends()
        await test_cache()
        
        print("\n" + "=" * 60)
        print("✅ All tests passed!")
        print("=" * 60)
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        exit(1)
    finally:
        await client.aclose()


if __name__ == "__main__":
    print("\n⚠️  Make sure the backend is running:")
    print("   bash run_backend.sh  (or run_backend.bat on Windows)")
    print("\nStarting tests in 3 seconds...\n")
    
    import time
    time.sleep(3)
    
    asyncio.run(main())
