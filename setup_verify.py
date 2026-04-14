#!/usr/bin/env python3
"""
Setup & Verification Script for Rigorous Analytics Engine
Checks dependencies, database connectivity, and runs basic tests
"""

import sys
import asyncio
import subprocess
from pathlib import Path
from typing import List, Tuple

# Color codes
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

def print_header(text: str):
    """Print section header"""
    print(f"\n{BLUE}{'='*70}{RESET}")
    print(f"{BLUE}{text.center(70)}{RESET}")
    print(f"{BLUE}{'='*70}{RESET}\n")

def print_success(text: str):
    """Print success message"""
    print(f"{GREEN}✅ {text}{RESET}")

def print_error(text: str):
    """Print error message"""
    print(f"{RED}❌ {text}{RESET}")

def print_warning(text: str):
    """Print warning message"""
    print(f"{YELLOW}⚠️  {text}{RESET}")

def print_info(text: str):
    """Print info message"""
    print(f"{BLUE}ℹ️  {text}{RESET}")

# ══════════════════════════════════════════════════════════════════════════════
# CHECKS
# ══════════════════════════════════════════════════════════════════════════════

def check_python_version() -> bool:
    """Check Python version >= 3.9"""
    print_header("Python Version Check")
    
    version = sys.version_info
    min_version = (3, 9)
    
    print_info(f"Current: Python {version.major}.{version.minor}.{version.micro}")
    print_info(f"Minimum: Python {min_version[0]}.{min_version[1]}")
    
    if version >= min_version:
        print_success(f"Python {version.major}.{version.minor} meets requirements")
        return True
    else:
        print_error(f"Python {version.major}.{version.minor} is too old")
        return False

def check_packages() -> Tuple[bool, List[str]]:
    """Check required packages"""
    print_header("Package Dependencies Check")
    
    required_packages = [
        'fastapi',
        'sqlalchemy',
        'pandas',
        'numpy',
        'structlog',
        'fuzzywuzzy',
        'pytest',
    ]
    
    missing = []
    
    for package in required_packages:
        try:
            __import__(package)
            print_success(f"{package} installed")
        except ImportError:
            print_error(f"{package} NOT installed")
            missing.append(package)
    
    if missing:
        print_warning(f"Missing packages: {', '.join(missing)}")
        print_info(f"Install with: pip install {' '.join(missing)}")
    
    return len(missing) == 0, missing

def check_file_structure() -> bool:
    """Check essential files exist"""
    print_header("File Structure Check")
    
    required_files = [
        'backend/src/analytics/rigorous_engine.py',
        'backend/src/api/routes/analytics.py',
        'backend/test_analytics_engine.py',
        'frontend/src/pages/AnalyticsDashboard.jsx',
        'ANALYTICS_ENGINE_DOCS.md',
        'INTEGRATION_GUIDE.md',
    ]
    
    all_exist = True
    
    for file_path in required_files:
        full_path = Path(file_path)
        if full_path.exists():
            size_kb = full_path.stat().st_size / 1024
            print_success(f"{file_path} ({size_kb:.1f} KB)")
        else:
            print_error(f"{file_path} NOT FOUND")
            all_exist = False
    
    return all_exist

async def check_database() -> bool:
    """Check database connectivity"""
    print_header("Database Connectivity Check")
    
    try:
        from src.db.session import AsyncSessionLocal
        from sqlalchemy import select, func
        from src.db.models import Job
        
        async with AsyncSessionLocal() as db:
            try:
                # Test connection
                result = await db.execute(select(func.count(Job.id)))
                job_count = result.scalar()
                
                print_success(f"Database connected")
                print_info(f"Total jobs in database: {job_count:,}")
                
                return True
            except Exception as e:
                print_error(f"Database query failed: {str(e)}")
                return False
    except ImportError as e:
        print_warning(f"Cannot import DB modules: {str(e)}")
        return False

async def check_analytics_engine() -> bool:
    """Run basic analytics engine test"""
    print_header("Analytics Engine Test")
    
    try:
        from src.analytics.rigorous_engine import (
            clean_jobs_data,
            validate_and_extract_skills,
            analyze_skill_demand,
        )
        
        print_success("Analytics engine imports successful")
        
        # Try loading the engine module
        from src.analytics import rigorous_engine
        
        # Check constants
        print_info(f"MIN_JOBS_FOR_SKILL_ANALYSIS: {rigorous_engine.MIN_JOBS_FOR_SKILL_ANALYSIS}")
        print_info(f"MIN_SKILL_APPEARANCES: {rigorous_engine.MIN_SKILL_APPEARANCES}")
        print_info(f"MIN_SAMPLE_FOR_HIGH_CONFIDENCE: {rigorous_engine.MIN_SAMPLE_FOR_HIGH_CONFIDENCE}")
        
        print_success("Analytics engine configuration loaded")
        
        return True
    except ImportError as e:
        print_error(f"Failed to import analytics engine: {str(e)}")
        return False
    except Exception as e:
        print_error(f"Analytics engine error: {str(e)}")
        return False

async def check_api_routes() -> bool:
    """Check API routes are configured"""
    print_header("API Routes Check")
    
    try:
        from src.api.routes import analytics
        
        # Check router exists
        if hasattr(analytics, 'router'):
            print_success("Analytics router found")
        else:
            print_error("Analytics router not found")
            return False
        
        # Check endpoints
        endpoints = [
            'get_rigorous_analytics',
            'get_skill_insights',
            'get_trending_skills',
            'get_salary_insights',
            'get_market_insights',
            'get_actionable_insights',
            'get_data_quality_report',
        ]
        
        for endpoint in endpoints:
            if hasattr(analytics, endpoint):
                print_success(f"Endpoint '{endpoint}' found")
            else:
                print_warning(f"Endpoint '{endpoint}' not found")
        
        return True
    except ImportError as e:
        print_error(f"Failed to import analytics routes: {str(e)}")
        return False

def check_frontend_component() -> bool:
    """Check frontend component exists"""
    print_header("Frontend Component Check")
    
    component_path = Path('frontend/src/pages/AnalyticsDashboard.jsx')
    
    if component_path.exists():
        with open(component_path, 'r') as f:
            content = f.read()
        
        checks = {
            'React import': 'import React' in content,
            'Analytics fetch': 'analytics/rigorous' in content,
            'Tabs UI': "activeTab === 'insights'" in content,
            'Charts': 'BarChart' in content,
        }
        
        all_pass = True
        for check_name, result in checks.items():
            if result:
                print_success(f"{check_name} present")
            else:
                print_warning(f"{check_name} missing")
                all_pass = False
        
        return all_pass
    else:
        print_error(f"{component_path} not found")
        return False

def check_documentation() -> bool:
    """Check documentation files"""
    print_header("Documentation Check")
    
    docs = {
        'ANALYTICS_ENGINE_DOCS.md': 'Complete API documentation',
        'INTEGRATION_GUIDE.md': 'Integration instructions',
    }
    
    all_exist = True
    
    for doc_file, description in docs.items():
        doc_path = Path(doc_file)
        if doc_path.exists():
            size_kb = doc_path.stat().st_size / 1024
            print_success(f"{doc_file} ({size_kb:.0f} KB) - {description}")
        else:
            print_error(f"{doc_file} NOT FOUND")
            all_exist = False
    
    return all_exist

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    """Run all checks"""
    
    print(f"\n{BLUE}{'='*70}{RESET}")
    print(f"{BLUE}{'RIGOROUS ANALYTICS ENGINE - SETUP VERIFICATION'.center(70)}{RESET}")
    print(f"{BLUE}{'='*70}{RESET}\n")
    
    results = {}
    
    # Synchronous checks
    results['Python Version'] = check_python_version()
    results['Packages'] = check_packages()[0]
    results['File Structure'] = check_file_structure()
    results['Frontend Component'] = check_frontend_component()
    results['Documentation'] = check_documentation()
    
    # Async checks
    results['Database'] = await check_database()
    results['Analytics Engine'] = await check_analytics_engine()
    results['API Routes'] = await check_api_routes()
    
    # Summary
    print_header("Summary")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for check_name, result in results.items():
        status = f"{GREEN}✅ PASS{RESET}" if result else f"{RED}❌ FAIL{RESET}"
        print(f"  {status} - {check_name}")
    
    print(f"\n{BLUE}Total: {passed}/{total} checks passed{RESET}\n")
    
    if passed == total:
        print_success("All checks passed! System is ready.")
        print_info("Next steps:")
        print_info("  1. Start backend: cd backend && uvicorn src.api.main:app --reload")
        print_info("  2. Start frontend: cd frontend && npm run dev")
        print_info("  3. Open: http://localhost:5173/analytics")
        return 0
    else:
        print_error(f"{total - passed} check(s) failed. See above for details.")
        return 1

if __name__ == '__main__':
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
