"""
JobPulse AI Backend - RESTful API with Clear Pipeline Stages
No database dependency - Uses mock data for instant responses
Shows clear data processing stages
"""

import os
from datetime import datetime
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger(__name__)

# Create FastAPI app
app = FastAPI(
    title="JobPulse AI",
    description="Job Market Intelligence & Analytics API",
    version="2.0.0",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json"
)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:5174", "http://localhost:5175", "http://localhost:5176", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Pipeline-Stage"],
)

# ============================================================================
# PIPELINE STAGE RESPONSES - Show what stage data is at
# ============================================================================

class PipelineStage:
    """Enum for pipeline stages"""
    FETCHING = "fetching"  # Pulling from Adzuna API
    CLEANING = "cleaning"  # Data quality & normalization
    EXTRACTING = "extracting"  # Skill extraction from descriptions
    ANALYZING = "analyzing"  # Insights & trends
    READY = "ready"  # Final results ready


def response_with_pipeline(data: dict, stage: str, message: str = None) -> dict:
    """Wrap response with pipeline stage information"""
    return {
        "status": "success",
        "pipeline_stage": stage,
        "message": message,
        "timestamp": datetime.utcnow().isoformat(),
        "data": data
    }


# ============================================================================
# HEALTH CHECK & STATUS
# ============================================================================

@app.get("/api/health")
async def health_check():
    """
    Health check endpoint - instant response
    STAGE: READY
    """
    return response_with_pipeline(
        {
            "service": "JobPulse AI Backend",
            "version": "2.0.0",
            "status": "operational",
            "database": "disabled_using_mock_data"
        },
        stage=PipelineStage.READY,
        message="Service is healthy and ready"
    )


@app.get("/api/pipeline/status")
async def pipeline_status():
    """
    Show current pipeline status
    STAGE: ANALYZING
    """
    return response_with_pipeline(
        {
            "last_run": "2026-03-31T08:00:00Z",
            "status": "idle",
            "jobs_fetched_today": 1245,
            "skills_extracted": 89,
            "errors": 0,
            "avg_processing_time_ms": 2340
        },
        stage=PipelineStage.ANALYZING,
        message="Pipeline overview - all systems operational"
    )


# ============================================================================
# SKILLS ENDPOINTS - Key Market Data
# ============================================================================

@app.get("/api/skills")
async def get_skills(
    limit: int = Query(20, ge=1, le=100),
    city: str = Query(None),
    country: str = Query("GB")
):
    """
    Get top skills by job demand
    PIPELINE STAGES: FETCHING → EXTRACTING → ANALYZING → READY
    """
    skills_data = [
        {"name": "Python", "demand": 1245, "avg_salary": 85000, "trend": "up", "jobs_count": 1245},
        {"name": "JavaScript", "demand": 1089, "avg_salary": 72000, "trend": "stable", "jobs_count": 1089},
        {"name": "SQL", "demand": 987, "avg_salary": 68000, "trend": "up", "jobs_count": 987},
        {"name": "React", "demand": 876, "avg_salary": 78000, "trend": "up", "jobs_count": 876},
        {"name": "AWS", "demand": 765, "avg_salary": 95000, "trend": "up", "jobs_count": 765},
        {"name": "Docker", "demand": 654, "avg_salary": 88000, "trend": "up", "jobs_count": 654},
        {"name": "TypeScript", "demand": 543, "avg_salary": 82000, "trend": "up", "jobs_count": 543},
        {"name": "Node.js", "demand": 512, "avg_salary": 79000, "trend": "stable", "jobs_count": 512},
        {"name": "Kubernetes", "demand": 445, "avg_salary": 92000, "trend": "up", "jobs_count": 445},
        {"name": "FastAPI", "demand": 367, "avg_salary": 84000, "trend": "up", "jobs_count": 367},
    ]
    
    logger.info("fetching_skills", limit=limit, country=country)
    
    return response_with_pipeline(
        {
            "skills": skills_data[:limit],
            "total_jobs": sum(s["jobs_count"] for s in skills_data),
            "filters": {"country": country, "city": city}
        },
        stage=PipelineStage.READY,
        message=f"Retrieved {min(limit, len(skills_data))} top skills"
    )


# ============================================================================
# TRENDS ENDPOINTS - Emerging Skills
# ============================================================================

@app.get("/api/trends/emerging")
async def get_emerging_skills(days: int = Query(30)):
    """
    Get emerging/trending skills
    PIPELINE STAGES: FETCHING → ANALYZING → READY
    """
    emerging = [
        {"name": "AI/LLM", "growth": 156, "growth_percent": 85, "new_jobs": 234},
        {"name": "Prompt Engineering", "growth": 127, "growth_percent": 120, "new_jobs": 89},
        {"name": "Vector Databases", "growth": 94, "growth_percent": 95, "new_jobs": 42},
        {"name": "Rust", "growth": 67, "growth_percent": 45, "new_jobs": 78},
        {"name": "Go", "growth": 45, "growth_percent": 38, "new_jobs": 65},
    ]
    
    logger.info("analyzing_trends", period_days=days)
    
    return response_with_pipeline(
        {
            "trending_skills": emerging,
            "period_days": days,
            "analysis_date": datetime.utcnow().isoformat()
        },
        stage=PipelineStage.ANALYZING,
        message=f"Analyzed {days}-day trend period"
    )


@app.get("/api/trends/heatmap")
async def get_skills_heatmap():
    """
    Get skills co-occurrence heatmap
    PIPELINE STAGES: EXTRACTING → ANALYZING → READY
    """
    heatmap_data = {
        "Python": {"JavaScript": 0.78, "SQL": 0.92, "Docker": 0.65},
        "JavaScript": {"React": 0.89, "Node.js": 0.87, "TypeScript": 0.76},
        "React": {"TypeScript": 0.84, "JavaScript": 0.89, "CSS": 0.91},
        "AWS": {"Docker": 0.72, "Kubernetes": 0.68, "DevOps": 0.81},
    }
    
    return response_with_pipeline(
        {"co_occurrence_matrix": heatmap_data},
        stage=PipelineStage.ANALYZING,
        message="Skill relationship analysis complete"
    )


# ============================================================================
# SALARY ENDPOINTS - Compensation Insights
# ============================================================================

@app.get("/api/salaries")
async def get_salary_insights(skill: str = Query(None)):
    """
    Get salary data by skill
    PIPELINE STAGES: ANALYZING → READY
    """
    salary_data = {
        "Python": {"avg": 85000, "min": 45000, "max": 180000, "p90": 140000},
        "JavaScript": {"avg": 72000, "min": 35000, "max": 160000, "p90": 125000},
        "React": {"avg": 78000, "min": 40000, "max": 170000, "p90": 135000},
        "AWS": {"avg": 95000, "min": 55000, "max": 200000, "p90": 165000},
        "Machine Learning": {"avg": 105000, "min": 60000, "max": 220000, "p90": 180000},
    }
    
    logger.info("analyzing_salaries", skill=skill)
    
    if skill and skill in salary_data:
        return response_with_pipeline(
            {
                "skill": skill,
                "salary": salary_data[skill],
                "currency": "GBP",
                "market": "UK"
            },
            stage=PipelineStage.READY,
            message=f"Salary analysis for {skill} complete"
        )
    
    return response_with_pipeline(
        salary_data,
        stage=PipelineStage.READY,
        message="Salary data for all top skills"
    )


@app.get("/api/salaries/skill-premium")
async def get_skill_premium():
    """
    Get salary premium by skill vs market average
    PIPELINE STAGES: ANALYZING → READY
    """
    premiums = {
        "Machine Learning": {"premium_percent": 47, "avg_salary": 105000},
        "AWS": {"premium_percent": 32, "avg_salary": 95000},
        "Kubernetes": {"premium_percent": 28, "avg_salary": 92000},
        "Python": {"premium_percent": 29, "avg_salary": 85000},
        "TypeScript": {"premium_percent": 18, "avg_salary": 82000},
    }
    
    return response_with_pipeline(
        premiums,
        stage=PipelineStage.ANALYZING,
        message="Skill premium analysis complete"
    )


# ============================================================================
# PIPELINE ENDPOINTS - Data Processing Pipeline
# ============================================================================

@app.get("/api/pipeline/runs")
async def get_pipeline_runs(limit: int = Query(10)):
    """
    Get recent pipeline execution runs
    PIPELINE STAGES: ANALYZING → READY
    """
    runs = [
        {"run_id": 1, "status": "success", "stage": "ready", "jobs_processed": 1245, "timestamp": "2026-03-31T07:30:00Z"},
        {"run_id": 2, "status": "success", "stage": "ready", "jobs_processed": 1089, "timestamp": "2026-03-31T06:30:00Z"},
        {"run_id": 3, "status": "success", "stage": "ready", "jobs_processed": 987, "timestamp": "2026-03-31T05:30:00Z"},
    ]
    
    return response_with_pipeline(
        {"runs": runs[:limit]},
        stage=PipelineStage.READY,
        message=f"Retrieved {min(limit, len(runs))} recent pipeline runs"
    )


@app.get("/api/pipeline/{run_id}/status")
async def get_pipeline_run_status(run_id: int):
    """
    Get status of specific pipeline run
    PIPELINE STAGES: ANALYZING → READY
    """
    stages = [
        {"stage": "fetching", "status": "completed", "records": 1245, "duration_ms": 5340},
        {"stage": "cleaning", "status": "completed", "records": 1245, "duration_ms": 2100},
        {"stage": "extracting", "status": "completed", "records": 1245, "duration_ms": 3450},
        {"stage": "analyzing", "status": "completed", "records": 1245, "duration_ms": 1280},
    ]
    
    return response_with_pipeline(
        {
            "run_id": run_id,
            "overall_status": "success",
            "stages": stages,
            "total_duration_ms": sum(s["duration_ms"] for s in stages)
        },
        stage=PipelineStage.READY,
        message="Pipeline run completed successfully"
    )


@app.post("/api/pipeline/trigger")
async def trigger_pipeline():
    """
    Manually trigger pipeline execution
    PIPELINE STAGES: FETCHING
    """
    logger.info("triggering_pipeline")
    
    return response_with_pipeline(
        {
            "run_id": 4,
            "status": "started",
            "first_stage": "fetching",
            "message": "Pipeline started - fetching jobs from Adzuna API"
        },
        stage=PipelineStage.FETCHING,
        message="Pipeline execution triggered"
    )


# ============================================================================
# ADZUNA DIRECT ENDPOINTS - Raw Adzuna Data
# ============================================================================

@app.get("/api/adzuna/jobs")
async def get_adzuna_jobs(
    location: str = Query("GB"),
    limit: int = Query(10, ge=1, le=50)
):
    """
    Get jobs from Adzuna API
    PIPELINE STAGES: FETCHING
    """
    jobs = [
        {
            "title": "Senior Python Developer",
            "company": "Tech Corp",
            "location": "London",
            "salary": "£80,000 - £120,000",
            "description": "Python FastAPI Docker AWS"
        },
        {
            "title": "React Developer",
            "company": "Web Solutions",
            "location": "Manchester",
            "salary": "£60,000 - £90,000",
            "description": "React TypeScript GraphQL Node.js"
        },
        {
            "title": "DevOps Engineer",
            "company": "Cloud Systems",
            "location": "Birmingham",
            "salary": "£70,000 - £110,000",
            "description": "Kubernetes Docker AWS Terraform CI/CD"
        },
    ]
    
    return response_with_pipeline(
        {
            "jobs": jobs[:limit],
            "location": location,
            "total_available": len(jobs)
        },
        stage=PipelineStage.FETCHING,
        message=f"Fetched {min(limit, len(jobs))} job listings"
    )


@app.get("/api/adzuna/skills-summary")
async def get_adzuna_skills_summary(limit: int = Query(10)):
    """
    Skill frequency from Adzuna jobs
    PIPELINE STAGES: FETCHING → EXTRACTING
    """
    skills = [
        {"name": "Python", "frequency": 145, "percentage": 24.5},
        {"name": "JavaScript", "frequency": 132, "percentage": 22.2},
        {"name": "SQL", "frequency": 128, "percentage": 21.6},
        {"name": "React", "frequency": 98, "percentage": 16.5},
        {"name": "AWS", "frequency": 87, "percentage": 14.7},
    ]
    
    return response_with_pipeline(
        {"skills": skills[:limit]},
        stage=PipelineStage.EXTRACTING,
        message=f"Extracted {min(limit, len(skills))} skills from job descriptions"
    )


# ============================================================================
# ROOT & DOCS
# ============================================================================

@app.get("/")
async def root():
    """API Documentation"""
    return {
        "message": "JobPulse AI - Job Market Intelligence API",
        "docs": "http://localhost:8000/api/docs",
        "version": "2.0.0",
        "endpoints": {
            "health": "/api/health",
            "skills": "/api/skills",
            "trends": "/api/trends/emerging",
            "salaries": "/api/salaries",
            "pipeline": "/api/pipeline/status",
            "jobs": "/api/adzuna/jobs"
        }
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("API_PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
