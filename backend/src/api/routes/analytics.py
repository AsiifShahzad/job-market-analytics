"""
Analytics API Routes
Serves all 9 analytics steps + data quality reports
"""

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
import json

from src.db.session import get_db
from src.analytics.rigorous_engine import compute_rigorous_analytics
from src.cache.analytics_cache import AnalyticsCache
import structlog

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/analytics", tags=["analytics"])


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "analytics-engine"}


@router.get("/rigorous", response_model=dict)
async def get_rigorous_analytics(
    db: AsyncSession = Depends(get_db)
):
    """
    Execute full rigorous analytics pipeline with 70-hour caching.
    
    Strategy:
    - Check if cache is valid (less than 70 hours old)
    - If valid: return cached data immediately (fast)
    - If expired: fetch new data and append with old data
    - Always update timestamp
    
    Returns:
    - skill_insights: Skills organized by category with demand metrics
    - trending_skills: Skills with week-over-week growth analysis
    - salary_insights: Salary benchmarks by skill and seniority
    - market_insights: Location and remote work statistics
    - skill_combinations: High-value skill pairs
    - actionable_insights: Executive summary (HIGH confidence only)
    - data_quality_report: Complete data cleaning audit trail
    """
    try:
        # Try to get from cache first (70-hour validity)
        cached_data = AnalyticsCache.get("analytics")
        if cached_data:
            cache_age = AnalyticsCache.get_cache_age_hours()
            logger.info(
                "Returning cached analytics",
                cache_age_hours=round(cache_age, 2),
                reason="Cache is valid (< 70 hours)"
            )
            return cached_data
        
        # Cache expired or doesn't exist, fetch new data
        logger.info("Cache expired or missing, computing fresh analytics...")
        output = await compute_rigorous_analytics(db)
        
        # Format output
        new_data = {
            "skill_insights": output.skill_insights,
            "trending_skills": output.trending_skills,
            "salary_insights": output.salary_insights,
            "market_insights": output.market_insights,
            "skill_combinations": output.skill_combinations,
            "actionable_insights": [
                {
                    "text": insight.text,
                    "confidence": insight.confidence,
                    "reason": insight.reason,
                    "sample_size": insight.sample_size,
                }
                for insight in output.actionable_insights
            ],
            "data_quality_report": {
                "invalid_skills_removed": output.data_quality_report.invalid_skills_removed,
                "invalid_locations_removed": output.data_quality_report.invalid_locations_removed,
                "low_sample_insights_filtered": output.data_quality_report.low_sample_insights_filtered,
                "jobs_before_cleaning": output.data_quality_report.jobs_before_cleaning,
                "jobs_after_cleaning": output.data_quality_report.jobs_after_cleaning,
                "skills_validated": output.data_quality_report.skills_validated,
                "skills_removed_as_noise": output.data_quality_report.skills_removed_as_noise,
            },
        }
        
        # Merge with existing cache (append strategy)
        old_cache = AnalyticsCache.get("analytics")
        if old_cache:
            logger.info("Appending new analytics to old cache data")
            result = AnalyticsCache.append(new_data, "analytics")
        else:
            logger.info("Setting new analytics cache")
            AnalyticsCache.set(new_data, "analytics")
            result = new_data
        
        logger.info("Analytics computed and cached successfully")
        return result
    
    except Exception as e:
        logger.error("Analytics request failed", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Analytics computation failed")


@router.get("/skills", response_model=dict)
async def get_skill_insights(db: AsyncSession = Depends(get_db)):
    """Get skill demand analysis only"""
    try:
        output = await compute_rigorous_analytics(db)
        return output.skill_insights
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trending", response_model=dict)
async def get_trending_skills(db: AsyncSession = Depends(get_db)):
    """Get trending skills analysis (week-over-week growth)"""
    try:
        output = await compute_rigorous_analytics(db)
        return {
            "trending_skills": output.trending_skills,
            "analysis_time_window": "7-day week-over-week comparison",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/salary", response_model=dict)
async def get_salary_insights(db: AsyncSession = Depends(get_db)):
    """Get salary analysis (outliers removed, minimum sample sizes enforced)"""
    try:
        output = await compute_rigorous_analytics(db)
        return output.salary_insights
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/market", response_model=dict)
async def get_market_insights(db: AsyncSession = Depends(get_db)):
    """Get market insights (locations, remote %, etc)"""
    try:
        output = await compute_rigorous_analytics(db)
        return output.market_insights
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/combinations", response_model=dict)
async def get_skill_combinations(db: AsyncSession = Depends(get_db)):
    """Get high-value skill combinations"""
    try:
        output = await compute_rigorous_analytics(db)
        return {"skill_combinations": output.skill_combinations}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insights", response_model=dict)
async def get_actionable_insights(db: AsyncSession = Depends(get_db)):
    """Get executive summary (actionable insights only)"""
    try:
        output = await compute_rigorous_analytics(db)
        return {
            "actionable_insights": [
                {
                    "text": insight.text,
                    "confidence": insight.confidence,
                    "reason": insight.reason,
                    "sample_size": insight.sample_size,
                }
                for insight in output.actionable_insights
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quality-report", response_model=dict)
async def get_data_quality_report(db: AsyncSession = Depends(get_db)):
    """Get complete data quality and cleaning audit trail"""
    try:
        output = await compute_rigorous_analytics(db)
        report = output.data_quality_report
        
        return {
            "jobs_before_cleaning": report.jobs_before_cleaning,
            "jobs_after_cleaning": report.jobs_after_cleaning,
            "jobs_removed": report.jobs_before_cleaning - report.jobs_after_cleaning,
            "data_retention_rate": round(
                (report.jobs_after_cleaning / report.jobs_before_cleaning * 100), 2
            ),
            "skills_validated": report.skills_validated,
            "skills_removed_as_noise": report.skills_removed_as_noise,
            "invalid_locations_removed": report.invalid_locations_removed,
            "invalid_skills_removed": report.invalid_skills_removed[:20],  # First 20
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── CACHE MANAGEMENT ENDPOINTS ────────────────────────────────────────────────

@router.get("/cache/status", response_model=dict)
async def get_cache_status():
    """Get current analytics cache status and age"""
    try:
        cache_age = AnalyticsCache.get_cache_age_hours()
        cached_data = AnalyticsCache.get("analytics")
        
        if cached_data and cache_age:
            logger.info("Cache status", age_hours=round(cache_age, 2), valid=True)
            return {
                "has_cache": True,
                "cache_age_hours": round(cache_age, 2),
                "cache_valid": cache_age < 70,
                "cache_expires_in_hours": round(70 - cache_age, 2),
                "cache_contains_keys": list(cached_data.keys()) if cached_data else [],
            }
        else:
            return {
                "has_cache": False,
                "cache_age_hours": None,
                "cache_valid": False,
                "cache_expires_in_hours": None,
                "cache_contains_keys": [],
            }
    except Exception as e:
        logger.error("Failed to get cache status", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get cache status")


@router.post("/cache/clear", response_model=dict)
async def clear_cache():
    """Manually clear analytics cache"""
    try:
        AnalyticsCache.clear()
        logger.info("Cache cleared via API request")
        return {
            "status": "success",
            "message": "Analytics cache cleared successfully",
            "timestamp": str(__import__('datetime').datetime.now(__import__('datetime').timezone.utc)),
        }
    except Exception as e:
        logger.error("Failed to clear cache", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to clear cache")
