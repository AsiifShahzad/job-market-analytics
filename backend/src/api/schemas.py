"""
Pydantic v2 response and request schemas for all API endpoints
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, computed_field


# ============================================================================
# Skills Endpoints Schemas
# ============================================================================

class SkillSummary(BaseModel):
    """Summary of a skill with demand metrics"""
    name: str = Field(..., description="Skill name")
    category: str = Field(..., description="language/framework/cloud/tool/data/soft")
    job_count: int = Field(..., description="Number of jobs requiring this skill")
    avg_salary_mid: Optional[float] = Field(None, description="Average mid-point salary for jobs with this skill")
    city: Optional[str] = Field(None, description="City filter applied")
    country: Optional[str] = Field(None, description="Country filter applied")

    model_config = {"from_attributes": True}


class SkillTrendPoint(BaseModel):
    """Single data point in skill trend time series"""
    date: datetime = Field(..., description="Snapshot date")
    job_count: int = Field(..., description="Number of jobs with this skill on this date")
    avg_salary_mid: Optional[float] = Field(None, description="Average salary on this date")
    week_over_week_growth: Optional[float] = Field(None, description="Week-over-week percentage growth")

    model_config = {"from_attributes": True}


class SkillTrendResponse(BaseModel):
    """Time series trend data for a skill"""
    skill_name: str = Field(..., description="Skill name")
    category: str = Field(..., description="Skill category")
    city: Optional[str] = Field(None, description="City filter applied")
    country: Optional[str] = Field(None, description="Country filter applied")
    timepoints: List[SkillTrendPoint] = Field(..., description="Weekly snapshot data points")
    total_data_points: int = Field(..., description="Total number of timepoints")

    model_config = {"from_attributes": True}


class CooccurrencePair(BaseModel):
    """Co-occurring skill pair with frequency"""
    skill_1: str = Field(..., description="First skill name")
    skill_2: str = Field(..., description="Second skill name")
    co_occurrence_count: int = Field(..., description="Number of jobs with both skills")
    confidence: float = Field(..., description="Confidence: co_occurrence_count / jobs_with_skill_1")

    model_config = {"from_attributes": True}


class SkillsListResponse(BaseModel):
    """Response for GET /api/skills"""
    skills: List[SkillSummary] = Field(..., description="List of skills")
    total_count: int = Field(..., description="Total skills matching filters")
    limit: int = Field(..., description="Limit applied")
    offset: int = Field(..., description="Offset applied")
    cache_status: str = Field("MISS", description="Cache hit/miss indicator")


# ============================================================================
# Salaries Endpoints Schemas
# ============================================================================

class SalaryBand(BaseModel):
    """Salary percentile band"""
    p25: Optional[float] = Field(None, description="25th percentile salary")
    p50: Optional[float] = Field(None, description="50th percentile (median) salary")
    p75: Optional[float] = Field(None, description="75th percentile salary")
    count: int = Field(..., description="Number of salary data points")


class SalaryResponse(BaseModel):
    """Response for GET /api/salaries"""
    salary_band: SalaryBand = Field(..., description="Salary percentile bands")
    title_filter: Optional[str] = Field(None, description="Job title filter applied")
    city_filter: Optional[str] = Field(None, description="City filter applied")
    skill_filter: Optional[str] = Field(None, description="Skill filter applied")
    seniority_filter: Optional[str] = Field(None, description="Seniority level filter applied")
    cache_status: str = Field("MISS", description="Cache hit/miss indicator")

    model_config = {"from_attributes": True}


class SkillPremium(BaseModel):
    """Salary uplift associated with a specific skill"""
    skill_name: str = Field(..., description="Skill name")
    baseline_salary: Optional[float] = Field(None, description="Average salary without this skill")
    with_skill_salary: Optional[float] = Field(None, description="Average salary with this skill")
    absolute_premium: Optional[float] = Field(None, description="Absolute salary increase")
    percent_premium: Optional[float] = Field(None, description="Percentage salary increase")
    job_count_with_skill: int = Field(..., description="Number of jobs with this skill")

    model_config = {"from_attributes": True}


class SalaryPremiumResponse(BaseModel):
    """Response for GET /api/salaries/skill-premium"""
    premiums: List[SkillPremium] = Field(..., description="Per-skill salary premiums")
    city_filter: Optional[str] = Field(None, description="City filter applied")
    title_filter: Optional[str] = Field(None, description="Job title filter applied")
    cache_status: str = Field("MISS", description="Cache hit/miss indicator")


# ============================================================================
# Pipeline Endpoints Schemas
# ============================================================================

class PipelineRunSummary(BaseModel):
    """Summary of a single pipeline run"""
    id: int = Field(..., description="Run ID")
    started_at: datetime = Field(..., description="Start timestamp")
    finished_at: Optional[datetime] = Field(None, description="Finish timestamp (null if running)")
    status: str = Field(..., description="running/success/failed")
    jobs_fetched: int = Field(..., description="Total jobs fetched from API")
    jobs_inserted: int = Field(..., description="Jobs successfully inserted to DB")
    jobs_skipped: int = Field(..., description="Jobs skipped (duplicates/errors)")
    error_message: Optional[str] = Field(None, description="Error message if status=failed")

    model_config = {"from_attributes": True}

    @computed_field
    @property
    def duration_seconds(self) -> Optional[float]:
        """Compute duration if both timestamps are available"""
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None


class PipelineRunsResponse(BaseModel):
    """Response for GET /api/pipeline/runs"""
    runs: List[PipelineRunSummary] = Field(..., description="List of recent pipeline runs")
    total_count: int = Field(..., description="Total runs in system")
    cache_status: str = Field("MISS", description="Cache hit/miss indicator")


class PipelineStatus(BaseModel):
    """Response for GET /api/pipeline/{run_id}/status"""
    run_id: int = Field(..., description="Run ID")
    status: str = Field(..., description="running/success/failed")
    started_at: datetime = Field(..., description="Start timestamp")
    finished_at: Optional[datetime] = Field(None, description="Finish timestamp (null if running)")
    jobs_fetched: int = Field(..., description="Jobs fetched so far")
    jobs_inserted: int = Field(..., description="Jobs inserted so far")
    jobs_skipped: int = Field(..., description="Jobs skipped so far")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    duration_seconds: Optional[float] = Field(None, description="Duration in seconds")
    cache_status: str = Field("MISS", description="Cache hit/miss indicator")

    model_config = {"from_attributes": True}


class PipelineTriggerResponse(BaseModel):
    """Response for POST /api/pipeline/trigger"""
    run_id: int = Field(..., description="Newly created run ID")
    status: str = Field("running", description="Pipeline status (should be 'running')")
    message: str = Field(..., description="Success message")


# ============================================================================
# Trends Endpoints Schemas
# ============================================================================

class TrendPoint(BaseModel):
    """Trending skill with growth metrics"""
    skill_name: str = Field(..., description="Skill name")
    category: str = Field(..., description="Skill category")
    current_job_count: int = Field(..., description="Current number of jobs with this skill")
    week_over_week_growth: float = Field(..., description="Week-over-week percentage growth")
    week_over_week_growth_percent: str = Field(..., description="Week-over-week growth as formatted string")
    avg_salary_mid: Optional[float] = Field(None, description="Average salary for this skill")

    model_config = {"from_attributes": True}


class EmergingTrendsResponse(BaseModel):
    """Response for GET /api/trends/emerging"""
    emerging_skills: List[TrendPoint] = Field(..., description="Rapidly growing skills")
    limit: int = Field(..., description="Limit applied")
    period: str = Field("week-over-week", description="Growth period measured")
    cache_status: str = Field("MISS", description="Cache hit/miss indicator")


class HeatmapCell(BaseModel):
    """Single cell in skill-city heatmap"""
    skill_name: str = Field(..., description="Skill name")
    city: str = Field(..., description="City name")
    job_count: int = Field(..., description="Number of jobs with this skill in this city")
    avg_salary_mid: Optional[float] = Field(None, description="Average salary")

    model_config = {"from_attributes": True}


class HeatmapResponse(BaseModel):
    """Response for GET /api/trends/heatmap"""
    heatmap_data: List[HeatmapCell] = Field(..., description="Skill-city job count matrix")
    top_n_skills: int = Field(..., description="Top N skills included")
    top_n_cities: int = Field(..., description="Top N cities included")
    cache_status: str = Field("MISS", description="Cache hit/miss indicator")


# ============================================================================
# Error & Health Schemas
# ============================================================================

class ErrorResponse(BaseModel):
    """Standard error response"""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    status_code: int = Field(..., description="HTTP status code")


class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field("healthy", description="Service status")
    database: str = Field(..., description="Database connectivity status")
    timestamp: datetime = Field(..., description="Response timestamp")
    cache_stats: Optional[Dict[str, Any]] = Field(None, description="Cache statistics")
