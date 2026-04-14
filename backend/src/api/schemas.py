"""
Pydantic v2 response schemas.
Field names here are the exact names the frontend expects — do not rename them.
"""

from datetime import datetime
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field, model_validator
import math


# ── /api/skills ────────────────────────────────────────────────────────────────

class SkillItem(BaseModel):
    name: str
    frequency: int                        # job count
    average_salary: Optional[float]       # avg salary_mid for jobs with this skill
    job_count: int                         # same as frequency (frontend uses both)
    category: str                          # language / framework / cloud / tool / data / soft

    model_config = {"from_attributes": True}


class SkillsResponse(BaseModel):
    skills: List[SkillItem]
    total_count: int
    limit: int
    offset: int
    cache_status: str = "MISS"


# ── /api/trends/emerging ───────────────────────────────────────────────────────

class EmergingSkillItem(BaseModel):
    name: str
    growth_rate: float      # 0.0 – 1.0  (e.g. 0.45 = 45% growth)
    job_count: int
    trending: bool = True

    model_config = {"from_attributes": True}


class EmergingTrendsResponse(BaseModel):
    emerging_skills: List[EmergingSkillItem]
    limit: int
    period: str = "week-over-week"
    cache_status: str = "MISS"


# ── /api/pipeline/runs ────────────────────────────────────────────────────────

class PipelineRunItem(BaseModel):
    id: int
    status: str                           # running / success / failed
    jobs_inserted: int
    jobs_fetched: int
    jobs_skipped: int
    unique_skills: int = 0
    completed_at: Optional[datetime]       # finished_at in DB
    started_at: datetime
    duration_seconds: Optional[float]
    error_message: Optional[str]

    model_config = {"from_attributes": True}


class PipelineRunsResponse(BaseModel):
    runs: List[PipelineRunItem]
    total_count: int
    cache_status: str = "MISS"


# ── /api/pipeline/run (POST) ──────────────────────────────────────────────────

class PipelineRunStats(BaseModel):
    jobs_inserted: int
    duplicates_skipped: int
    skills_extracted: int
    errors: int


class PipelineRunResponse(BaseModel):
    status: str
    message: str
    run_id: int
    statistics: PipelineRunStats
    timestamp: datetime


# ── /api/jobs/search ──────────────────────────────────────────────────────────

class JobItem(BaseModel):
    id: str
    title: str
    company: str
    location: str
    description: str
    salary_min: Optional[float]
    salary_max: Optional[float]
    salary_mid: Optional[float]
    salary_currency: str = "USD"
    created: Optional[datetime]            # posted_at from DB
    url: Optional[str]
    remote: bool = False
    skills: List[str] = []

    model_config = {"from_attributes": True}


class PaginationMeta(BaseModel):
    page: int
    limit: int
    total: int
    pages: int


class JobsSearchResponse(BaseModel):
    data: List[JobItem]
    pagination: PaginationMeta
    cache_status: str = "MISS"


# ── /api/health ───────────────────────────────────────────────────────────────

class CacheStats(BaseModel):
    total_entries: int
    max_entries: int
    utilization_percent: float


class HealthResponse(BaseModel):
    status: str = "healthy"
    database: str
    timestamp: datetime
    cache_stats: Optional[CacheStats]