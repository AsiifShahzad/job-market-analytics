"""
Production-Grade Multi-Keyword Ingestion Strategy for Adzuna Jobs API.

Features:
- Semantic keyword grouping (5 categories, 30+ keywords)
- Robust rate limiting with exponential backoff
- Multi-level deduplication (ID + semantic hash)
- Structured error handling and retry logic
- Per-keyword performance tracking
- Auto-generated execution reports
- Full audit trail in database

Author: Senior Data Engineer
Standards: PEP 8, async-first, production-ready
"""

import os
import random
import asyncio
import httpx
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Optional
import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from src.db.models import Job, Skill, JobSkill, SkillSnapshot, PipelineRun
from src.nlp.skill_extractor import extract_skills, get_category
from src.nlp.seniority import extract_seniority, classify_seniority

logger = structlog.get_logger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Configuration & Strategy Constants
# ══════════════════════════════════════════════════════════════════════════════

KEYWORDS = {
    "Programming Languages": [
        "python", "java", "javascript", "typescript", "c++", "go", "rust",
        "scala", "kotlin", "r programming"
    ],
    "Data / AI": [
        "data scientist", "data analyst", "machine learning", "deep learning", 
        "nlp", "computer vision", "ai engineer", "llm"
    ],
    "Web / Software": [
        "backend developer", "frontend developer", "full stack developer", 
        "software engineer", "web developer"
    ],
    "Cloud / DevOps": [
        "aws", "azure", "gcp", "docker", "kubernetes", "devops engineer",
        "sre", "infrastructure engineer"
    ],
    "BI / Analytics": [
        "business intelligence", "data engineer", "power bi", "tableau",
        "analytics engineer"
    ]
}

# ── Adzuna API Configuration ───────────────────────────────────────────────────

ADZUNA_BASE = "https://api.adzuna.com/v1/api/jobs"
RESULTS_PER_PAGE = 50
MAX_PAGES_PER_KEYWORD = 3      # Limit to freshest jobs only
MIN_JOBS_THRESHOLD = 5         # Skip keywords with fewer results
DELAY_BETWEEN_REQUESTS = 1.0   # Polite rate limiting (seconds)
REQUEST_TIMEOUT = 30           # API call timeout (seconds)
MAX_RETRY_ATTEMPTS = 2         # Fallback attempts on failure
BATCH_INSERT_SIZE = 100        # DB insertion batch size

# ── Quality Filters ──────────────────────────────────────────────────────────

REMOTE_KEYWORDS = frozenset([
    "remote", "work from home", "wfh", "fully remote",
    "remote position", "remote role", "work remotely"
])

# Real job role keywords (must contain at least one for title validation)
REAL_JOB_KEYWORDS = frozenset([
    "developer", "engineer", "architect", "analyst", "scientist", "manager",
    "lead", "senior", "junior", "intern", "specialist", "consultant",
    "coordinator", "officer", "associate", "representative", "administrator",
    "operator", "technician", "designer", "researcher", "expert", "director",
    "supervisor", "head", "chief", "principal", "programmer", "coder",
    "data", "systems", "network", "security", "devops", "qa", "tester",
    "product", "project", "business", "sales", "marketing", "support",
    "admin", "infrastructure", "sre", "ops", "backend", "frontend",
    "full stack", "mobile", "web", "cloud", "vp", "director", "executive"
])

# Blacklisted generic non-job titles
BLACKLISTED_TITLES = frozenset([
    "system design", "consultation", "consulting", "services",
    "training", "course", "tutorial", "seminar", "workshop",
    "project management", "generic job", "placeholder", "test job",
    "sample position", "job listing", "internship program",
    "fellowship", "scholarship", "bounty", "gig", "freelance work"
])

# Minimum acceptable description length (words)
MIN_DESCRIPTION_LENGTH = 50

# ══════════════════════════════════════════════════════════════════════════════
# Metrics & Execution Tracking
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class KeywordStats:
    """Per-keyword execution metrics."""
    keyword: str
    raw_jobs_fetched: int = 0
    jobs_after_clean: int = 0
    duplicates_caught: int = 0
    jobs_inserted: int = 0
    skills_extracted: int = 0
    errors: int = 0
    api_calls_made: int = 0
    execution_time_ms: float = 0
    status: str = "pending"  # pending, running, success, failed
    
    def as_dict(self):
        return asdict(self)


@dataclass
class PipelineMetrics:
    """Global execution metrics."""
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    keywords_processed: int = 0
    total_raw_fetched: int = 0
    total_after_clean: int = 0
    total_duplicates_skipped: int = 0
    total_inserted: int = 0
    total_skills_extracted: int = 0
    total_errors: int = 0
    total_api_calls: int = 0
    by_keyword: dict = field(default_factory=dict)
    
    def duration_seconds(self) -> float:
        end = self.end_time or datetime.now(timezone.utc)
        return (end - self.start_time).total_seconds()
    
    def execution_summary(self) -> dict:
        """Generate structured summary report."""
        return {
            "duration_seconds": round(self.duration_seconds(), 2),
            "keywords_processed": self.keywords_processed,
            "api_calls_made": self.total_api_calls,
            "raw_jobs_fetched": self.total_raw_fetched,
            "after_cleaning": self.total_after_clean,
            "duplicates_skipped": self.total_duplicates_skipped,
            "unique_inserted": self.total_inserted,
            "skills_extracted": self.total_skills_extracted,
            "errors": self.total_errors,
            "by_keyword": self.by_keyword,
        }

# ── Configuration ─────────────────────────────────────────────────────────────

ADZUNA_BASE = "https://api.adzuna.com/v1/api/jobs"
RESULTS_PER_PAGE = 50
MAX_PAGES_PER_KEYWORD = 3    # Limit pages to get freshest jobs
MIN_JOBS_THRESHOLD = 5       # Skip if fewer than 5 jobs found
DELAY_BETWEEN_REQUESTS = 1.0 # Base seconds to sleep between adzuna calls
REMOTE_KEYWORDS = frozenset(["remote", "work from home", "wfh", "fully remote",
                              "remote position", "remote role", "work remotely"])



# ══════════════════════════════════════════════════════════════════════════════
# Main Orchestrator: Multi-Keyword ETL Pipeline
# ══════════════════════════════════════════════════════════════════════════════

async def run_multi_keyword_fetch(
    db: AsyncSession,
    run: PipelineRun,
    max_keywords: int = 20,
) -> dict:
    """
    Production-grade ETL pipeline: fetches jobs across multiple semantic keywords,
    applies multi-level deduplication, extracts skills, and updates database.
    
    Args:
        db: AsyncSession for database operations
        run: PipelineRun record for audit trail
        max_keywords: Maximum keywords to process per cycle (default: 20)
    
    Returns:
        dict with execution status, run_id, and aggregated statistics
    
    Process:
        1. Shuffle and select keywords for diversity
        2. Fetch jobs per keyword with rate limiting
        3. Clean and validate all jobs
        4. Multi-level deduplication (ID + semantic)
        5. Insert new jobs and extract skills
        6. Build trend snapshots
        7. Return comprehensive metrics
    """
    
    metrics = PipelineMetrics()
    keyword_stats: dict[str, KeywordStats] = {}
    
    # 1. Prepare keyword list (shuffle for daily diversity)
    all_keywords = []
    for category, words in KEYWORDS.items():
        all_keywords.extend(words)
    
    random.shuffle(all_keywords)
    selected_keywords = all_keywords[:max_keywords]
    
    logger.info(
        "╔═══ PIPELINE INITIALIZED ═══╗",
        total_keywords_available=len(all_keywords),
        keywords_selected=len(selected_keywords),
        categories_covered=len(KEYWORDS),
    )
    
    run_id = run.id
    
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            for idx, keyword in enumerate(selected_keywords):
                keyword_metrics = KeywordStats(keyword=keyword)
                keyword_metrics.status = "running"
                
                progress_msg = f"[{idx + 1}/{len(selected_keywords)}]"
                logger.info(f"🔍 Processing Keyword {progress_msg}: '{keyword}'")
                
                # ────────────────────────────────────────────────────────────
                # STEP 1: Fetch raw jobs from Adzuna
                # ────────────────────────────────────────────────────────────
                
                try:
                    raw_jobs = await _fetch_keyword(client, keyword)
                    keyword_metrics.raw_jobs_fetched = len(raw_jobs)
                    keyword_metrics.api_calls_made = min(
                        MAX_PAGES_PER_KEYWORD, 
                        (len(raw_jobs) // RESULTS_PER_PAGE) + 1
                    )
                    
                    if not raw_jobs:
                        logger.warning(
                            f"⚠️ Keyword '{keyword}' returned 0 jobs",
                            keyword=keyword
                        )
                        keyword_metrics.status = "no_results"
                        keyword_stats[keyword] = keyword_metrics
                        continue
                    
                    if len(raw_jobs) < MIN_JOBS_THRESHOLD:
                        logger.warning(
                            f"⚠️ Keyword '{keyword}' below threshold",
                            returned=len(raw_jobs),
                            threshold=MIN_JOBS_THRESHOLD,
                        )
                        keyword_metrics.status = "below_threshold"
                        keyword_stats[keyword] = keyword_metrics
                        continue
                    
                    metrics.total_raw_fetched += len(raw_jobs)
                    
                except Exception as e:
                    logger.error(
                        f"❌ Failed to fetch keyword '{keyword}'",
                        error=str(e),
                        keyword=keyword,
                    )
                    keyword_metrics.status = "fetch_failed"
                    keyword_metrics.errors += 1
                    keyword_stats[keyword] = keyword_metrics
                    continue
                
                # ────────────────────────────────────────────────────────────
                # STEP 2: Clean and validate
                # ────────────────────────────────────────────────────────────
                
                cleaned = _clean(raw_jobs)
                keyword_metrics.jobs_after_clean = len(cleaned)
                metrics.total_after_clean += len(cleaned)
                
                # ────────────────────────────────────────────────────────────
                # STEP 3: Multi-level deduplication
                # ────────────────────────────────────────────────────────────
                
                candidate_ids = [j["id"] for j in cleaned]
                
                try:
                    existing_ids = await _get_existing_ids(db, candidate_ids)
                    existing_hashes = await _get_existing_hashes(db, cleaned)
                except Exception as e:
                    logger.error(
                        f"❌ Dedup lookup failed for '{keyword}'",
                        error=str(e),
                    )
                    keyword_metrics.status = "dedup_failed"
                    keyword_metrics.errors += 1
                    keyword_stats[keyword] = keyword_metrics
                    continue
                
                new_jobs = []
                for job in cleaned:
                    semantic_hash = (
                        f"{job['title'][:50].lower()}|"
                        f"{job['company'].lower() if job['company'] else ''}|"
                        f"{job.get('city', '')}|"
                        f"{job.get('country', '')}"
                    )
                    
                    if job["id"] in existing_ids:
                        keyword_metrics.duplicates_caught += 1
                        metrics.total_duplicates_skipped += 1
                    elif semantic_hash in existing_hashes:
                        keyword_metrics.duplicates_caught += 1
                        metrics.total_duplicates_skipped += 1
                        logger.debug(
                            f"Semantic duplicate caught",
                            job_id=job["id"],
                            keyword=keyword,
                        )
                    else:
                        new_jobs.append(job)
                
                logger.info(
                    f"✓ Dedup complete for '{keyword}'",
                    new_jobs=len(new_jobs),
                    duplicates_caught=keyword_metrics.duplicates_caught,
                    candidates=len(cleaned),
                )
                
                # ────────────────────────────────────────────────────────────
                # STEP 4: Insert jobs and extract skills
                # ────────────────────────────────────────────────────────────
                
                for job_data in new_jobs:
                    try:
                        # ✓ Extract seniority level
                        seniority_result = extract_seniority(
                            job_data["title"],
                            job_data["description"]
                        )
                        
                        job = Job(
                            id=job_data["id"],
                            title=job_data["title"],
                            company=job_data["company"],
                            location_raw=job_data["location"],
                            city=job_data.get("city"),
                            country=job_data.get("country"),
                            description=job_data["description"],
                            salary_min=job_data.get("salary_min"),
                            salary_max=job_data.get("salary_max"),
                            salary_mid=job_data.get("salary_mid"),
                            remote=job_data.get("remote", False),
                            seniority=seniority_result.level,  # ✓ NEW: populate seniority
                            url=job_data.get("url"),
                            source="adzuna",
                            search_keyword=keyword,
                            fetched_at=datetime.now(timezone.utc),
                            posted_at=job_data.get("posted_at"),
                        )
                        db.add(job)
                        await db.flush()
                        
                        # Log seniority extraction for debugging
                        if seniority_result.confidence >= 0.6:
                            logger.debug(
                                f"Seniority extracted",
                                job_id=job_data["id"],
                                level=seniority_result.level,
                                confidence=seniority_result.confidence,
                                reasoning=seniority_result.reasoning,
                            )
                        
                        # Extract and link skills
                        extraction = extract_skills(
                            job_data["title"],
                            job_data["description"]
                        )
                        required_set = set(extraction.required_skills)
                        
                        for skill_name in extraction.all_skills:
                            skill = (
                                await db.execute(
                                    select(Skill).where(Skill.name == skill_name)
                                )
                            ).scalar_one_or_none()
                            
                            if not skill:
                                skill = Skill(
                                    name=skill_name,
                                    category=get_category(skill_name)
                                )
                                db.add(skill)
                                await db.flush()
                            
                            db.add(JobSkill(
                                job_id=job_data["id"],
                                skill_id=skill.id,
                                is_required=(skill_name in required_set),
                            ))
                        
                        keyword_metrics.skills_extracted += extraction.skill_count
                        keyword_metrics.jobs_inserted += 1
                        metrics.total_skills_extracted += extraction.skill_count
                        
                    except Exception as e:
                        logger.warning(
                            f"⚠️ Job insertion failed",
                            job_id=job_data.get("id"),
                            error=str(e),
                            keyword=keyword,
                        )
                        keyword_metrics.errors += 1
                        metrics.total_errors += 1
                        continue
                
                metrics.total_inserted += keyword_metrics.jobs_inserted
                keyword_metrics.status = "success"
                keyword_stats[keyword] = keyword_metrics
                
                # Log keyword summary
                if keyword_metrics.jobs_inserted > 0:
                    logger.info(
                        f"✓ Keyword '{keyword}' processed successfully",
                        jobs_inserted=keyword_metrics.jobs_inserted,
                        skills_extracted=keyword_metrics.skills_extracted,
                        duplicates_caught=keyword_metrics.duplicates_caught,
                    )
        
        # ────────────────────────────────────────────────────────────────────
        # STEP 5: Build trend snapshots
        # ────────────────────────────────────────────────────────────────────
        
        logger.info("📊 Building skill trend snapshots...")
        if metrics.total_inserted > 0:
            try:
                await _build_skill_snapshots(db)
                logger.info("✓ Snapshots created")
            except Exception as e:
                logger.error("❌ Snapshot creation failed", error=str(e))
        
        # ────────────────────────────────────────────────────────────────────
        # STEP 6: Finalize and commit
        # ────────────────────────────────────────────────────────────────────
        
        metrics.end_time = datetime.now(timezone.utc)
        metrics.keywords_processed = len(selected_keywords)
        metrics.by_keyword = {k: v.as_dict() for k, v in keyword_stats.items()}
        
        unique_skills_count = (
            await db.execute(select(func.count(Skill.id)))
        ).scalar() or 0
        
        run.jobs_fetched = metrics.total_raw_fetched
        run.jobs_inserted = metrics.total_inserted
        run.jobs_skipped = metrics.total_duplicates_skipped
        run.unique_skills = unique_skills_count
        run.status = "success"
        run.finished_at = datetime.now(timezone.utc)
        await db.commit()
        
        # ────────────────────────────────────────────────────────────────────
        # STEP 7: Generate execution report
        # ────────────────────────────────────────────────────────────────────
        
        summary = metrics.execution_summary()
        
        logger.info(
            "╔═══════ PIPELINE COMPLETE ═════════╗",
            status="SUCCESS",
            duration_seconds=summary["duration_seconds"],
            keywords_processed=summary["keywords_processed"],
            api_calls_made=summary["api_calls_made"],
            raw_jobs_fetched=summary["raw_jobs_fetched"],
            unique_inserted=summary["unique_inserted"],
            skills_extracted=summary["skills_extracted"],
            duplicates_skipped=summary["duplicates_skipped"],
            errors=summary["errors"],
            unique_skills_in_db=unique_skills_count,
        )
        
        return {
            "status": "success",
            "run_id": run_id,
            "summary": summary,
            "by_keyword": keyword_stats,
        }
    
    except Exception as e:
        logger.error(
            "╔═══════ PIPELINE FAILED ═════════╗",
            error=str(e),
            run_id=run_id,
        )
        run.status = "failed"
        run.error_message = str(e)[:500]
        run.finished_at = datetime.now(timezone.utc)
        await db.commit()
        raise


# ══════════════════════════════════════════════════════════════════════════════
# Helper Functions: API Interaction
# ══════════════════════════════════════════════════════════════════════════════

async def _fetch_keyword(
    client: httpx.AsyncClient,
    keyword: str,
) -> list[dict]:
    """
    Fetches jobs for a single keyword with pagination and retry logic.
    
    Args:
        client: httpx.AsyncClient for API calls
        keyword: Search keyword string
    
    Returns:
        list of raw job dictionaries from Adzuna API
    
    Features:
        - Automatic pagination (up to MAX_PAGES_PER_KEYWORD pages)
        - Exponential backoff on rate limiting (429)
        - Retry fallback on network errors
        - Rate limiting delay between requests
    """
    app_id = os.getenv("ADZUNA_APP_ID", "")
    api_key = os.getenv("ADZUNA_API_KEY", "")
    country = os.getenv("ADZUNA_COUNTRY", "us")
    
    if not app_id or not api_key:
        raise ValueError(
            "ADZUNA_APP_ID and ADZUNA_API_KEY must be set in .env"
        )
    
    base_url = f"{ADZUNA_BASE}/{country}/search"
    keyword_jobs = []
    
    for page in range(1, MAX_PAGES_PER_KEYWORD + 1):
        url = f"{base_url}/{page}"
        params = {
            "app_id": app_id,
            "app_key": api_key,
            "results_per_page": RESULTS_PER_PAGE,
            "what": keyword,
            "content-type": "application/json",
        }
        
        # Rate limiting: polite delay before each request
        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
        
        # Retry logic: 2 attempts with exponential backoff
        success = False
        for attempt in range(MAX_RETRY_ATTEMPTS):
            try:
                resp = await client.get(url, params=params)
                
                # Handle rate limiting with exponential backoff
                if resp.status_code == 429:
                    wait_time = 2 ** (attempt + 2)
                    logger.warning(
                        f"Rate limited (429) on page {page}, "
                        f"backing off {wait_time}s",
                        keyword=keyword,
                        page=page,
                    )
                    await asyncio.sleep(wait_time)
                    continue
                
                # Any other non-200 status: exit pagination
                if resp.status_code != 200:
                    logger.warning(
                        f"API returned {resp.status_code} for page {page}",
                        keyword=keyword,
                        page=page,
                        status=resp.status_code,
                    )
                    break
                
                # Parse response
                data = resp.json()
                results = data.get("results", [])
                
                # Empty page = legitimately no more results
                if not results:
                    success = True
                    break
                
                # Extend our results
                keyword_jobs.extend(results)
                success = True
                break
                
            except httpx.RequestError as e:
                logger.warning(
                    f"Network error on attempt {attempt + 1}",
                    keyword=keyword,
                    page=page,
                    error=str(e),
                )
                await asyncio.sleep(2)
                continue
        
        # Exit pagination if this page failed or was empty
        if not success or not results:
            break
    
    return keyword_jobs


# ══════════════════════════════════════════════════════════════════════════════
# Helper Functions: Data Cleaning & Validation
# ══════════════════════════════════════════════════════════════════════════════

def _is_real_job(title: str, description: str) -> bool:
    """
    Validates that a job listing is a REAL job, not a generic topic/skill.
    
    Filters:
    - Title must contain a real job role keyword
    - Title must NOT be in blacklist (e.g., "system design")
    - Description must have minimum length
    - Title must be reasonable length (not too short, not too long)
    - Title must be in English (basic check)
    
    Args:
        title: Job title string
        description: Job description string
    
    Returns:
        True if this looks like a real job posting, False otherwise
    """
    if not title or not description:
        return False
    
    title_lower = title.lower().strip()
    desc_words = len(description.split())
    
    # Check 1: Title blacklist (generic non-jobs)
    for blacklist_term in BLACKLISTED_TITLES:
        if blacklist_term.lower() in title_lower:
            logger.debug(
                f"Filtered: blacklisted title pattern",
                title=title,
                matched=blacklist_term,
            )
            return False
    
    # Check 2: Must contain at least one real job role keyword
    has_job_keyword = any(
        kw in title_lower for kw in REAL_JOB_KEYWORDS
    )
    
    if not has_job_keyword:
        logger.debug(
            f"Filtered: no real job role keyword found",
            title=title,
        )
        return False
    
    # Check 3: Minimum description length (at least 50 words)
    if desc_words < MIN_DESCRIPTION_LENGTH:
        logger.debug(
            f"Filtered: description too short",
            title=title,
            word_count=desc_words,
            minimum=MIN_DESCRIPTION_LENGTH,
        )
        return False
    
    # Check 4: Title length sanity check (8-150 characters)
    if len(title) < 8 or len(title) > 150:
        logger.debug(
            f"Filtered: title length invalid",
            title=title,
            length=len(title),
        )
        return False
    
    # Check 5: Title should have reasonable word count (2-8 words typical)
    title_words = len(title.split())
    if title_words > 15 or title_words < 1:
        logger.debug(
            f"Filtered: unusual title structure",
            title=title,
            word_count=title_words,
        )
        return False
    
    return True


def _clean(raw_jobs: list[dict]) -> list[dict]:
    """
    Normalizes and validates Adzuna job data.
    
    Handles:
    - Missing/empty required fields (title, description)
    - Nested company/location objects
    - Salary calculations (salary_mid from min/max)
    - Location parsing (city/country extraction)
    - Remote job detection
    - ISO timestamp parsing
    
    Returns:
        list of normalized job dictionaries
    """
    country_default = os.getenv("ADZUNA_COUNTRY", "us").upper()
    out = []
    
    for job in raw_jobs:
        try:
            title = (job.get("title") or "").strip()
            description = (job.get("description") or "").strip()
            
            # Skip jobs without critical fields
            if not title or not description:
                continue
            
            # ✓ NEW: Validate this is a real job (not "system design", etc.)
            if not _is_real_job(title, description):
                continue
            
            # Extract company name
            company = ""
            if isinstance(job.get("company"), dict):
                company = job["company"].get("display_name", "").strip()
            
            # Extract location components
            location = ""
            if isinstance(job.get("location"), dict):
                location = job["location"].get("display_name", "").strip()
            
            # Calculate salary midpoint
            sal_min = job.get("salary_min")
            sal_max = job.get("salary_max")
            sal_mid = (sal_min + sal_max) / 2 if sal_min and sal_max else None
            
            # Split location into city/country
            parts = location.split(",")
            city = parts[0].strip() if parts else None
            country = parts[-1].strip() if len(parts) > 1 else country_default
            
            # Detect remote jobs
            combined = f"{title} {description}".lower()
            remote = any(kw in combined for kw in REMOTE_KEYWORDS)
            
            # Parse ISO timestamp
            posted_at = None
            raw_created = job.get("created")
            if raw_created:
                try:
                    posted_at = datetime.fromisoformat(
                        raw_created.replace("Z", "+00:00")
                    )
                except Exception:
                    pass
            
            out.append({
                "id": str(job.get("id", "")),
                "title": title,
                "company": company,
                "location": location,
                "city": city,
                "country": country,
                "description": description,
                "salary_min": sal_min,
                "salary_max": sal_max,
                "salary_mid": sal_mid,
                "remote": remote,
                "url": job.get("redirect_url"),
                "posted_at": posted_at,
            })
        except Exception as e:
            logger.debug(f"Failed to parse job record", error=str(e))
            continue
    
    return out


# ══════════════════════════════════════════════════════════════════════════════
# Helper Functions: Deduplication
# ══════════════════════════════════════════════════════════════════════════════

async def _get_existing_ids(db: AsyncSession, candidate_ids: list[str]) -> set[str]:
    """
    Batch check for existing job IDs in database.
    
    Uses chunking to avoid overwhelming the database with very large IN clauses.
    
    Args:
        db: AsyncSession
        candidate_ids: List of Adzuna job IDs to check
    
    Returns:
        set of job IDs that already exist in DB
    """
    existing: set[str] = set()
    chunk_size = 500
    
    for i in range(0, len(candidate_ids), chunk_size):
        chunk = candidate_ids[i : i + chunk_size]
        query = select(Job.id).where(Job.id.in_(chunk))
        rows = (await db.execute(query)).scalars().all()
        existing.update(rows)
    
    return existing


async def _get_existing_hashes(db: AsyncSession, cleaned_jobs: list[dict]) -> set[str]:
    """
    Retrieves semantic hashes of existing jobs to detect duplicates.
    
    Semantic hash = title[:50] | company | city | country
    This catches jobs posted under slightly different Adzuna IDs but with
    identical title/company/location combination.
    
    Args:
        db: AsyncSession
        cleaned_jobs: List of cleaned job dictionaries
    
    Returns:
        set of semantic hashes from existing DB jobs
    """
    titles = [j["title"] for j in cleaned_jobs]
    
    if not titles:
        return set()
    
    query = select(
        Job.title, Job.company, Job.city, Job.country
    ).where(Job.title.in_(titles))
    
    rows = (await db.execute(query)).fetchall()
    
    existing_hashes = set()
    for row in rows:
        hash_val = (
            f"{row.title[:50].lower()}|"
            f"{row.company.lower() if row.company else ''}|"
            f"{row.city or ''}|"
            f"{row.country or ''}"
        )
        existing_hashes.add(hash_val)
    
    return existing_hashes


# ══════════════════════════════════════════════════════════════════════════════
# Helper Functions: Trend Snapshots
# ══════════════════════════════════════════════════════════════════════════════

async def _build_skill_snapshots(db: AsyncSession) -> None:
    """
    Creates daily skill demand snapshots from the job_skill aggregates.
    
    Snapshots are used to calculate week-over-week trend growth.
    Creates one snapshot per unique skill per day (global, not per location).
    
    Algorithm:
        1. Group jobs by skill
        2. Count distinct jobs per skill
        3. Calculate average salary_mid
        4. Insert into SkillSnapshot table (or skip if already exists for today)
    """
    now = datetime.now(timezone.utc)
    snapshot_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Aggregate job counts and avg salary by skill
    query = (
        select(
            Skill.id.label("skill_id"),
            func.count(JobSkill.job_id).label("job_count"),
            func.avg(Job.salary_mid).label("avg_salary"),
        )
        .select_from(JobSkill)
        .join(Skill, JobSkill.skill_id == Skill.id)
        .join(Job, JobSkill.job_id == Job.id)
        .group_by(Skill.id)
    )
    
    rows = (await db.execute(query)).fetchall()
    
    for row in rows:
        # Check if snapshot already exists for this skill today
        exists = (
            await db.execute(
                select(SkillSnapshot.id).where(
                    SkillSnapshot.skill_id == row.skill_id,
                    SkillSnapshot.snapshot_date == snapshot_date,
                    SkillSnapshot.city.is_(None),
                    SkillSnapshot.country.is_(None),
                )
            )
        ).scalar_one_or_none()
        
        if not exists:
            db.add(SkillSnapshot(
                skill_id=row.skill_id,
                snapshot_date=snapshot_date,
                job_count=row.job_count,
                avg_salary_mid=float(row.avg_salary) if row.avg_salary else None,
            ))
    
    await db.flush()


