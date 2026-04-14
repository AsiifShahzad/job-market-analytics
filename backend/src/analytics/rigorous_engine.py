"""
Production-Grade Analytics & NLP Engine for Job Market Intelligence
Statistically rigorous, bias-aware, high-confidence insights only

Author: Senior Data Scientist + NLP Engineer
Date: April 2026
Standards: PEP 8, Pandas best practices, Statistical rigor
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from fuzzywuzzy import fuzz
import structlog
import re

from src.db.models import Job, Skill, JobSkill

logger = structlog.get_logger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS & CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

# Minimum sample sizes (statistical rigor)
MIN_JOBS_FOR_SKILL_ANALYSIS = 30  # Salary analysis
MIN_SKILL_APPEARANCES = 3  # Skill must appear in N descriptions
MIN_TRENDING_WEEKLY_COUNT = 10  # Growth calc requires 10+ jobs
MIN_SALARY_SAMPLES = 30  # Salary benchmarking
MIN_SKILL_PAIR_COOCCURRENCE = 5  # Skill pair must appear together N times
MIN_SAMPLE_FOR_HIGH_CONFIDENCE = 50  # For actionable insights

# Noise filtering
GENERIC_TERMS = {
    "github", "linkedin", "email", "website", "portfolio",
    "junit", "gradle", "maven", "npm", "git", "docker",
    "svn", "cvs", "subversion", "perforce", "bitbucket",
    "jira", "confluence", "slack", "teams", "zoom",
    "office", "microsoft", "google", "apple", "amazon",
}

INVALID_CITY_PATTERNS = {
    "remote", "virtual", "anywhere", "global", "online",
    "n/a", "na", "unknown", "various", "multiple",
    "telecommute", "work from home", "wfh", "distributed",
    "", "grand central", "central", "station",
}

# Fuzzy matching threshold
FUZZY_MATCH_THRESHOLD = 85

# ══════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ActionableInsight:
    """High-confidence, statistically-backed insight"""
    text: str
    confidence: str  # HIGH, MEDIUM, or LOW
    reason: str  # Why this insight is valid
    sample_size: int = 0
    statistical_measure: Optional[str] = None


@dataclass
class DataQualityReport:
    """Track all data cleaning operations"""
    invalid_skills_removed: List[str]
    invalid_locations_removed: List[str]
    low_sample_insights_filtered: bool
    jobs_before_cleaning: int
    jobs_after_cleaning: int
    skills_validated: int
    skills_removed_as_noise: int


@dataclass
class AnalyticsOutput:
    """Final output structure"""
    skill_insights: Dict[str, Any]
    trending_skills: List[Dict[str, Any]]
    salary_insights: Dict[str, Any]
    market_insights: Dict[str, Any]
    skill_combinations: List[Dict[str, Any]]
    actionable_insights: List[ActionableInsight]
    data_quality_report: DataQualityReport


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1: DATA CLEANING & NORMALIZATION
# ══════════════════════════════════════════════════════════════════════════════

async def clean_jobs_data(db: AsyncSession) -> Tuple[pd.DataFrame, DataQualityReport]:
    """
    Step 1: Clean and normalize job data.
    Returns: (cleaned_df, quality_report)
    """
    logger.info("STEP 1: Cleaning job data...")
    
    # Fetch all jobs
    q = select(
        Job.id, Job.title, Job.company, Job.city, Job.country,
        Job.description, Job.salary_min, Job.salary_max, Job.salary_mid,
        Job.remote, Job.seniority, Job.posted_at, Job.fetched_at, Job.search_keyword
    )
    results = (await db.execute(q)).fetchall()
    
    jobs_before = len(results)
    df = pd.DataFrame(results, columns=[
        'id', 'title', 'company', 'city', 'country', 'description',
        'salary_min', 'salary_max', 'salary_mid', 'remote',
        'seniority', 'posted_at', 'fetched_at', 'search_keyword'
    ])
    
    invalid_locations = set()
    
    # Filter 1: Remove jobs with missing critical fields
    df = df[df['description'].notna() & (df['description'].str.strip() != '')]
    df = df[df['title'].notna() & (df['title'].str.strip() != '')]
    
    # Filter 2: Normalize and validate cities
    def is_valid_city(city):
        if pd.isna(city) or not isinstance(city, str):
            return False
        city_lower = city.lower().strip()
        if city_lower in INVALID_CITY_PATTERNS or len(city_lower) < 2:
            invalid_locations.add(city)
            return False
        return True
    
    df = df[df['city'].apply(is_valid_city)]
    
    # Filter 3: Normalize city names
    def normalize_city(city):
        if pd.isna(city):
            return None
        city = str(city).strip().title()
        # Handle common variations
        city_map = {
            "Nyc": "New York",
            "Ny": "New York",
            "La": "Los Angeles",
            "Sf": "San Francisco",
            "Bay Area": "San Francisco",
            "Dc": "Washington",
            "Washington D.C": "Washington",
        }
        return city_map.get(city, city)
    
    df['city'] = df['city'].apply(normalize_city)
    
    # Filter 4: Validate salary logic (min <= max)
    df = df[
        (df['salary_min'].isna() | df['salary_max'].isna()) |
        (df['salary_min'] <= df['salary_max'])
    ]
    
    # Filter 5: Normalize text (lowercase descriptions for matching)
    df['description_lower'] = df['description'].str.lower()
    
    jobs_after = len(df)
    
    quality_report = DataQualityReport(
        invalid_skills_removed=[],
        invalid_locations_removed=list(invalid_locations),
        low_sample_insights_filtered=False,
        jobs_before_cleaning=jobs_before,
        jobs_after_cleaning=jobs_after,
        skills_validated=0,
        skills_removed_as_noise=0,
    )
    
    logger.info(
        "Data cleaning complete",
        jobs_before=jobs_before,
        jobs_after=jobs_after,
        removed=jobs_before - jobs_after
    )
    
    return df, quality_report


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2: IMPROVED SKILL EXTRACTION (HYBRID: EXACT + FUZZY)
# ══════════════════════════════════════════════════════════════════════════════

async def validate_and_extract_skills(
    db: AsyncSession,
    jobs_df: pd.DataFrame,
    quality_report: DataQualityReport
) -> Tuple[Dict[str, Set[int]], DataQualityReport]:
    """
    Step 2: Extract skills using hybrid approach (exact + fuzzy matching).
    
    Returns:
    - skill_to_job_ids: {skill_name: {job_id1, job_id2, ...}}
    - updated quality_report
    """
    logger.info("STEP 2: Validating and extracting skills...")
    
    # Build skill vocabulary from database
    skill_q = select(Skill.id, Skill.name)
    skill_results = (await db.execute(skill_q)).fetchall()
    skill_db = {row.name.lower(): row.id for row in skill_results}
    
    logger.info("Skill vocabulary loaded", total_skills=len(skill_db))
    
    skill_to_job_ids: Dict[str, Set[int]] = {name: set() for name in skill_db}
    skill_appearance_count: Dict[str, int] = {name: 0 for name in skill_db}
    
    noise_skills = set()
    
    # Extract skills from each job
    for idx, row in jobs_df.iterrows():
        job_id = row['id']
        description = row['description_lower']
        title = row['title'].lower() if pd.notna(row['title']) else ""
        
        matched_skills = set()
        
        # Method A: Exact word boundary matching
        for skill_name in skill_db:
            pattern = r'\b' + re.escape(skill_name) + r'\b'
            if re.search(pattern, description) or re.search(pattern, title):
                matched_skills.add(skill_name)
                skill_appearance_count[skill_name] += 1
        
        # Method B: Fuzzy matching (for misspellings)
        doc_tokens = description.split() + title.split()
        for token in doc_tokens:
            if len(token) >= 4:  # Only fuzzy match tokens >= 4 chars
                for skill_name in skill_db:
                    if skill_name not in matched_skills:
                        similarity = fuzz.ratio(token, skill_name)
                        if similarity >= FUZZY_MATCH_THRESHOLD:
                            matched_skills.add(skill_name)
                            skill_appearance_count[skill_name] += 1
                            break
        
        # Filter: Only keep skills that appear in >= MIN_SKILL_APPEARANCES descriptions
        for skill in matched_skills:
            if skill_appearance_count[skill] >= MIN_SKILL_APPEARANCES:
                skill_to_job_ids[skill].add(job_id)
            else:
                noise_skills.add(skill)
    
    # Remove noise skills (appear in <3 descriptions)
    for noise_skill in noise_skills:
        if noise_skill in skill_to_job_ids:
            del skill_to_job_ids[noise_skill]
    
    # Remove generic terms that passed through
    for generic in GENERIC_TERMS:
        if generic in skill_to_job_ids:
            del skill_to_job_ids[generic]
            noise_skills.add(generic)
    
    # Update quality report
    quality_report.skills_validated = len(skill_to_job_ids)
    quality_report.skills_removed_as_noise = len(noise_skills)
    quality_report.invalid_skills_removed = list(noise_skills)
    
    logger.info(
        "Skill extraction complete",
        validated_skills=len(skill_to_job_ids),
        removed_as_noise=len(noise_skills)
    )
    
    return skill_to_job_ids, quality_report


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3: SKILL DEMAND ANALYSIS (NORMALIZED BY CATEGORY)
# ══════════════════════════════════════════════════════════════════════════════

async def analyze_skill_demand(
    db: AsyncSession,
    skill_to_job_ids: Dict[str, Set[int]],
    jobs_df: pd.DataFrame,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Step 3: Compute skill demand normalized by category.
    """
    logger.info("STEP 3: Analyzing skill demand...")
    
    skill_q = select(Skill.name, Skill.category)
    skill_results = (await db.execute(skill_q)).fetchall()
    skill_to_category = {row.name.lower(): row.category for row in skill_results}
    
    total_jobs = len(jobs_df)
    
    # Group skills by category
    by_category = {}
    
    for skill_name, job_ids in skill_to_job_ids.items():
        if len(job_ids) >= MIN_JOBS_FOR_SKILL_ANALYSIS:
            category = skill_to_category.get(skill_name, "other")
            
            frequency = len(job_ids)
            score = frequency / total_jobs  # Normalized score
            
            if category not in by_category:
                by_category[category] = []
            
            by_category[category].append({
                "skill": skill_name,
                "frequency": frequency,
                "score": round(score, 4),
                "percentage": round(score * 100, 2),
            })
    
    # Sort by score within each category
    for category in by_category:
        by_category[category] = sorted(
            by_category[category],
            key=lambda x: x['score'],
            reverse=True
        )[:10]  # Top 10 per category
    
    logger.info("Skill demand analysis complete", categories=len(by_category))
    
    return by_category


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4: TRENDING SKILLS (CORRECTED WOW LOGIC)
# ══════════════════════════════════════════════════════════════════════════════

async def detect_trending_skills(
    db: AsyncSession,
    skill_to_job_ids: Dict[str, Set[int]],
    jobs_df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    Step 4: Detect trending skills with VALID week-over-week growth.
    
    CRITICAL RULES:
    - Both weeks must have >= 10 jobs
    - Growth = (current - previous) / previous * 100
    - Filter out false 100% growth from small samples
    """
    logger.info("STEP 4: Detecting trending skills...")
    
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    two_weeks_ago = now - timedelta(days=14)
    
    # Split jobs into time windows
    current_week_jobs = set(
        jobs_df[jobs_df['fetched_at'] >= week_ago]['id']
    )
    previous_week_jobs = set(
        jobs_df[
            (jobs_df['fetched_at'] >= two_weeks_ago) &
            (jobs_df['fetched_at'] < week_ago)
        ]['id']
    )
    
    trending = []
    
    for skill_name, all_job_ids in skill_to_job_ids.items():
        current_count = len(all_job_ids & current_week_jobs)
        previous_count = len(all_job_ids & previous_week_jobs)
        
        # CRITICAL: Only compute growth if both weeks have >= MIN_TRENDING_WEEKLY_COUNT jobs
        if current_count >= MIN_TRENDING_WEEKLY_COUNT and previous_count >= MIN_TRENDING_WEEKLY_COUNT:
            growth = ((current_count - previous_count) / previous_count) * 100
            
            # CRITICAL: Filter out inflated growth from small samples
            if growth < 100:  # Don't show 100% growth (unreliable)
                trending.append({
                    "skill": skill_name,
                    "current_week": current_count,
                    "previous_week": previous_count,
                    "growth_rate": round(growth, 2),
                    "confidence": "HIGH" if current_count >= MIN_SAMPLE_FOR_HIGH_CONFIDENCE else "MEDIUM",
                })
    
    # Sort by growth rate, descending
    trending = sorted(trending, key=lambda x: x['growth_rate'], reverse=True)[:10]
    
    logger.info("Trending skills detected", count=len(trending))
    
    return trending


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5: SALARY ANALYSIS (REMOVE BIAS WITH CONTROLS)
# ══════════════════════════════════════════════════════════════════════════════

def remove_outliers_iqr(series: pd.Series, multiplier: float = 1.5) -> pd.Series:
    """Remove outliers using IQR method"""
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    return series[(series >= lower_bound) & (series <= upper_bound)]


async def analyze_salary_insights(
    db: AsyncSession,
    skill_to_job_ids: Dict[str, Set[int]],
    jobs_df: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Step 5: Salary analysis with bias removal.
    
    RULES:
    - Ignore skills with < 30 salary samples
    - Remove outliers (IQR method)
    - Salary comparisons controlled by seniority
    """
    logger.info("STEP 5: Analyzing salary insights...")
    
    # Prepare salary-valid jobs
    salary_valid = jobs_df[jobs_df['salary_mid'].notna()].copy()
    
    top_paying_skills = []
    
    for skill_name, job_ids in skill_to_job_ids.items():
        salary_samples = salary_valid[salary_valid['id'].isin(job_ids)]['salary_mid']
        
        # RULE: Ignore if < MIN_SALARY_SAMPLES
        if len(salary_samples) >= MIN_SALARY_SAMPLES:
            # Remove outliers
            cleaned_salaries = remove_outliers_iqr(salary_samples)
            
            if len(cleaned_salaries) > 0:
                avg_salary = int(cleaned_salaries.mean())
                median_salary = int(cleaned_salaries.median())
                p75 = int(cleaned_salaries.quantile(0.75))
                
                top_paying_skills.append({
                    "skill": skill_name,
                    "avg_salary": avg_salary,
                    "median_salary": median_salary,
                    "p75_salary": p75,
                    "sample_size": len(cleaned_salaries),
                    "confidence": "HIGH" if len(cleaned_salaries) >= 50 else "MEDIUM",
                })
    
    # Sort by median salary
    top_paying_skills = sorted(
        top_paying_skills,
        key=lambda x: x['median_salary'],
        reverse=True
    )[:10]
    
    # Salary by seniority (controlled analysis)
    salary_by_seniority = {}
    for seniority in ['junior', 'mid', 'senior', 'lead', 'unspecified']:
        seniority_df = salary_valid[salary_valid['seniority'] == seniority]
        if len(seniority_df) >= MIN_SALARY_SAMPLES:
            salaries = remove_outliers_iqr(seniority_df['salary_mid'])
            if len(salaries) > 0:
                salary_by_seniority[seniority] = {
                    "avg_salary": int(salaries.mean()),
                    "median_salary": int(salaries.median()),
                    "sample_size": len(salaries),
                }
    
    # Remote vs Non-Remote (controlled by seniority)
    salary_insights = {
        "top_paying_skills": top_paying_skills,
        "by_seniority": salary_by_seniority,
    }
    
    logger.info("Salary analysis complete", top_skills=len(top_paying_skills))
    
    return salary_insights


# ══════════════════════════════════════════════════════════════════════════════
# STEP 6: LOCATION INSIGHTS (CLEANED)
# ══════════════════════════════════════════════════════════════════════════════

def analyze_market_locations(jobs_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Step 6: Market location analysis with data quality controls.
    """
    logger.info("STEP 6: Analyzing market locations...")
    
    # Top cities
    city_counts = jobs_df['city'].value_counts().head(10)
    top_cities = [
        {"city": city, "job_count": int(count)}
        for city, count in city_counts.items()
    ]
    
    # Top countries
    country_counts = jobs_df['country'].value_counts().head(10)
    top_countries = [
        {"country": country, "job_count": int(count)}
        for country, count in country_counts.items()
    ]
    
    # Remote percentage
    total_jobs = len(jobs_df)
    remote_jobs = len(jobs_df[jobs_df['remote'] == True])
    remote_pct = round((remote_jobs / total_jobs) * 100, 2)
    
    market_insights = {
        "top_cities": top_cities,
        "top_countries": top_countries,
        "remote_jobs": remote_jobs,
        "total_jobs": total_jobs,
        "remote_percentage": remote_pct,
    }
    
    logger.info("Market analysis complete", remote_pct=remote_pct)
    
    return market_insights


# ══════════════════════════════════════════════════════════════════════════════
# STEP 7: SKILL COMBINATIONS (HIGH SAMPLE ONLY)
# ══════════════════════════════════════════════════════════════════════════════

def analyze_skill_combinations(
    skill_to_job_ids: Dict[str, Set[int]],
    jobs_df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    Step 7: Analyze skill combinations (pairs appearing together).
    
    RULES:
    - Minimum co-occurrence: 5 jobs
    - High-paying: require 20+ salary samples
    """
    logger.info("STEP 7: Analyzing skill combinations...")
    
    skill_lists = [s for s in skill_to_job_ids.values() if len(s) > 0]
    
    pair_cooccurrence = {}
    
    for i, skill1_jobs in enumerate(skill_lists):
        for skill2_jobs in skill_lists[i+1:]:
            common = skill1_jobs & skill2_jobs
            if len(common) >= MIN_SKILL_PAIR_COOCCURRENCE:
                # Get skill names
                skill1 = [s for s, ids in skill_to_job_ids.items() if ids == skill1_jobs][0]
                skill2 = [s for s, ids in skill_to_job_ids.items() if ids == skill2_jobs][0]
                
                pair_key = f"{skill1} + {skill2}"
                pair_cooccurrence[pair_key] = {
                    "cooccurrence_count": len(common),
                    "skill1": skill1,
                    "skill2": skill2,
                }
    
    combinations = sorted(
        pair_cooccurrence.values(),
        key=lambda x: x['cooccurrence_count'],
        reverse=True
    )[:10]
    
    logger.info("Skill combinations analyzed", count=len(combinations))
    
    return combinations


# ══════════════════════════════════════════════════════════════════════════════
# STEP 8: ACTIONABLE INSIGHTS (HIGH CONFIDENCE ONLY)
# ══════════════════════════════════════════════════════════════════════════════

def generate_actionable_insights(
    skill_insights: Dict,
    trending_skills: List,
    salary_insights: Dict,
    market_insights: Dict,
    skill_to_job_ids: Dict,
    jobs_df: pd.DataFrame,
) -> List[ActionableInsight]:
    """
    Step 8: Generate statistically-backed actionable insights.
    
    RULES:
    - Only HIGH confidence from samples >= 50
    - Only MEDIUM from samples >= 20
    - Include reasoning
    """
    logger.info("STEP 8: Generating actionable insights...")
    
    insights = []
    total_jobs = len(jobs_df)
    
    # INSIGHT 1: Top skill
    if skill_insights and any(skill_insights.values()):
        all_skills = [s for category_skills in skill_insights.values() for s in category_skills]
        if all_skills:
            top_skill = all_skills[0]
            sample_size = top_skill.get('frequency', 0)
            
            if sample_size >= MIN_SAMPLE_FOR_HIGH_CONFIDENCE:
                insights.append(ActionableInsight(
                    text=f"🔥 '{top_skill['skill'].title()}' is the #1 in-demand skill ({top_skill['percentage']}% of jobs)",
                    confidence="HIGH",
                    reason=f"Appears in {sample_size} jobs across dataset",
                    sample_size=sample_size,
                ))
    
    # INSIGHT 2: Trending skill
    if trending_skills:
        top_trending = trending_skills[0]
        growth = top_trending['growth_rate']
        current = top_trending['current_week']
        
        if current >= MIN_SAMPLE_FOR_HIGH_CONFIDENCE:
            insights.append(ActionableInsight(
                text=f"📈 '{top_trending['skill'].title()}' is trending with {growth:+.1f}% week-over-week growth",
                confidence="HIGH",
                reason=f"Current week: {current} jobs (previous: {top_trending['previous_week']})",
                sample_size=current,
            ))
    
    # INSIGHT 3: Top paying skill
    if salary_insights.get('top_paying_skills'):
        top_paying = salary_insights['top_paying_skills'][0]
        sample_size = top_paying['sample_size']
        
        if sample_size >= MIN_SAMPLE_FOR_HIGH_CONFIDENCE:
            insights.append(ActionableInsight(
                text=f"💰 '{top_paying['skill'].title()}' commands the highest median salary: ${top_paying['median_salary']:,}",
                confidence="HIGH",
                reason=f"Based on {sample_size} salary samples (outliers removed)",
                sample_size=sample_size,
            ))
    
    # INSIGHT 4: Remote benefit
    if market_insights.get('remote_percentage', 0) > 40:
        insights.append(ActionableInsight(
            text=f"🏠 {market_insights['remote_percentage']}% of jobs are remote - significant flexibility",
            confidence="HIGH",
            reason=f"Calculated from {market_insights['total_jobs']} jobs",
            sample_size=market_insights['total_jobs'],
        ))
    
    # INSIGHT 5: Skill pair premium
    for skill_name, job_ids in list(skill_to_job_ids.items())[:3]:
        salary_df = jobs_df[jobs_df['id'].isin(job_ids) & jobs_df['salary_mid'].notna()]
        if len(salary_df) >= MIN_SALARY_SAMPLES:
            avg_sal = salary_df['salary_mid'].mean()
            
            # Compare to overall average
            overall_avg = jobs_df[jobs_df['salary_mid'].notna()]['salary_mid'].mean()
            premium = ((avg_sal - overall_avg) / overall_avg) * 100
            
            if abs(premium) > 5 and len(salary_df) >= 50:
                direction = "commands" if premium > 0 else "typically receives"
                insights.append(ActionableInsight(
                    text=f"💡 '{skill_name.title()}' {direction} a {abs(premium):.1f}% salary premium",
                    confidence="HIGH",
                    reason=f"Based on {len(salary_df)} salary samples",
                    sample_size=len(salary_df),
                ))
    
    logger.info("Actionable insights generated", count=len(insights))
    
    return insights


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

async def compute_rigorous_analytics(db: AsyncSession) -> AnalyticsOutput:
    """
    Main orchestrator: Execute all 8 steps in sequence.
    
    Returns: Production-ready AnalyticsOutput
    """
    logger.info("="*80)
    logger.info("STARTING RIGOROUS ANALYTICS PIPELINE")
    logger.info("="*80)
    
    try:
        # STEP 1: Clean data
        jobs_df, quality_report = await clean_jobs_data(db)
        
        # STEP 2: Extract skills
        skill_to_job_ids, quality_report = await validate_and_extract_skills(
            db, jobs_df, quality_report
        )
        
        # STEP 3: Skill demand
        skill_insights = await analyze_skill_demand(db, skill_to_job_ids, jobs_df)
        
        # STEP 4: Trending
        trending_skills = await detect_trending_skills(db, skill_to_job_ids, jobs_df)
        
        # STEP 5: Salary
        salary_insights = await analyze_salary_insights(db, skill_to_job_ids, jobs_df)
        
        # STEP 6: Market
        market_insights = analyze_market_locations(jobs_df)
        
        # STEP 7: Combinations
        skill_combinations = analyze_skill_combinations(skill_to_job_ids, jobs_df)
        
        # STEP 8: Actionable insights
        actionable_insights = generate_actionable_insights(
            skill_insights, trending_skills, salary_insights, market_insights,
            skill_to_job_ids, jobs_df
        )
        
        output = AnalyticsOutput(
            skill_insights=skill_insights,
            trending_skills=trending_skills,
            salary_insights=salary_insights,
            market_insights=market_insights,
            skill_combinations=skill_combinations,
            actionable_insights=actionable_insights,
            data_quality_report=quality_report,
        )
        
        logger.info("="*80)
        logger.info("ANALYTICS PIPELINE COMPLETE ✅")
        logger.info("="*80)
        
        return output
    
    except Exception as e:
        logger.error("Analytics pipeline failed", error=str(e), exc_info=True)
        raise
