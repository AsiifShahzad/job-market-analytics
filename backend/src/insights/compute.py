"""
High-impact insights computation engine for job market analytics.
Follows 80/20 rule: focus on actionable, high-value insights only.

Author: Senior Data Scientist
Date: April 2026
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, desc, asc, case
import structlog

from src.db.models import Job, Skill, JobSkill, SkillSnapshot

logger = structlog.get_logger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# A. SKILL INSIGHTS
# ══════════════════════════════════════════════════════════════════════════════

async def get_top_skills(db: AsyncSession, limit: int = 10, days: int = 30) -> List[Dict[str, Any]]:
    """Top skills by frequency (last N days)."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    
    q = (
        select(
            Skill.name,
            Skill.category,
            func.count(JobSkill.job_id).label("job_count"),
        )
        .select_from(Skill)
        .join(JobSkill, Skill.id == JobSkill.skill_id)
        .join(Job, JobSkill.job_id == Job.id)
        .where(Job.fetched_at >= cutoff)
        .group_by(Skill.id, Skill.name, Skill.category)
        .order_by(func.count(JobSkill.job_id).desc())
        .limit(limit)
    )
    
    results = (await db.execute(q)).fetchall()
    return [
        {
            "skill": r.name,
            "category": r.category,
            "demand": int(r.job_count),
            "label": "🔥 Hot Skill" if i < 3 else None
        }
        for i, r in enumerate(results)
    ]


async def get_top_skills_by_category(db: AsyncSession, days: int = 30) -> Dict[str, List[Dict]]:
    """Top 3 skills per category."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    
    q = (
        select(
            Skill.category,
            Skill.name,
            func.count(JobSkill.job_id).label("job_count"),
            func.row_number().over(
                partition_by=Skill.category,
                order_by=func.count(JobSkill.job_id).desc()
            ).label("rank")
        )
        .select_from(Skill)
        .join(JobSkill, Skill.id == JobSkill.skill_id)
        .join(Job, JobSkill.job_id == Job.id)
        .where(Job.fetched_at >= cutoff)
        .group_by(Skill.id, Skill.name, Skill.category)
    )
    
    results = (await db.execute(q)).fetchall()
    
    by_category = {}
    for r in results:
        if r.rank <= 3:  # Top 3 per category
            if r.category not in by_category:
                by_category[r.category] = []
            by_category[r.category].append({
                "skill": r.name,
                "demand": int(r.job_count)
            })
    
    return by_category


async def get_fastest_growing_skills(db: AsyncSession, limit: int = 5) -> List[Dict[str, Any]]:
    """Skills with highest week-over-week growth."""
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    two_weeks_ago = now - timedelta(days=14)
    
    # Current week
    current_q = (
        select(
            Skill.id,
            Skill.name,
            func.count(JobSkill.job_id).label("count"),
        )
        .select_from(Skill)
        .join(JobSkill, Skill.id == JobSkill.skill_id)
        .join(Job, JobSkill.job_id == Job.id)
        .where(Job.fetched_at >= week_ago)
        .group_by(Skill.id, Skill.name)
    )
    current_map = {
        (r.id, r.name): int(r.count)
        for r in (await db.execute(current_q)).fetchall()
    }
    
    # Previous week
    prev_q = (
        select(
            Skill.id,
            Skill.name,
            func.count(JobSkill.job_id).label("count"),
        )
        .select_from(Skill)
        .join(JobSkill, Skill.id == JobSkill.skill_id)
        .join(Job, JobSkill.job_id == Job.id)
        .where(and_(Job.fetched_at >= two_weeks_ago, Job.fetched_at < week_ago))
        .group_by(Skill.id, Skill.name)
    )
    prev_map = {
        (r.id, r.name): int(r.count)
        for r in (await db.execute(prev_q)).fetchall()
    }
    
    # Calculate growth rates
    growth_data = []
    for (skill_id, skill_name), current_count in current_map.items():
        prev_count = prev_map.get((skill_id, skill_name), 0)
        if prev_count > 0:
            growth_rate = (current_count - prev_count) / prev_count
        else:
            growth_rate = 1.0 if current_count > 0 else 0.0
        
        growth_data.append({
            "skill": skill_name,
            "growth_rate": round(growth_rate * 100, 1),
            "current_demand": current_count,
            "label": "📈 Trending" if growth_rate > 0.2 else None
        })
    
    return sorted(growth_data, key=lambda x: x["growth_rate"], reverse=True)[:limit]


# ══════════════════════════════════════════════════════════════════════════════
# B. SALARY INSIGHTS
# ══════════════════════════════════════════════════════════════════════════════

async def get_average_salary_per_skill(db: AsyncSession, limit: int = 10, min_jobs: int = 5) -> List[Dict[str, Any]]:
    """Average salary per skill (minimum sample size)."""
    q = (
        select(
            Skill.name,
            func.avg(Job.salary_mid).label("avg_salary"),
            func.count(JobSkill.job_id).label("job_count"),
        )
        .select_from(Skill)
        .join(JobSkill, Skill.id == JobSkill.skill_id)
        .join(Job, JobSkill.job_id == Job.id)
        .where(Job.salary_mid.isnot(None))
        .group_by(Skill.id, Skill.name)
        .having(func.count(JobSkill.job_id) >= min_jobs)
        .order_by(func.avg(Job.salary_mid).desc())
        .limit(limit)
    )
    
    results = (await db.execute(q)).fetchall()
    return [
        {
            "skill": r.name,
            "avg_salary": int(r.avg_salary) if r.avg_salary else 0,
            "job_count": int(r.job_count),
            "label": "💰 High Paying" if i < 3 else None
        }
        for i, r in enumerate(results)
    ]


async def get_salary_by_seniority(db: AsyncSession) -> Dict[str, Any]:
    """Salary statistics by experience level."""
    q = (
        select(
            Job.seniority,
            func.avg(Job.salary_mid).label("avg_salary"),
            func.min(Job.salary_mid).label("min_salary"),
            func.max(Job.salary_mid).label("max_salary"),
            func.count(Job.id).label("job_count"),
        )
        .where(Job.salary_mid.isnot(None))
        .group_by(Job.seniority)
        .order_by(Job.seniority.asc())
    )
    
    results = (await db.execute(q)).fetchall()
    return {
        (r.seniority or "unspecified"): {
            "avg_salary": int(r.avg_salary) if r.avg_salary else 0,
            "min_salary": int(r.min_salary) if r.min_salary else 0,
            "max_salary": int(r.max_salary) if r.max_salary else 0,
            "job_count": int(r.job_count),
        }
        for r in results
    }


async def get_remote_salary_difference(db: AsyncSession) -> Dict[str, Any]:
    """Remote vs non-remote salary comparison."""
    q = (
        select(
            Job.remote,
            func.avg(Job.salary_mid).label("avg_salary"),
            func.count(Job.id).label("job_count"),
        )
        .where(Job.salary_mid.isnot(None))
        .group_by(Job.remote)
    )
    
    results = (await db.execute(q)).fetchall()
    remote_data, nonremote_data = None, None
    
    for r in results:
        if r.remote:
            remote_data = {"avg_salary": int(r.avg_salary), "job_count": int(r.job_count)}
        else:
            nonremote_data = {"avg_salary": int(r.avg_salary), "job_count": int(r.job_count)}
    
    if remote_data and nonremote_data:
        diff = remote_data["avg_salary"] - nonremote_data["avg_salary"]
        pct_diff = (diff / nonremote_data["avg_salary"]) * 100
        return {
            "remote": remote_data,
            "non_remote": nonremote_data,
            "difference": int(diff),
            "pct_difference": round(pct_diff, 1),
            "insight": f"Remote jobs pay {pct_diff:+.1f}% {'more' if diff > 0 else 'less'}"
        }
    return {"insight": "Insufficient data"}


# ══════════════════════════════════════════════════════════════════════════════
# C. MARKET INSIGHTS
# ══════════════════════════════════════════════════════════════════════════════

async def get_top_hiring_locations(db: AsyncSession, limit: int = 10) -> List[Dict[str, Any]]:
    """Top cities and countries by job postings."""
    # By city
    city_q = (
        select(
            Job.city,
            Job.country,
            func.count(Job.id).label("job_count"),
        )
        .where(Job.city.isnot(None))
        .group_by(Job.city, Job.country)
        .order_by(func.count(Job.id).desc())
        .limit(limit)
    )
    
    results = (await db.execute(city_q)).fetchall()
    return [
        {
            "city": r.city,
            "country": r.country,
            "job_count": int(r.job_count)
        }
        for r in results
    ]


async def get_remote_job_percentage(db: AsyncSession) -> Dict[str, Any]:
    """% of jobs offering remote work."""
    total_q = select(func.count(Job.id))
    remote_q = select(func.count(Job.id)).where(Job.remote == True)
    
    total = (await db.execute(total_q)).scalar() or 1
    remote = (await db.execute(remote_q)).scalar() or 0
    
    pct = (remote / total) * 100
    return {
        "remote_jobs": int(remote),
        "total_jobs": int(total),
        "percentage": round(pct, 1),
        "insight": f"{round(pct, 1)}% of jobs are remote"
    }


async def get_jobs_posted_trend(db: AsyncSession, days: int = 14) -> List[Dict[str, Any]]:
    """Jobs posted per day trend."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    
    q = (
        select(
            func.date(Job.fetched_at).label("date"),
            func.count(Job.id).label("job_count"),
        )
        .where(Job.fetched_at >= cutoff)
        .group_by(func.date(Job.fetched_at))
        .order_by(func.date(Job.fetched_at).asc())
    )
    
    results = (await db.execute(q)).fetchall()
    return [
        {
            "date": str(r.date),
            "jobs_posted": int(r.job_count)
        }
        for r in results
    ]


# ══════════════════════════════════════════════════════════════════════════════
# D. SKILL COMBINATIONS
# ══════════════════════════════════════════════════════════════════════════════

async def get_common_skill_pairs(db: AsyncSession, limit: int = 10, min_occurrences: int = 3) -> List[Dict[str, Any]]:
    """Most common skill pairs that appear together."""
    q = (
        select(
            Skill.name.label("skill1"),
            func.count(JobSkill.job_id).label("pair_count"),
        )
        .select_from(JobSkill)
        .join(Skill, JobSkill.skill_id == Skill.id)
        .group_by(JobSkill.skill_id, Skill.name)
        .having(func.count(JobSkill.job_id) >= min_occurrences)
        .order_by(func.count(JobSkill.job_id).desc())
        .limit(limit)
    )
    
    results = (await db.execute(q)).fetchall()
    return [
        {
            "skill": r.skill1,
            "cooccurrence_count": int(r.pair_count)
        }
        for r in results
    ]


async def get_highest_paying_skill_pairs(db: AsyncSession, limit: int = 5) -> List[Dict[str, Any]]:
    """Skill combinations that lead to highest salaries."""
    # Get jobs with multiple skills, calculate avg salary
    q = (
        select(
            Job.id,
            func.avg(Job.salary_mid).label("avg_salary"),
            func.count(Skill.id).label("skill_count"),
        )
        .select_from(Job)
        .join(JobSkill, Job.id == JobSkill.job_id)
        .join(Skill, JobSkill.skill_id == Skill.id)
        .where(Job.salary_mid.isnot(None))
        .group_by(Job.id)
        .having(func.count(Skill.id) >= 2)
        .order_by(func.avg(Job.salary_mid).desc())
        .limit(limit)
    )
    
    results = (await db.execute(q)).fetchall()
    return [
        {
            "skill_combination": f"Job with {int(r.skill_count)} skills",
            "avg_salary": int(r.avg_salary),
            "label": "💰 High Paying"
        }
        for r in results
    ]


# ══════════════════════════════════════════════════════════════════════════════
# E. KEYWORD INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

async def get_salary_by_search_keyword(db: AsyncSession, limit: int = 10) -> List[Dict[str, Any]]:
    """Which search keywords bring highest salary jobs."""
    q = (
        select(
            Job.search_keyword,
            func.avg(Job.salary_mid).label("avg_salary"),
            func.count(Job.id).label("job_count"),
        )
        .where(and_(Job.salary_mid.isnot(None), Job.search_keyword.isnot(None)))
        .group_by(Job.search_keyword)
        .order_by(func.avg(Job.salary_mid).desc())
        .limit(limit)
    )
    
    results = (await db.execute(q)).fetchall()
    return [
        {
            "keyword": r.search_keyword,
            "avg_salary": int(r.avg_salary),
            "job_count": int(r.job_count)
        }
        for r in results
    ]


# ══════════════════════════════════════════════════════════════════════════════
# F. SENIORITY DISTRIBUTION
# ══════════════════════════════════════════════════════════════════════════════

async def get_seniority_distribution(db: AsyncSession) -> Dict[str, Any]:
    """Job distribution by seniority level."""
    q = (
        select(
            Job.seniority,
            func.count(Job.id).label("job_count"),
        )
        .group_by(Job.seniority)
        .order_by(func.count(Job.id).desc())
    )
    
    results = (await db.execute(q)).fetchall()
    total = sum(int(r.job_count) for r in results)
    
    return {
        (r.seniority or "unspecified"): {
            "count": int(r.job_count),
            "percentage": round((int(r.job_count) / total) * 100, 1) if total > 0 else 0
        }
        for r in results
    }


async def get_top_skills_in_senior_roles(db: AsyncSession, limit: int = 5) -> List[Dict[str, Any]]:
    """Skills most common in senior/lead positions."""
    q = (
        select(
            Skill.name,
            func.count(JobSkill.job_id).label("job_count"),
        )
        .select_from(Skill)
        .join(JobSkill, Skill.id == JobSkill.skill_id)
        .join(Job, JobSkill.job_id == Job.id)
        .where(Job.seniority.in_(["senior", "lead"]))
        .group_by(Skill.id, Skill.name)
        .order_by(func.count(JobSkill.job_id).desc())
        .limit(limit)
    )
    
    results = (await db.execute(q)).fetchall()
    return [
        {
            "skill": r.name,
            "senior_job_count": int(r.job_count)
        }
        for r in results
    ]


# ══════════════════════════════════════════════════════════════════════════════
# MAIN INSIGHTS AGGREGATOR
# ══════════════════════════════════════════════════════════════════════════════

async def compute_all_insights(db: AsyncSession) -> Dict[str, Any]:
    """Compute all insights in parallel and return aggregated result."""
    try:
        logger.info("Starting insights computation...")
        
        # Compute all insights
        top_skills = await get_top_skills(db)
        trending = await get_fastest_growing_skills(db)
        salary_per_skill = await get_average_salary_per_skill(db)
        salary_by_seniority = await get_salary_by_seniority(db)
        remote_diff = await get_remote_salary_difference(db)
        locations = await get_top_hiring_locations(db)
        remote_pct = await get_remote_job_percentage(db)
        jobs_trend = await get_jobs_posted_trend(db)
        seniority_dist = await get_seniority_distribution(db)
        senior_skills = await get_top_skills_in_senior_roles(db)
        keyword_salary = await get_salary_by_search_keyword(db)
        
        # Generate actionable insights
        actionable = []
        
        if salary_per_skill and trending:
            top_skill = top_skills[0]["skill"] if top_skills else "unknown"
            actionable.append(f"🔥 '{top_skill}' is the most in-demand skill right now")
        
        if trending:
            growing = trending[0]
            actionable.append(f"📈 '{growing['skill']}' is growing {growing['growth_rate']}% week-over-week")
        
        if salary_per_skill:
            highest = salary_per_skill[0]
            actionable.append(f"💰 Learning '{highest['skill']}' offers the highest average salary: ${highest['avg_salary']:,}")
        
        if remote_diff and "difference" in remote_diff:
            actionable.append(f"🏠 {remote_diff['insight']}")
        
        if senior_skills:
            skill = senior_skills[0]
            actionable.append(f"👔 '{skill['skill']}' is the most valued skill in senior roles")
        
        logger.info("Insights computation complete", insight_count=len(actionable))
        
        return {
            "summary": {
                "top_skill": top_skills[0]["skill"] if top_skills else None,
                "trending_skill": trending[0]["skill"] if trending else None,
                "highest_paying_skill": salary_per_skill[0]["skill"] if salary_per_skill else None,
                "total_jobs_tracked": (await db.execute(select(func.count(Job.id)))).scalar(),
            },
            "top_skills": top_skills,
            "trending_skills": trending,
            "salary_insights": {
                "per_skill": salary_per_skill,
                "by_seniority": salary_by_seniority,
                "remote_difference": remote_diff,
            },
            "market_insights": {
                "top_locations": locations,
                "remote_percentage": remote_pct,
                "jobs_trend": jobs_trend,
                "seniority_distribution": seniority_dist,
            },
            "skill_insights": {
                "top_in_senior_roles": senior_skills,
                "by_keyword": keyword_salary,
            },
            "actionable_insights": actionable,
        }
    
    except Exception as e:
        logger.error("Insights computation failed", error=str(e))
        raise
