"""
Phase 5: Intelligence Layer Task
Market intelligence analytics including trends, career paths, and dynamics
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict
from pathlib import Path
from collections import defaultdict
from prefect import task

import sys
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.logger import setup_logger
from utils.storage import PartitionedStorage

logger = setup_logger(__name__)


def compute_skill_trends(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate skill demand trends"""
    logger.info("Computing skill trends")
    
    # Get all skill columns
    skill_cols = [c for c in df.columns if c not in [
        'job_id', 'title', 'description', 'company', 'location', 'created',
        'salary_min', 'salary_max', 'contract_type', 'redirect_url', 'category',
        'combined_text', 'layer1_skills', 'extracted_skills', 'skill_count',
        'salary_mid', 'desc_length', 'title_length', 'seniority_score', 'is_remote',
        'year', 'month', 'day_of_week', 'week_of_year', 'days_since_start',
        'skill_demand_growth_30d', 'role_emergence_score', 'salary_growth_rate',
        'role_frequency'
    ]]
    
    if not skill_cols:
        logger.warning("No skill columns found")
        return pd.DataFrame(columns=["skill", "demand"])
    
    # Calculate total demand per skill
    trends = df[skill_cols].sum().reset_index()
    trends.columns = ["skill", "demand"]
    trends = trends.sort_values("demand", ascending=False)
    
    logger.info(f"Computed trends for {len(trends)} skills")
    return trends


def compute_emerging_declining_skills(df: pd.DataFrame) -> tuple:
    """Identify emerging and declining skills based on temporal trends"""
    logger.info("Computing emerging/declining skills")
    if "created" not in df.columns:
        logger.warning("No 'created' column for temporal analysis")
        return pd.DataFrame(), pd.DataFrame()

    # Create month period (keep as Period for correct ordering)
    df["month_period"] = pd.to_datetime(df["created"]).dt.to_period("M")

    # Explode skills and compute counts per month per skill in a memory-friendly way
    skill_time = (
        df.explode("extracted_skills")
        .dropna(subset=["extracted_skills"])  # avoid NaNs
        .groupby(["month_period", "extracted_skills"])
        .size()
        .reset_index(name="count")
    )

    # If insufficient periods or data, bail out early
    if skill_time["month_period"].nunique() < 2:
        logger.warning("Insufficient time periods for trend analysis")
        return pd.DataFrame(), pd.DataFrame()

    # Compute a simple growth metric per skill without pivoting (streaming/grouped)
    growth_rows = []
    for skill, grp in skill_time.groupby("extracted_skills"):
        grp_sorted = grp.sort_values("month_period")
        counts = grp_sorted["count"].values
        if len(counts) < 2:
            continue
        # mean of month-to-month differences as growth proxy
        growth_rate = float(np.diff(counts).mean())
        growth_rows.append((skill, growth_rate))

    if not growth_rows:
        logger.info("No skill time-series found for growth computation")
        return pd.DataFrame(), pd.DataFrame()

    growth_df = pd.DataFrame(growth_rows, columns=["skill", "growth_rate"]).sort_values("growth_rate", ascending=False)

    emerging = growth_df[growth_df["growth_rate"] > 0].head(15).reset_index(drop=True)
    declining = growth_df[growth_df["growth_rate"] < 0].tail(15).reset_index(drop=True)

    logger.info(f"Found {len(emerging)} emerging, {len(declining)} declining skills")
    return emerging, declining


def infer_career_paths(df: pd.DataFrame) -> pd.DataFrame:
    """Map career paths based on skills"""
    logger.info("Inferring career paths")
    
    if "extracted_skills" not in df.columns or "title" not in df.columns:
        logger.warning("Missing required columns for career path inference")
        return pd.DataFrame(columns=["role", "core_skills", "avg_salary", "count"])
    
    # Group by role
    role_data = []
    
    for title in df["title"].unique():
        if pd.isna(title):
            continue
        
        role_df = df[df["title"] == title]
        
        # Get top skills for this role
        all_role_skills = []
        for skills in role_df["extracted_skills"]:
            if isinstance(skills, list):
                all_role_skills.extend(skills)
        
        if not all_role_skills:
            continue
        
        skill_freq = pd.Series(all_role_skills).value_counts().head(8)
        
        role_data.append({
            "role": title,
            "core_skills": ", ".join(skill_freq.index.tolist()),
            "avg_salary": role_df["salary_mid"].mean() if "salary_mid" in role_df.columns else np.nan,
            "count": len(role_df),
            "avg_seniority": role_df["seniority_score"].mean() if "seniority_score" in role_df.columns else np.nan
        })
    
    if not role_data:
        return pd.DataFrame(columns=["role", "core_skills", "avg_salary", "count", "avg_seniority"])

    career_df = pd.DataFrame(role_data)
    career_df = career_df.sort_values("count", ascending=False)
    
    logger.info(f"Mapped {len(career_df)} career paths")
    return career_df


def calculate_salary_trends(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate salary trends by role and time"""
    logger.info("Calculating salary trends")
    
    if "salary_mid" not in df.columns or "created" not in df.columns:
        logger.warning("Missing required columns for salary trends")
        return pd.DataFrame()
    
    df["month"] = pd.to_datetime(df["created"]).dt.to_period("M").astype(str)
    
    salary_trends = (
        df.groupby(["month", "title"])["salary_mid"]
        .mean()
        .reset_index()
        .sort_values("month")
    )
    
    return salary_trends


def calculate_geo_trends(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate geographic hiring trends"""
    logger.info("Calculating geographic trends")
    
    if "location" not in df.columns:
        logger.warning("No location column for geo trends")
        return pd.DataFrame()
    
    geo_counts = df["location"].value_counts().reset_index()
    geo_counts.columns = ["location", "job_count"]
    
    # Add remote percentage by location
    if "is_remote" in df.columns:
        remote_by_location = df.groupby("location")["is_remote"].mean().reset_index()
        remote_by_location.columns = ["location", "remote_rate"]
        geo_counts = geo_counts.merge(remote_by_location, on="location", how="left")
    
    return geo_counts


@task(
    name="compute_intelligence",
    description="Generate market intelligence analytics and insights",
    retries=1,
    retry_delay_seconds=30,
    timeout_seconds=300
)
def compute_intelligence(run_date: datetime, features_metadata: Dict) -> Dict:
    """
    Compute market intelligence analytics
    
    Args:
        run_date: Date of the run
        features_metadata: Metadata from feature engineering
    
    Returns:
        Metadata dict with intelligence statistics
    """
    logger.info(f"Starting intelligence computation for {run_date.date()}")
    
    # Load feature-engineered data from gold zone
    storage = PartitionedStorage('gold')
    df = storage.read_partition(run_date, filename="features.parquet")
    
    logger.info(f"Loaded {len(df)} feature records")
    
    # Create analytics directory
    analytics_path = Path(config.get_storage_path('analytics'))
    analytics_path.mkdir(parents=True, exist_ok=True)
    
    # 1. Skill trends
    skill_trends = compute_skill_trends(df)
    skill_trends.to_parquet(analytics_path / "skill_trends.parquet", index=False)
    
    # 2. Emerging/declining skills
    emerging, declining = compute_emerging_declining_skills(df)
    if not emerging.empty:
        emerging.to_parquet(analytics_path / "emerging_skills.parquet", index=False)
    if not declining.empty:
        declining.to_parquet(analytics_path / "declining_skills.parquet", index=False)
    
    # 3. Career paths
    career_paths = infer_career_paths(df)
    career_paths.to_parquet(analytics_path / "career_paths.parquet", index=False)
    
    # 4. Salary trends
    salary_trends = calculate_salary_trends(df)
    if not salary_trends.empty:
        salary_trends.to_parquet(analytics_path / "salary_trends.parquet", index=False)
    
    # 5. Geographic trends
    geo_trends = calculate_geo_trends(df)
    if not geo_trends.empty:
        geo_trends.to_parquet(analytics_path / "geo_trends.parquet", index=False)
    
    # Create metadata
    metadata = {
        "run_date": run_date.isoformat(),
        "phase": "intelligence",
        "total_records_analyzed": len(df),
        "skill_trends_count": len(skill_trends),
        "emerging_skills_count": len(emerging) if not emerging.empty else 0,
        "declining_skills_count": len(declining) if not declining.empty else 0,
        "career_paths_count": len(career_paths),
        "analytics_path": str(analytics_path),
        "timestamp": datetime.utcnow().isoformat()
    }
    
    logger.info(
        f"✅ Intelligence computation complete: {len(skill_trends)} skill trends, "
        f"{len(career_paths)} career paths saved to {analytics_path}"
    )
    
    return metadata


if __name__ == "__main__":
    # For standalone testing
    from datetime import datetime
    import json
    
    mock_metadata = {
        "run_date": datetime.now().isoformat(),
        "total_features": 100
    }
    
    result = compute_intelligence(datetime.now(), mock_metadata)
    print(json.dumps(result, indent=2))
