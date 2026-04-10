"""
Phase 4: Feature Engineering Task
Create ML-ready features with temporal and growth indicators
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict
from pathlib import Path
from prefect import task

import sys
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.logger import setup_logger
from utils.storage import PartitionedStorage

logger = setup_logger(__name__)


def get_seniority_level(title: str) -> int:
    """Infer seniority level from job title"""
    if pd.isna(title):
        return 2  # Default to mid-level
    
    title = str(title).lower()
    
    levels = {
        1: ["junior", "entry", "intern", "associate", "jr"],
        2: ["mid", "intermediate"],
        3: ["senior", "sr", "staff"],
        4: ["lead", "principal", "manager"],
        5: ["director", "head", "vp", "chief", "executive"]
    }
    
    for level, keywords in levels.items():
        if any(keyword in title for keyword in keywords):
            return level
    
    return 2  # Default


def encode_remote(description: str) -> int:
    """Detect if job is remote"""
    if pd.isna(description):
        return 0
    
    description = str(description).lower()
    remote_keywords = ["remote", "work from home", "wfh", "distributed"]
    
    return 1 if any(kw in description for kw in remote_keywords) else 0


def calculate_salary_mid(row) -> float:
    """Calculate midpoint salary"""
    try:
        min_val = row.get("salary_min")
        max_val = row.get("salary_max")
        
        if pd.isna(min_val) or pd.isna(max_val):
            return np.nan
        
        return (float(min_val) + float(max_val)) / 2
    except:
        return np.nan


def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add time-based features"""
    logger.info("Adding temporal features")
    
    if "created" not in df.columns:
        return df
    
    df["created"] = pd.to_datetime(df["created"])
    
    # Extract time components
    df["year"] = df["created"].dt.year
    df["month"] = df["created"].dt.month
    df["day_of_week"] = df["created"].dt.dayofweek
    df["week_of_year"] = df["created"].dt.isocalendar().week
    
    # Days since oldest posting (temporal ordering)
    min_date = df["created"].min()
    df["days_since_start"] = (df["created"] - min_date).dt.days
    
    return df


def add_growth_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add growth and trend indicators (requires historical data)"""
    logger.info("Adding growth features")
    
    # For now, initialize with defaults
    # In production, this would compare against historical partitions
    df["skill_demand_growth_30d"] = 0.0
    df["role_emergence_score"] = 0.0
    df["salary_growth_rate"] = 0.0
    
    # Could calculate within current batch as proxy
    if "title" in df.columns and "created" in df.columns:
        # Role frequency as emergence proxy
        role_counts = df["title"].value_counts()
        df["role_frequency"] = df["title"].map(role_counts)
        df["role_emergence_score"] = 1.0 / np.log1p(df["role_frequency"])
    
    return df


@task(
    name="build_features",
    description="Engineer ML-ready features from enriched data",
    retries=1,
    retry_delay_seconds=30,
    timeout_seconds=300
)
def build_features(run_date: datetime, skills_metadata: Dict) -> Dict:
    """
    Build feature-engineered dataset
    
    Args:
        run_date: Date of the run
        skills_metadata: Metadata from skill extraction
    
    Returns:
        Metadata dict with feature engineering statistics
    """
    logger.info(f"Starting feature engineering for {run_date.date()}")
    
    # Load skills-enriched data from silver zone
    storage = PartitionedStorage('silver')
    df = storage.read_partition(run_date, filename="jobs_with_skills.parquet")
    
    initial_count = len(df)
    logger.info(f"Loaded {initial_count} jobs with skills")
    
    # Load skill matrix
    skill_matrix_path = config.get_storage_path('gold').parent / "processed" / "skill_matrix.parquet"
    
    if skill_matrix_path.exists():
        skill_matrix_df = pd.read_parquet(skill_matrix_path)
        if "job_id" not in skill_matrix_df.columns:
            logger.warning("Skill matrix missing job_id column, recreating empty")
            skill_matrix_df = pd.DataFrame({"job_id": df["job_id"]})
        else:
            logger.info(f"Loaded skill matrix with {len(skill_matrix_df.columns) - 1} skills")
    else:
        logger.warning("Skill matrix not found, skipping skill features")
        skill_matrix_df = pd.DataFrame({"job_id": df["job_id"]})
    
    # Basic features
    logger.info("Creating basic features")
    df["skill_count"] = df["extracted_skills"].apply(lambda x: len(x) if isinstance(x, list) else 0)
    df["salary_mid"] = df.apply(calculate_salary_mid, axis=1)
    df["desc_length"] = df["description"].apply(lambda x: len(str(x).split()))
    df["title_length"] = df["title"].apply(lambda x: len(str(x).split()))
    df["seniority_score"] = df["title"].apply(get_seniority_level)
    df["is_remote"] = df["description"].apply(encode_remote)
    
    # Temporal features
    df = add_temporal_features(df)
    
    # Growth features
    df = add_growth_features(df)
    
    # Merge with skill matrix
    # Prefix skill columns to avoid collisions with text columns (e.g., "description" as a skill)
    new_cols = []
    for col in skill_matrix_df.columns:
        if col == "job_id":
            new_cols.append(col)
        else:
            new_cols.append(f"skill_{col}")
    skill_matrix_df.columns = new_cols

    features_df = df.merge(skill_matrix_df, on="job_id", how="left")
    
    # Fill only skill columns with 0 (leave categorical cols alone)
    skill_cols = [c for c in skill_matrix_df.columns if c != "job_id"]
    if skill_cols:
        features_df[skill_cols] = features_df[skill_cols].fillna(0)
    
    # Save to gold zone (partitioned)
    gold_storage = PartitionedStorage('gold')
    file_path = gold_storage.write(features_df, run_date, filename="features.parquet")
    
    # Feature statistics
    numeric_features = features_df.select_dtypes(include=[np.number]).columns.tolist()
    
    metadata = {
        "run_date": run_date.isoformat(),
        "phase": "feature_engineering",
        "total_records": len(features_df),
        "total_features": len(features_df.columns),
        "numeric_features": len(numeric_features),
        "feature_names": features_df.columns.tolist()[:50],  # First 50
        "file_path": str(file_path),
        "timestamp": datetime.utcnow().isoformat()
    }
    
    logger.info(
        f"✅ Feature engineering complete: {len(features_df)} records, "
        f"{len(features_df.columns)} features saved to {file_path}"
    )
    
    return metadata


if __name__ == "__main__":
    # For standalone testing
    from datetime import datetime
    import json
    
    mock_metadata = {
        "run_date": datetime.now().isoformat(),
        "unique_skills": 100
    }
    
    result = build_features(datetime.now(), mock_metadata)
    print(json.dumps(result, indent=2))
