"""
Phase 2: Data Cleaning Task
Validates, cleans, and standardizes raw job data with schema validation
"""
import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Optional
from pathlib import Path
from prefect import task

import sys
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.logger import setup_logger
from utils.storage import PartitionedStorage, incremental_merge
from utils.validation import validate_schema, calculate_quality_metrics, check_quality_thresholds

logger = setup_logger(__name__)


def load_raw_jobs(run_date: datetime) -> pd.DataFrame:
    """Load raw jobs from partitioned storage"""
    storage = PartitionedStorage('raw')
    
    try:
        data = storage.read_partition(run_date)
        
        # Handle dict, list, and DataFrame formats
        if isinstance(data, pd.DataFrame):
            # Already a DataFrame (from updated storage.py)
            return data
        elif isinstance(data, dict):
            jobs = data.get('jobs', [])
            return pd.DataFrame(jobs)
        elif isinstance(data, list):
            jobs = data
            return pd.DataFrame(jobs)
        else:
            raise ValueError(f"Unexpected data format: {type(data)}")
        
    except FileNotFoundError:
        logger.error(f"No raw data found for {run_date.date()}")
        raise


def normalize_text(text):
    """Normalize text fields"""
    if pd.isna(text):
        return None
    return str(text).lower().strip()


def flatten_jobs(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Flatten nested JSON structure into tabular format"""
    flattened = []
    
    for _, job in raw_df.iterrows():
        record = {
            "job_id": job.get("id"),
            "title": job.get("title"),
            "description": job.get("description"),
            "company": job.get("company", {}).get("display_name") if isinstance(job.get("company"), dict) else job.get("company"),
            "location": job.get("location", {}).get("display_name") if isinstance(job.get("location"), dict) else job.get("location"),
            "salary_min": job.get("salary_min"),
            "salary_max": job.get("salary_max"),
            "contract_type": job.get("contract_type"),
            "created": job.get("created"),
            "redirect_url": job.get("redirect_url"),
            "category": job.get("category", {}).get("label") if isinstance(job.get("category"), dict) else job.get("category"),
        }
        flattened.append(record)
    
    df = pd.DataFrame(flattened)
    
    if df.empty:
        raise ValueError("Flattening failed: DataFrame is empty")
    
    logger.info(f"Flattened {len(df)} job records")
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply cleaning transformations"""
    logger.info("Applying data cleaning transformations")
    
    # Ensure expected columns exist
    expected_cols = ["title", "description", "company", "location", "contract_type", "category"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    
    # Normalize text fields
    text_cols = ["title", "company", "location", "contract_type", "category"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].apply(normalize_text)
    
    # Drop records missing critical fields
    initial_count = len(df)
    df = df.dropna(subset=["title", "description"])
    dropped = initial_count - len(df)
    if dropped > 0:
        logger.warning(f"Dropped {dropped} records with missing title/description")
    
    # Fix salary inversions
    if "salary_min" in df.columns and "salary_max" in df.columns:
        mask = (df["salary_min"] > df["salary_max"]) & df["salary_max"].notna()
        if mask.sum() > 0:
            logger.info(f"Fixed {mask.sum()} salary inversions")
            df.loc[mask, ["salary_min", "salary_max"]] = df.loc[mask, ["salary_max", "salary_min"]].values
    
    # Parse dates
    if "created" in df.columns:
        df["created"] = pd.to_datetime(df["created"], errors="coerce")
        invalid_dates = df["created"].isna().sum()
        if invalid_dates > 0:
            logger.warning(f"{invalid_dates} records have invalid dates")
    
    logger.info(f"Cleaning complete: {len(df)} records")
    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate records"""
    before = len(df)
    
    if "job_id" in df.columns:
        df = df.drop_duplicates(subset=["job_id"])
    else:
        # Fallback: composite key
        df["dedup_key"] = df["title"] + df["company"].fillna("") + df["location"].fillna("")
        df = df.drop_duplicates(subset=["dedup_key"])
        df = df.drop(columns=["dedup_key"])
    
    after = len(df)
    removed = before - after
    
    if removed > 0:
        logger.info(f"Removed {removed} duplicates ({removed/before:.1%})")
    
    return df


@task(
    name="clean_and_validate_jobs",
    description="Clean, validate, and standardize job data with quality checks",
    retries=2,
    retry_delay_seconds=30,
    timeout_seconds=300
)
def clean_jobs(run_date: datetime, ingestion_metadata: Dict) -> Dict:
    """
    Clean and validate job data
    
    Args:
        run_date: Date of the run
        ingestion_metadata: Metadata from ingestion task
    
    Returns:
        Metadata dict with cleaning statistics
    """
    logger.info(f"Starting data cleaning for {run_date.date()}")
    
    # Load raw data
    raw_df = load_raw_jobs(run_date)
    initial_count = len(raw_df)
    
    # Flatten nested structure
    df = flatten_jobs(raw_df)
    
    # Clean data
    df = clean_dataframe(df)
    
    # Deduplicate
    df = deduplicate(df)
    
    # Schema validation
    validation_results = validate_schema(df, raise_on_error=False)
    
    # Quality metrics
    quality_metrics = calculate_quality_metrics(df)
    
    # Check quality thresholds
    quality_passed = check_quality_thresholds(quality_metrics)
    
    if not quality_passed:
        logger.warning("Data quality below thresholds, but continuing pipeline")
    
    # Save to bronze zone (partitioned Parquet)
    storage = PartitionedStorage('bronze')
    file_path = storage.write(df, run_date)
    
    # Create metadata
    metadata = {
        "run_date": run_date.isoformat(),
        "phase": "cleaning",
        "initial_records": initial_count,
        "final_records": len(df),
        "records_dropped": initial_count - len(df),
        "validation_violations": validation_results['violations'],
        "quality_metrics": quality_metrics.dict(),
        "quality_passed": quality_passed,
        "file_path": str(file_path),
        "timestamp": datetime.utcnow().isoformat()
    }
    
    logger.info(
        f"✅ Cleaning complete: {len(df)} clean records saved to {file_path}"
    )
    
    return metadata


if __name__ == "__main__":
    # For standalone testing
    from datetime import datetime
    
    # Mock metadata from ingestion
    mock_metadata = {
        "run_date": datetime.now().isoformat(),
        "total_jobs_collected": 1000
    }
    
    result = clean_jobs(datetime.now(), mock_metadata)
    print(json.dumps(result, indent=2))
