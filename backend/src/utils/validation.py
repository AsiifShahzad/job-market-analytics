"""
Schema validation and data quality checks
Uses Pydantic for schema validation and custom quality metrics
"""
from datetime import datetime
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, validator
import pandas as pd
from .logger import setup_logger
from .config import config

logger = setup_logger(__name__)


# Schema Models

class JobRecord(BaseModel):
    """Schema for a job posting record"""
    job_id: str = Field(..., description="Unique job identifier")
    title: str = Field(..., min_length=1, description="Job title")
    description: str = Field(..., min_length=10, description="Job description")
    company: Optional[str] = Field(None, description="Company name")
    location: Optional[str] = Field(None, description="Job location")
    salary_min: Optional[float] = Field(None, ge=0, description="Minimum salary")
    salary_max: Optional[float] = Field(None, ge=0, description="Maximum salary")
    contract_type: Optional[str] = Field(None, description="Contract type")
    created: datetime = Field(..., description="Job created date")
    redirect_url: Optional[str] = Field(None, description="Job URL")
    category: Optional[str] = Field(None, description="Job category")
    
    @validator('salary_max')
    def salary_max_greater_than_min(cls, v, values):
        """Ensure salary_max >= salary_min"""
        if v is not None and 'salary_min' in values and values['salary_min'] is not None:
            if v < values['salary_min']:
                raise ValueError('salary_max must be >= salary_min')
        return v
    
    class Config:
        extra = 'allow'  # Allow extra fields


class DataQualityMetrics(BaseModel):
    """Data quality metrics for a dataset"""
    total_records: int
    null_rates: Dict[str, float]
    duplicate_count: int
    duplicate_rate: float
    outlier_count: int = 0
    outlier_rate: float = 0.0
    schema_violations: int = 0
    timestamp: datetime = Field(default_factory=datetime.utcnow)


# Validation Functions

def validate_schema(df: pd.DataFrame, raise_on_error: bool = False) -> Dict[str, Any]:
    """
    Validate DataFrame against JobRecord schema
    
    Args:
        df: DataFrame to validate
        raise_on_error: If True, raise exception on validation errors
    
    Returns:
        Validation results dict
    """
    violations = []
    valid_count = 0
    
    for idx, row in df.iterrows():
        try:
            JobRecord(**row.to_dict())
            valid_count += 1
        except Exception as e:
            violations.append({
                'row': idx,
                'error': str(e)
            })
    
    results = {
        'total_records': len(df),
        'valid_records': valid_count,
        'violations': len(violations),
        'violation_rate': len(violations) / len(df) if len(df) > 0 else 0,
        'violation_details': violations[:10]  # First 10 violations
    }
    
    if raise_on_error and violations:
        raise ValueError(f"Schema validation failed: {len(violations)} violations")
    
    logger.info(
        f"Schema validation: {valid_count}/{len(df)} valid records, "
        f"{len(violations)} violations"
    )
    
    return results


def calculate_quality_metrics(df: pd.DataFrame) -> DataQualityMetrics:
    """
    Calculate comprehensive data quality metrics
    
    Args:
        df: DataFrame to analyze
    
    Returns:
        DataQualityMetrics object
    """
    # Null rates
    null_rates = {
        col: df[col].isna().sum() / len(df)
        for col in df.columns
    }
    
    # Duplicates
    duplicates = df.duplicated(subset=['job_id']) if 'job_id' in df.columns else df.duplicated()
    duplicate_count = duplicates.sum()
    
    # Outliers (salary-based)
    outlier_count = 0
    if 'salary_max' in df.columns:
        salary_values = df['salary_max'].dropna()
        if len(salary_values) > 0:
            Q1 = salary_values.quantile(0.25)
            Q3 = salary_values.quantile(0.75)
            IQR = Q3 - Q1
            outliers = (salary_values < (Q1 - 3 * IQR)) | (salary_values > (Q3 + 3 * IQR))
            outlier_count = outliers.sum()
    
    metrics = DataQualityMetrics(
        total_records=len(df),
        null_rates=null_rates,
        duplicate_count=int(duplicate_count),
        duplicate_rate=float(duplicate_count / len(df)) if len(df) > 0 else 0.0,
        outlier_count=int(outlier_count),
        outlier_rate=float(outlier_count / len(df)) if len(df) > 0 else 0.0
    )
    
    logger.info(f"Quality metrics: {metrics.dict()}")
    
    return metrics


def check_quality_thresholds(metrics: DataQualityMetrics) -> bool:
    """
    Check if quality metrics meet configured thresholds
    
    Args:
        metrics: Quality metrics to check
    
    Returns:
        True if all thresholds pass, False otherwise
    """
    min_records = config.get('quality.min_records_per_run', 100)
    max_null_rate = config.get('quality.max_null_rate', 0.3)
    max_duplicate_rate = config.get('quality.max_duplicate_rate', 0.1)
    
    issues = []
    
    if metrics.total_records < min_records:
        issues.append(f"Insufficient records: {metrics.total_records} < {min_records}")
    
    # Check critical fields for null rates
    critical_fields = ['title', 'description']
    for field in critical_fields:
        if field in metrics.null_rates and metrics.null_rates[field] > max_null_rate:
            issues.append(
                f"High null rate in {field}: {metrics.null_rates[field]:.2%} > {max_null_rate:.2%}"
            )
    
    if metrics.duplicate_rate > max_duplicate_rate:
        issues.append(
            f"High duplicate rate: {metrics.duplicate_rate:.2%} > {max_duplicate_rate:.2%}"
        )
    
    if issues:
        logger.warning(f"Quality threshold violations: {issues}")
        return False
    
    logger.info("All quality thresholds passed")
    return True


def validate_incremental_update(
    new_df: pd.DataFrame,
    previous_df: Optional[pd.DataFrame] = None
) -> Dict[str, Any]:
    """
    Validate incremental update integrity
    
    Args:
        new_df: New data to validate
        previous_df: Previous data for comparison
    
    Returns:
        Validation results
    """
    results = {
        'new_records': len(new_df),
        'is_valid': True,
        'issues': []
    }
    
    if previous_df is not None and len(previous_df) > 0:
        # Check for temporal consistency
        if 'created' in new_df.columns and 'created' in previous_df.columns:
            new_df['created'] = pd.to_datetime(new_df['created'])
            previous_df['created'] = pd.to_datetime(previous_df['created'])
            
            new_min_date = new_df['created'].min()
            prev_max_date = previous_df['created'].max()
            
            # New data should generally be after previous data
            if new_min_date < prev_max_date:
                results['issues'].append(
                    f"Temporal inconsistency: new data starts at {new_min_date}, "
                    f"previous data ends at {prev_max_date}"
                )
        
        # Check for schema consistency
        new_cols = set(new_df.columns)
        prev_cols = set(previous_df.columns)
        
        if new_cols != prev_cols:
            missing = prev_cols - new_cols
            extra = new_cols - prev_cols
            
            if missing:
                results['issues'].append(f"Missing columns: {missing}")
            if extra:
                results['issues'].append(f"Extra columns: {extra}")
    
    results['is_valid'] = len(results['issues']) == 0
    
    if not results['is_valid']:
        logger.warning(f"Incremental update validation issues: {results['issues']}")
    
    return results
