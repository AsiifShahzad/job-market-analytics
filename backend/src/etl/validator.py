import pandas as pd
import logging
from typing import Optional

# Configure module logger
logger = logging.getLogger(__name__)

def clean_and_validate_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates and cleans job data before insertion into PostgreSQL.
    
    Requirements addressed:
    - Drops rows with empty descriptions
    - Ensures salary_min <= salary_max (swaps them if they are reversed, or drops if invalid)
    - Normalizes city and country fields (title case, strips whitespace)
    - Ensures text encoding is clean (removes invalid/unprintable characters)
    
    Returns:
        pd.DataFrame: Cleaned dataframe ready for DB insertion.
    """
    if df.empty:
        logger.warning("Input dataframe is empty. Nothing to clean.")
        return df

    original_count = len(df)
    logger.info(f"Starting data validation on {original_count} rows.")

    # 1. Ensure text encoding is clean (remove non-printable characters)
    # This regex removes control characters except newline and carriage return
    text_cols = df.select_dtypes(include=['object', 'string']).columns
    for col in text_cols:
        df[col] = df[col].astype(str).str.replace(r'[^\x20-\x7e\n\r]', '', regex=True)
        # Convert "nan" or "None" strings back to actual NaN
        df[col] = df[col].replace({'nan': pd.NA, 'None': pd.NA, '': pd.NA})

    # 2. Drop rows with empty descriptions
    df = df.dropna(subset=['description'])
    
    # 3. Handle salary logic
    # Ensure numerical types for salaries
    if 'salary_min' in df.columns and 'salary_max' in df.columns:
        df['salary_min'] = pd.to_numeric(df['salary_min'], errors='coerce')
        df['salary_max'] = pd.to_numeric(df['salary_max'], errors='coerce')
        
        # Swap misordered max and min salaries instead of dropping them
        mask = (df['salary_min'] > df['salary_max']) & df['salary_min'].notna() & df['salary_max'].notna()
        df.loc[mask, ['salary_min', 'salary_max']] = df.loc[mask, ['salary_max', 'salary_min']].values

    # 4. Normalize city and country fields
    if 'city' in df.columns:
        df['city'] = df['city'].astype(str).str.strip().str.title().replace('Nan', pd.NA)
    if 'country' in df.columns:
        df['country'] = df['country'].astype(str).str.strip().str.upper().replace('NAN', pd.NA)
        
    # Final count and logging
    final_count = len(df)
    dropped_count = original_count - final_count
    
    logger.info(
        f"Validation complete. "
        f"Dropped {dropped_count} rows ({dropped_count/original_count:.1%} of data). "
        f"Returning {final_count} clean rows ready for insertion."
    )
    
    return df
