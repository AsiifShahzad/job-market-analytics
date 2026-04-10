"""
Phase 1: Job Ingestion Task
Fetches job data from Adzuna API with fault tolerance and partitioned storage
"""
import os
import json
import time
from datetime import datetime
from typing import Dict, List
import requests
from prefect import task
from prefect.tasks import task_input_hash
from datetime import timedelta

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.logger import setup_logger
from utils.storage import PartitionedStorage
from utils.validation import calculate_quality_metrics

logger = setup_logger(__name__)


@task(
    name="ingest_jobs_from_api",
    description="Fetch job postings from Adzuna API with pagination and rate limiting",
    retries=3,
    retry_delay_seconds=60,
    timeout_seconds=600,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1)
)
def ingest_jobs(run_date: datetime, incremental: bool = True) -> Dict:

    logger.info(f"Starting job ingestion for {run_date.date()}")
    
    # FORCE reload .env explicitly here to ensure fresh load
    from dotenv import load_dotenv
    from pathlib import Path as EnvPath
    env_file = EnvPath(__file__).parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_file, override=True)
    
    # Now get credentials from environment directly as fallback
    app_id = os.getenv("APP_ID") or config.get('api.adzuna.app_id')
    api_key = os.getenv("API_KEY") or config.get('api.adzuna.api_key')
    
    if not app_id or not api_key:
        error_msg = (
            f"API credentials not found!\n"
            f"  .env path tried: {env_file}\n"
            f"  .env exists: {env_file.exists()}\n"
            f"  APP_ID from env: {'SET' if os.getenv('APP_ID') else 'NOT SET'}\n"
            f"  API_KEY from env: {'SET' if os.getenv('API_KEY') else 'NOT SET'}\n"
            f"  Config app_id: {config.get('api.adzuna.app_id')}\n"
            f"  Config api_key: {config.get('api.adzuna.api_key')}"
        )
        raise ValueError(error_msg)
    
    credentials = {'app_id': app_id, 'api_key': api_key}
    
    # Get API configuration
    base_url = config.get('api.adzuna.base_url')
    country = config.get('api.adzuna.country')
    results_per_page = config.get('api.adzuna.results_per_page')
    max_pages = config.get('api.adzuna.max_pages')
    rate_limit_delay = config.get('api.adzuna.rate_limit_delay')
    timeout = config.get('api.adzuna.timeout')
    
    # Initialize storage
    storage = PartitionedStorage('raw')
    
    all_jobs = []
    page = 1
    api_calls = 0
    errors = []
    start_time = time.time()
    
    logger.info(f"Fetching up to {max_pages} pages from Adzuna API")
    
    while page <= max_pages:
        try:
            logger.debug(f"Fetching page {page}/{max_pages}")
            
            url = f"{base_url}/{country}/search/{page}"
            params = {
                "app_id": credentials['app_id'],
                "app_key": credentials['api_key'],
                "results_per_page": results_per_page,
                "what": "software data ai",
                "content-type": "application/json"
            }
            
            response = requests.get(url, params=params, timeout=timeout)
            api_calls += 1
            
            if response.status_code != 200:
                error_msg = f"API returned status {response.status_code} on page {page}"
                logger.warning(error_msg)
                errors.append(error_msg)
                
                # Stop if we get a 4xx client error
                if 400 <= response.status_code < 500:
                    break
                
                # Retry on 5xx server errors
                if 500 <= response.status_code < 600:
                    time.sleep(rate_limit_delay * 2)
                    continue
            
            data = response.json()
            results = data.get('results', [])
            
            if not results:
                logger.info(f"No more results at page {page}, stopping pagination")
                break
            
            all_jobs.extend(results)
            logger.info(f"Page {page}: fetched {len(results)} jobs (total: {len(all_jobs)})")
            
            page += 1
            
            # Rate limiting
            time.sleep(rate_limit_delay)
            
        except requests.exceptions.Timeout:
            error_msg = f"Timeout on page {page}"
            logger.warning(error_msg)
            errors.append(error_msg)
            break
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error on page {page}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
            
            # Stop after 3 consecutive errors
            if len(errors) >= 3:
                break
            
            time.sleep(rate_limit_delay * 2)
            continue
        
        except Exception as e:
            error_msg = f"Unexpected error on page {page}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
            break
    
    duration = time.time() - start_time
    
    # Create metadata
    metadata = {
        "run_date": run_date.isoformat(),
        "source": "adzuna",
        "country": country,
        "total_jobs_collected": len(all_jobs),
        "pages_fetched": page - 1,
        "api_calls": api_calls,
        "duration_seconds": round(duration, 2),
        "errors": errors,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    if len(all_jobs) == 0:
        raise ValueError("No jobs were fetched. Check API credentials and connectivity.")
    
    # Save to partitioned storage
    output_data = {
        **metadata,
        "jobs": all_jobs
    }
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"jobs_{run_date.strftime('%Y-%m-%d')}_run_{timestamp}.json"
    
    file_path = storage.write(
        data=output_data,
        run_date=run_date,
        filename=filename
    )
    
    logger.info(
        f"✅ Ingestion complete: {len(all_jobs)} jobs saved to {file_path} "
        f"in {duration:.1f}s"
    )
    
    # Return metadata for next task
    metadata['file_path'] = str(file_path)
    return metadata


if __name__ == "__main__":
    # For standalone testing
    from datetime import datetime
    result = ingest_jobs(datetime.now())
    print(json.dumps(result, indent=2))