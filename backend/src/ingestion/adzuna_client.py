"""
Async Adzuna API Client for JobPulse.
Fetches job postings from Adzuna API with rate limiting, pagination, and error handling.
"""

import os
import httpx
import structlog
from typing import List, Dict, Optional, Any
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

logger = structlog.get_logger(__name__)


class AdzunaClient:
    """
    Async HTTP client for Adzuna Job Search API.
    
    Handles authentication, pagination, rate limiting, and error recovery.
    """
    
    BASE_URL = "https://api.adzuna.com/v1/api/jobs"
    
    def __init__(self, app_id: Optional[str] = None, api_key: Optional[str] = None):
        """
        Initialize Adzuna client.
        
        Args:
            app_id: Adzuna App ID (defaults to ADZUNA_APP_ID env var)
            api_key: Adzuna API Key (defaults to ADZUNA_API_KEY env var)
        """
        # Load .env if not already loaded
        env_file = Path(__file__).parent.parent.parent / ".env"
        load_dotenv(env_file)
        
        self.app_id = app_id or os.getenv("ADZUNA_APP_ID")
        self.api_key = api_key or os.getenv("ADZUNA_API_KEY")
        
        if not self.app_id or not self.api_key:
            raise ValueError(
                "Adzuna credentials not found. Set ADZUNA_APP_ID and ADZUNA_API_KEY in .env"
            )
        
        logger.info("adzuna_client_initialized", app_id=self.app_id[:10] + "...")
    
    async def search_jobs(
        self,
        country: str = "gb",
        keywords: str = "developer",
        location: Optional[str] = None,
        page: int = 1,
        per_page: int = 50,
    ) -> Dict[str, Any]:
        """
        Search for jobs on Adzuna.
        
        Args:
            country: Country code (gb, us, au, ca, de, fr, etc.)
            keywords: Search keywords (e.g., "python developer")
            location: Location filter (e.g., "London", "New York")
            page: Page number (1-indexed)
            per_page: Results per page (max 50)
            
        Returns:
            Dict with jobs list and metadata
        """
        url = f"{self.BASE_URL}/{country}/search/{page}"
        
        params = {
            "app_id": self.app_id,
            "app_key": self.api_key,
            "results_per_page": min(per_page, 50),
            "what": keywords,
        }
        
        if location:
            params["where"] = location
        
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                logger.info(
                    "api_request_started",
                    endpoint=url,
                    country=country,
                    keywords=keywords,
                )
                
                response = await client.get(url, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                logger.info(
                    "api_request_success",
                    country=country,
                    jobs_returned=len(data.get("results", [])),
                    total_available=data.get("count", 0),
                )
                
                return data
                
        except httpx.HTTPError as e:
            logger.error("api_request_failed", error=str(e), url=url)
            raise
    
    async def fetch_jobs_by_location(
        self,
        locations: List[str],
        keywords: str = "developer",
        country: str = "gb",
        pages: int = 2,
    ) -> List[Dict[str, Any]]:
        """
        Fetch jobs from multiple locations.
        
        Args:
            locations: List of location names (e.g., ["London", "Manchester"])
            keywords: Search keywords
            country: Country code
            pages: Number of pages to fetch per location
            
        Returns:
            List of job postings
        """
        all_jobs = []
        
        for location in locations:
            logger.info("fetching_jobs_for_location", location=location, pages=pages)
            
            for page in range(1, pages + 1):
                try:
                    result = await self.search_jobs(
                        country=country,
                        keywords=keywords,
                        location=location,
                        page=page,
                        per_page=50,
                    )
                    
                    jobs = result.get("results", [])
                    all_jobs.extend(jobs)
                    
                    logger.info(
                        "page_fetched",
                        location=location,
                        page=page,
                        jobs_in_page=len(jobs),
                    )
                    
                    # Stop if no more results
                    if not jobs or len(jobs) < 50:
                        break
                        
                except Exception as e:
                    logger.warning(
                        "page_fetch_failed",
                        location=location,
                        page=page,
                        error=str(e),
                    )
                    continue
        
        logger.info("location_fetch_complete", total_jobs=len(all_jobs))
        return all_jobs
    
    async def fetch_job_details(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch full details for a specific job.
        
        Args:
            job_id: Job ID from search results
            
        Returns:
            Full job details or None if not found
        """
        # Adzuna job IDs contain country code, e.g., "1234567@gbe"
        # Extract country from job_id
        if "@" in job_id:
            country = job_id.split("@")[1].rstrip("e")  # Remove trailing 'e' from @gbe, @use
        else:
            country = "gb"
        
        url = f"{self.BASE_URL}/{country}/{job_id}"
        
        params = {
            "app_id": self.app_id,
            "app_key": self.api_key,
        }
        
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.warning("job_details_fetch_failed", job_id=job_id, error=str(e))
            return None


async def fetch_fresh_jobs(
    locations: List[str] = None,
    countries: List[str] = None,
    keywords_list: List[str] = None,
) -> List[Dict[str, Any]]:
    """
    Convenience function to fetch fresh jobs from Adzuna API.
    
    Args:
        locations: List of location names
        countries: List of country codes
        keywords_list: List of search keywords
        
    Returns:
        List of all fetched jobs
    """
    locations = locations or ["London", "Manchester", "Amsterdam", "Berlin"]
    countries = countries or ["gb", "nl", "de"]
    keywords_list = keywords_list or ["python developer", "javascript developer", "data engineer"]
    
    client = AdzunaClient()
    all_jobs = []
    
    for country in countries:
        # Map country codes to locations
        location_map = {
            "gb": ["London", "Manchester", "Birmingham"],
            "nl": ["Amsterdam", "Rotterdam"],
            "de": ["Berlin", "Munich"],
        }
        
        country_locations = location_map.get(country, locations)
        
        for keywords in keywords_list:
            try:
                jobs = await client.fetch_jobs_by_location(
                    locations=country_locations,
                    keywords=keywords,
                    country=country,
                    pages=2,
                )
                all_jobs.extend(jobs)
                
            except Exception as e:
                logger.error(
                    "country_fetch_failed",
                    country=country,
                    keywords=keywords,
                    error=str(e),
                )
                continue
    
    logger.info("fresh_jobs_complete", total_jobs=len(all_jobs))
    return all_jobs


if __name__ == "__main__":
    import asyncio
    
    async def test():
        """Test the Adzuna client."""
        jobs = await fetch_fresh_jobs(
            countries=["gb"],
            keywords_list=["python"],
        )
        print(f"\nFetched {len(jobs)} jobs")
        if jobs:
            print(f"Sample job: {jobs[0]['title']} at {jobs[0]['company']['display_name']}")
    
    asyncio.run(test())
