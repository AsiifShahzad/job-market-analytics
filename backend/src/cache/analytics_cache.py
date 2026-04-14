"""
Analytics Caching System
Implements 70-hour cache with data appending for performance optimization
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Any, Dict
import structlog

logger = structlog.get_logger(__name__)

# Cache file location
CACHE_DIR = Path(__file__).parent.parent.parent / ".cache"
ANALYTICS_CACHE_FILE = CACHE_DIR / "analytics_cache.json"
CACHE_DURATION_HOURS = 70


class AnalyticsCache:
    """
    In-memory analytics cache with file persistence.
    
    Strategy:
    - Check if cache exists and is less than 70 hours old
    - If yes: return cached data (avoid recompute)
    - If no: fetch new data and append with previous data
    - Update timestamp and persist to disk
    """
    
    _cache: Optional[Dict[str, Any]] = None
    _last_updated: Optional[datetime] = None
    
    @classmethod
    def _ensure_cache_dir(cls) -> None:
        """Ensure cache directory exists"""
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def _is_cache_valid(cls) -> bool:
        """Check if in-memory cache is still valid (less than 70 hours old)"""
        if cls._cache is None or cls._last_updated is None:
            return False
        
        elapsed = datetime.now(timezone.utc) - cls._last_updated
        return elapsed < timedelta(hours=CACHE_DURATION_HOURS)
    
    @classmethod
    def _load_from_disk(cls) -> None:
        """Load cache from disk if it exists and is valid"""
        cls._ensure_cache_dir()
        
        if not ANALYTICS_CACHE_FILE.exists():
            logger.info("No cache file found on disk")
            return
        
        try:
            with open(ANALYTICS_CACHE_FILE, 'r') as f:
                data = json.load(f)
                
            timestamp_str = data.get('timestamp')
            if not timestamp_str:
                return
            
            cached_time = datetime.fromisoformat(timestamp_str)
            elapsed = datetime.now(timezone.utc) - cached_time
            
            if elapsed < timedelta(hours=CACHE_DURATION_HOURS):
                cls._cache = data.get('data')
                cls._last_updated = cached_time
                logger.info(
                    "Loaded cache from disk",
                    age_hours=elapsed.total_seconds() / 3600,
                    timestamp=timestamp_str
                )
            else:
                logger.info(
                    "Cache expired on disk",
                    age_hours=elapsed.total_seconds() / 3600,
                    threshold_hours=CACHE_DURATION_HOURS
                )
        except Exception as e:
            logger.error("Failed to load cache from disk", error=str(e))
    
    @classmethod
    def _save_to_disk(cls, data: Dict[str, Any]) -> None:
        """Save cache to disk"""
        cls._ensure_cache_dir()
        
        try:
            cache_data = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'data': data,
            }
            
            with open(ANALYTICS_CACHE_FILE, 'w') as f:
                json.dump(cache_data, f, indent=2, default=str)
            
            logger.info("Saved analytics cache to disk")
        except Exception as e:
            logger.error("Failed to save cache to disk", error=str(e))
    
    @classmethod
    def get(cls, key: str = "analytics") -> Optional[Dict[str, Any]]:
        """
        Get cache value if valid, otherwise return None
        
        Args:
            key: Cache key (default: "analytics")
        
        Returns:
            Cached data dict or None if cache is invalid/expired
        """
        # First, try to load from disk if in-memory cache is empty
        if cls._cache is None:
            cls._load_from_disk()
        
        # If cache is valid, return it
        if cls._is_cache_valid() and cls._cache is not None:
            logger.info("Returning cached analytics data")
            return cls._cache.get(key)
        
        return None
    
    @classmethod
    def set(cls, data: Dict[str, Any], key: str = "analytics") -> None:
        """
        Set cache value and persist to disk
        
        Args:
            data: Data to cache
            key: Cache key (default: "analytics")
        """
        cls._cache = {key: data}
        cls._last_updated = datetime.now(timezone.utc)
        cls._save_to_disk(cls._cache)
        logger.info("Updated analytics cache", key=key)
    
    @classmethod
    def append(cls, new_data: Dict[str, Any], key: str = "analytics") -> Dict[str, Any]:
        """
        Append new data to cached data (merge)
        
        Strategy for merging:
        - For trending_skills: append new items and update existing ones
        - For salary_insights: merge salary data by skill
        - For skill_insights: merge by category and skill
        - For market_insights: update with new statistics
        - Keep the latest actionable_insights
        
        Args:
            new_data: New analytics data to append
            key: Cache key (default: "analytics")
        
        Returns:
            Merged data
        """
        # Load existing cache if not in memory
        if cls._cache is None:
            cls._load_from_disk()
        
        if cls._cache is None or key not in cls._cache:
            # No existing cache, just use new data
            cls.set(new_data, key)
            logger.info("No existing cache found, starting fresh", key=key)
            return new_data
        
        existing_data = cls._cache[key]
        merged_data = cls._merge_analytics(existing_data, new_data)
        cls.set(merged_data, key)
        
        logger.info("Appended new analytics to cached data", key=key)
        return merged_data
    
    @classmethod
    def _merge_analytics(cls, old: Dict, new: Dict) -> Dict:
        """
        Merge old and new analytics data intelligently
        
        Args:
            old: Existing cached analytics data
            new: New analytics data to append
        
        Returns:
            Merged analytics data
        """
        merged = {}
        
        # Merge skill_insights (by category → skill)
        if 'skill_insights' in old and 'skill_insights' in new:
            merged['skill_insights'] = cls._merge_skill_insights(
                old['skill_insights'],
                new['skill_insights']
            )
        else:
            merged['skill_insights'] = new.get('skill_insights', old.get('skill_insights'))
        
        # Merge trending_skills (append and deduplicate)
        if 'trending_skills' in old and 'trending_skills' in new:
            merged['trending_skills'] = cls._merge_trending_skills(
                old['trending_skills'],
                new['trending_skills']
            )
        else:
            merged['trending_skills'] = new.get('trending_skills', old.get('trending_skills'))
        
        # Merge salary_insights
        if 'salary_insights' in old and 'salary_insights' in new:
            merged['salary_insights'] = cls._merge_salary_insights(
                old['salary_insights'],
                new['salary_insights']
            )
        else:
            merged['salary_insights'] = new.get('salary_insights', old.get('salary_insights'))
        
        # Merge market_insights (take the newer statistics)
        merged['market_insights'] = new.get('market_insights', old.get('market_insights'))
        
        # Merge skill_combinations
        merged['skill_combinations'] = new.get('skill_combinations', old.get('skill_combinations'))
        
        # Keep only new actionable_insights (newer is better)
        merged['actionable_insights'] = new.get('actionable_insights', old.get('actionable_insights'))
        
        # Update data_quality_report with new values
        merged['data_quality_report'] = new.get('data_quality_report', old.get('data_quality_report'))
        
        return merged
    
    @classmethod
    def _merge_skill_insights(cls, old: Dict, new: Dict) -> Dict:
        """Merge skill insights by category"""
        merged = old.copy()
        
        for category, skills in new.items():
            if category in merged:
                # Merge skills in this category
                merged_skills = cls._merge_skill_list(merged[category], skills)
                merged[category] = merged_skills
            else:
                merged[category] = skills
        
        return merged
    
    @classmethod
    def _merge_skill_list(cls, old_skills: list, new_skills: list) -> list:
        """Merge skill lists, updating frequency/percentage"""
        # Create a map of old skills by name
        old_map = {s.get('skill'): s for s in old_skills}
        
        # Update with new skills
        for new_skill in new_skills:
            skill_name = new_skill.get('skill')
            if skill_name in old_map:
                # Average the percentages
                old_skill = old_map[skill_name]
                old_pct = old_skill.get('percentage', 0)
                new_pct = new_skill.get('percentage', 0)
                new_skill['percentage'] = (old_pct + new_pct) / 2
                new_skill['frequency'] = old_skill.get('frequency', 0) + new_skill.get('frequency', 0)
                old_map[skill_name] = new_skill
            else:
                old_map[skill_name] = new_skill
        
        # Sort by percentage descending
        return sorted(old_map.values(), key=lambda x: x.get('percentage', 0), reverse=True)
    
    @classmethod
    def _merge_trending_skills(cls, old: list, new: list) -> list:
        """Merge trending skills, updating growth rates"""
        old_map = {s.get('skill'): s for s in old}
        new_map = {s.get('skill'): s for s in new}
        
        # Merge: prefer new data with old historical data
        merged_map = old_map.copy()
        merged_map.update(new_map)
        
        return sorted(
            merged_map.values(),
            key=lambda x: x.get('growth_rate', 0),
            reverse=True
        )
    
    @classmethod
    def _merge_salary_insights(cls, old: Dict, new: Dict) -> Dict:
        """Merge salary insights by skill and seniority"""
        merged = {}
        
        # Merge top_paying_skills
        if 'top_paying_skills' in old or 'top_paying_skills' in new:
            merged['top_paying_skills'] = cls._merge_skill_list(
                old.get('top_paying_skills', []),
                new.get('top_paying_skills', [])
            )
        
        # Merge by_seniority (average salaries)
        if 'by_seniority' in old or 'by_seniority' in new:
            old_sen = old.get('by_seniority', {})
            new_sen = new.get('by_seniority', {})
            merged['by_seniority'] = {}
            
            for level in set(list(old_sen.keys()) + list(new_sen.keys())):
                if level in old_sen and level in new_sen:
                    # Average the salaries
                    merged['by_seniority'][level] = {
                        'median_salary': (old_sen[level].get('median_salary', 0) +
                                         new_sen[level].get('median_salary', 0)) / 2,
                        'avg_salary': (old_sen[level].get('avg_salary', 0) +
                                      new_sen[level].get('avg_salary', 0)) / 2,
                        'sample_size': old_sen[level].get('sample_size', 0) +
                                      new_sen[level].get('sample_size', 0),
                    }
                elif level in old_sen:
                    merged['by_seniority'][level] = old_sen[level]
                else:
                    merged['by_seniority'][level] = new_sen[level]
        
        return merged
    
    @classmethod
    def clear(cls) -> None:
        """Clear cache from memory and disk"""
        cls._cache = None
        cls._last_updated = None
        
        if ANALYTICS_CACHE_FILE.exists():
            try:
                ANALYTICS_CACHE_FILE.unlink()
                logger.info("Cleared analytics cache")
            except Exception as e:
                logger.error("Failed to clear cache file", error=str(e))
    
    @classmethod
    def get_cache_age_hours(cls) -> Optional[float]:
        """Get the age of the current cache in hours"""
        if cls._last_updated is None:
            cls._load_from_disk()
        
        if cls._last_updated is None:
            return None
        
        elapsed = datetime.now(timezone.utc) - cls._last_updated
        return elapsed.total_seconds() / 3600
