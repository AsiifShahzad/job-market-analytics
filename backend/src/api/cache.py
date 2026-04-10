"""
In-process response caching with TTL (no Redis required)
Implements simple LRU cache with per-route TTL settings and X-Cache headers
"""

import time
import hashlib
import json
from functools import wraps
from typing import Any, Callable, Dict, Optional, Tuple
from datetime import datetime, timedelta
import structlog

logger = structlog.get_logger(__name__)

# Global cache store: {key: (value, expiry_time)}
_cache_store: Dict[str, Tuple[Any, float]] = {}
_MAX_CACHE_ENTRIES = 200
_CACHE_HIT = "HIT"
_CACHE_MISS = "MISS"


def _generate_cache_key(route_path: str, query_params: Dict[str, Any]) -> str:
    """Generate cache key from route path and sorted query parameters"""
    sorted_params = json.dumps(query_params, sort_keys=True, default=str)
    key_base = f"{route_path}:{sorted_params}"
    return hashlib.md5(key_base.encode()).hexdigest()


def _evict_oldest() -> None:
    """Evict oldest entry (simple LRU) when cache is full"""
    if len(_cache_store) >= _MAX_CACHE_ENTRIES:
        oldest_key = min(_cache_store.keys(), key=lambda k: _cache_store[k][1])
        del _cache_store[oldest_key]
        logger.debug("Cache evicted oldest entry", evicted_key=oldest_key[:16])


def _is_expired(expiry_time: float) -> bool:
    """Check if cache entry has expired"""
    return time.time() > expiry_time


def _cleanup_expired() -> None:
    """Remove expired entries from cache periodically"""
    expired_keys = [k for k, (_, exp_time) in _cache_store.items() if _is_expired(exp_time)]
    for key in expired_keys:
        del _cache_store[key]
    if expired_keys:
        logger.debug("Cache cleanup", removed_count=len(expired_keys))


def cache_response(ttl_seconds: int = 3600) -> Callable:
    """
    Decorator to cache async route responses with TTL.
    
    Args:
        ttl_seconds: Time-to-live in seconds (default 1 hour)
    
    Usage:
        @cache_response(ttl_seconds=300)
        async def get_skills():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Tuple[Any, str]:
            # Extract route path and query parameters for cache key
            # For FastAPI routes, we can get this from request context if available
            from starlette.requests import Request
            
            request = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            
            # If no request found in args, check kwargs
            if not request:
                request = kwargs.get("request")
            
            cache_status = _CACHE_MISS
            
            if request:
                route_path = request.url.path
                query_params = dict(request.query_params)
                cache_key = _generate_cache_key(route_path, query_params)
                
                # Check cache
                _cleanup_expired()
                if cache_key in _cache_store:
                    value, expiry_time = _cache_store[cache_key]
                    if not _is_expired(expiry_time):
                        cache_status = _CACHE_HIT
                        logger.debug("Cache hit", key=cache_key[:16], ttl=ttl_seconds)
                        # Return value with cache status header info
                        return (value, _CACHE_HIT)
                
                # Cache miss: call function
                logger.debug("Cache miss", key=cache_key[:16])
                result = await func(*args, **kwargs)
                
                # Store in cache
                expiry_time = time.time() + ttl_seconds
                _evict_oldest()
                _cache_store[cache_key] = (result, expiry_time)
                logger.debug("Cached response", key=cache_key[:16], ttl=ttl_seconds)
                
                return (result, _CACHE_MISS)
            else:
                # No request context, skip caching
                result = await func(*args, **kwargs)
                return (result, _CACHE_MISS)
        
        return wrapper
    return decorator


def get_cache_status(route_path: str, query_params: Dict[str, Any]) -> str:
    """Get cache status for a specific route and parameters"""
    cache_key = _generate_cache_key(route_path, query_params)
    if cache_key in _cache_store:
        _, expiry_time = _cache_store[cache_key]
        if not _is_expired(expiry_time):
            return _CACHE_HIT
    return _CACHE_MISS


def clear_cache() -> None:
    """Clear all cached entries (useful for testing or manual invalidation)"""
    global _cache_store
    count = len(_cache_store)
    _cache_store.clear()
    logger.info("Cache cleared", entries_removed=count)


def get_cache_stats() -> Dict[str, Any]:
    """Get current cache statistics"""
    _cleanup_expired()
    return {
        "total_entries": len(_cache_store),
        "max_entries": _MAX_CACHE_ENTRIES,
        "utilization_percent": (len(_cache_store) / _MAX_CACHE_ENTRIES) * 100,
    }
