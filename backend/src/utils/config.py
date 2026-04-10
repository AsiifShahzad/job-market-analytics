"""
Configuration management for JobPulseAI
Loads configuration from YAML and environment variables
"""
import os
from pathlib import Path
from typing import Any, Dict
import yaml
from dotenv import load_dotenv


class Config:
    """Configuration manager with environment variable support"""
    
    def __init__(self, config_path: str = None):
        # Determine base directory (project root)
        base_dir = Path(__file__).parent.parent.parent
        
        # Load environment variables from project root .env file
        env_path = base_dir / ".env"
        load_dotenv(dotenv_path=env_path)
        
        # Determine config file path
        if config_path is None:
            config_path = base_dir / "config" / "config.yaml"
        
        # Load YAML configuration
        with open(config_path, 'r') as f:
            self._config = yaml.safe_load(f)
        
        # Override with environment variables
        self._load_env_overrides()
    
    def _load_env_overrides(self):
        """Override config with environment variables"""
        # API credentials from environment
        app_id = os.getenv("APP_ID")
        api_key = os.getenv("API_KEY")
        
        if app_id:
            self._config.setdefault('api', {}).setdefault('adzuna', {})['app_id'] = app_id
        if api_key:
            self._config.setdefault('api', {}).setdefault('adzuna', {})['api_key'] = api_key
        
        # Environment override
        env = os.getenv("ENVIRONMENT")
        if env:
            self._config['environment'] = env
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get config value using dot notation
        Example: config.get('api.adzuna.base_url')
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    def get_storage_path(self, zone: str) -> Path:
        """Get storage path for a specific zone (raw, bronze, silver, gold)"""
        base_dir = Path(__file__).parent.parent.parent
        storage_path = self.get(f'storage.{zone}.path')
        return base_dir / storage_path
    
    def get_partition_path(self, zone: str, run_date) -> Path:
        """Generate partitioned path for a zone based on run_date"""
        base_path = self.get_storage_path(zone)
        partition_cols = self.get(f'storage.{zone}.partition_cols', [])
        
        path = base_path
        for col in partition_cols:
            if col == 'year':
                path = path / f"year={run_date.year}"
            elif col == 'month':
                path = path / f"month={run_date.month:02d}"
            elif col == 'day':
                path = path / f"day={run_date.day:02d}"
        
        return path
    
    @property
    def api_credentials(self) -> Dict[str, str]:
        """Get API credentials"""
        return {
            'app_id': self.get('api.adzuna.app_id'),
            'api_key': self.get('api.adzuna.api_key')
        }
    
    @property
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.get('environment') == 'production'
    
    @property
    def is_development(self) -> bool:
        """Check if running in development"""
        return self.get('environment') == 'development'


# Global config instance
config = Config()
