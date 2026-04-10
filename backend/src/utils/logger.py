"""
Structured logging for JobPulseAI
JSON-formatted logs compatible with Prefect
"""
import logging
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from .config import config


class JSONFormatter(logging.Formatter):
    """Format logs as JSON for structured logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields
        if hasattr(record, 'extra_data'):
            log_data.update(record.extra_data)
        
        return json.dumps(log_data)


def setup_logger(name: str) -> logging.Logger:
    """
    Set up a logger with JSON formatting
    
    Args:
        name: Logger name (typically __name__)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Get log level from config
    log_level = config.get('logging.level', 'INFO')
    logger.setLevel(getattr(logging, log_level))
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    
    # Use JSON format for production, simple format for development
    if config.is_production:
        console_handler.setFormatter(JSONFormatter())
    else:
        console_handler.setFormatter(
            logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        )
    
    logger.addHandler(console_handler)
    
    # File handler (if configured)
    handlers = config.get('logging.handlers', [])
    if 'file' in handlers:
        log_file = config.get('logging.file.path', 'logs/jobpulse.log')
        log_path = Path(__file__).parent.parent.parent / log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(JSONFormatter())
        logger.addHandler(file_handler)
    
    return logger


def log_metrics(logger: logging.Logger, metrics: Dict[str, Any], level: str = 'INFO'):
    """
    Log metrics as structured data
    
    Args:
        logger: Logger instance
        metrics: Dictionary of metrics to log
        level: Log level (default: INFO)
    """
    log_func = getattr(logger, level.lower())
    
    # Create a log record with extra data
    extra_record = logging.LogRecord(
        name=logger.name,
        level=getattr(logging, level),
        pathname='',
        lineno=0,
        msg=f"Metrics: {metrics.get('phase', 'unknown')}",
        args=(),
        exc_info=None
    )
    extra_record.extra_data = metrics
    
    logger.handle(extra_record)
