"""
FastAPI BackgroundTasks integration for async pipeline execution.
Manages pipeline run lifecycle and real-time status updates.
"""

import asyncio
import structlog
from datetime import datetime
from typing import Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, insert
import json

logger = structlog.get_logger(__name__)


# In-process task queue for managing background tasks
_active_tasks: Dict[int, asyncio.Task] = {}
_task_results: Dict[int, Dict] = {}


async def run_pipeline_task(
    db: AsyncSession,
    run_id: int,
    run_date: Optional[datetime] = None,
    incremental: bool = True,
    log_context: Optional[Dict] = None,
) -> Dict:
    """
    Execute full pipeline as background task with status tracking.
    
    Args:
        db: AsyncSession for database operations
        run_id: Unique pipeline run ID
        run_date: Date for this run (defaults to today)
        incremental: If True, only process new jobs
        log_context: Optional logging context
        
    Returns:
        Dict with pipeline execution results
    """
    logger = structlog.get_logger(__name__).bind(**{**(log_context or {}), "run_id": run_id})
    run_date = run_date or datetime.now()
    
    try:
        logger.info("background_pipeline_task_started")
        
        # Update PipelineRun status to RUNNING
        from src.db.models import PipelineRun
        query = (
            update(PipelineRun)
            .where(PipelineRun.id == run_id)
            .values(status="RUNNING", started_at=datetime.utcnow())
        )
        await db.execute(query)
        await db.commit()
        
        # Import and run pipeline
        from src.flows.main_pipeline import run_pipeline
        
        result = await run_pipeline(
            db=db,
            run_id=run_id,
            run_date=run_date,
            incremental=incremental,
            log_context=log_context,
        )
        
        # Store result in memory
        _task_results[run_id] = result
        
        logger.info("background_pipeline_task_completed", status=result.get("status"))
        
        return result
        
    except Exception as e:
        logger.error("background_pipeline_task_failed", error=str(e))
        
        # Update status to FAILED
        from src.db.models import PipelineRun
        query = (
            update(PipelineRun)
            .where(PipelineRun.id == run_id)
            .values(
                status="FAILED",
                error_message=str(e),
                completed_at=datetime.utcnow(),
            )
        )
        await db.execute(query)
        await db.commit()
        
        result = {
            "run_id": run_id,
            "status": "FAILED",
            "error": str(e),
            "completed_at": datetime.utcnow().isoformat(),
        }
        _task_results[run_id] = result
        
        raise


def get_task_status(run_id: int) -> Dict:
    """
    Get current status of a background pipeline task.
    
    Args:
        run_id: Pipeline run ID
        
    Returns:
        Dict with current task status
    """
    if run_id in _active_tasks:
        task = _active_tasks[run_id]
        
        if task.done():
            try:
                result = task.result()
                return {
                    "run_id": run_id,
                    "status": "COMPLETED",
                    "result": result,
                }
            except Exception as e:
                return {
                    "run_id": run_id,
                    "status": "FAILED",
                    "error": str(e),
                }
        else:
            return {
                "run_id": run_id,
                "status": "RUNNING",
                "progress": "Pipeline execution in progress...",
            }
    
    if run_id in _task_results:
        return {
            "run_id": run_id,
            "status": _task_results[run_id].get("status"),
            "result": _task_results[run_id],
        }
    
    return {
        "run_id": run_id,
        "status": "NOT_FOUND",
        "error": "Task not found",
    }


def create_background_task(
    db: AsyncSession,
    run_id: int,
    run_date: Optional[datetime] = None,
    incremental: bool = True,
) -> asyncio.Task:
    """
    Create and register a background pipeline task.
    
    Args:
        db: AsyncSession for database operations
        run_id: Pipeline run ID
        run_date: Date for this run
        incremental: If True, only process new jobs
        
    Returns:
        asyncio.Task for the background pipeline execution
    """
    logger.info("creating_background_task", run_id=run_id)
    
    task = asyncio.create_task(
        run_pipeline_task(
            db=db,
            run_id=run_id,
            run_date=run_date,
            incremental=incremental,
            log_context={"source": "background_task"},
        )
    )
    
    # Register task
    _active_tasks[run_id] = task
    
    # Add cleanup callback
    def cleanup(t):
        logger.info("background_task_cleanup", run_id=run_id)
        if run_id in _active_tasks:
            del _active_tasks[run_id]
    
    task.add_done_callback(cleanup)
    
    return task


async def wait_for_task(run_id: int, timeout: Optional[float] = None) -> Dict:
    """
    Wait for a background task to complete.
    
    Args:
        run_id: Pipeline run ID
        timeout: Optional timeout in seconds
        
    Returns:
        Dict with task result
    """
    if run_id not in _active_tasks:
        return {
            "run_id": run_id,
            "status": "NOT_FOUND",
            "error": "Task not found",
        }
    
    task = _active_tasks[run_id]
    
    try:
        result = await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
        return result
    except asyncio.TimeoutError:
        logger.warning("task_wait_timeout", run_id=run_id, timeout=timeout)
        return {
            "run_id": run_id,
            "status": "TIMEOUT",
            "error": f"Task did not complete within {timeout} seconds",
        }
    except Exception as e:
        logger.error("task_wait_failed", run_id=run_id, error=str(e))
        return {
            "run_id": run_id,
            "status": "ERROR",
            "error": str(e),
        }


async def cancel_task(run_id: int) -> bool:
    """
    Cancel a background pipeline task.
    
    Args:
        run_id: Pipeline run ID
        
    Returns:
        True if task was cancelled, False otherwise
    """
    if run_id not in _active_tasks:
        logger.warning("cannot_cancel_nonexistent_task", run_id=run_id)
        return False
    
    task = _active_tasks[run_id]
    
    if task.done():
        logger.warning("cannot_cancel_completed_task", run_id=run_id)
        return False
    
    task.cancel()
    logger.info("background_task_cancelled", run_id=run_id)
    
    return True


async def get_task_logs(
    run_id: int,
    db: AsyncSession,
    limit: int = 100,
) -> Dict:
    """
    Fetch structured logs for a pipeline run.
    
    Args:
        run_id: Pipeline run ID
        db: AsyncSession for database operations
        limit: Maximum number of log entries to return
        
    Returns:
        Dict with pipeline logs (requires log table implementation)
    """
    logger.info("fetching_task_logs", run_id=run_id, limit=limit)
    
    # This would fetch from a PipelineLog table if implemented
    # For now, return status
    status = get_task_status(run_id)
    
    return {
        "run_id": run_id,
        "status": status.get("status"),
        "logs": [
            {
                "timestamp": datetime.utcnow().isoformat(),
                "level": "INFO",
                "message": f"Pipeline status: {status.get('status')}",
            }
        ],
    }


class BackgroundTaskManager:
    """Manage multiple background pipeline tasks."""
    
    def __init__(self):
        self.tasks: Dict[int, asyncio.Task] = {}
        self.results: Dict[int, Dict] = {}
    
    async def submit(
        self,
        db: AsyncSession,
        run_id: int,
        run_date: Optional[datetime] = None,
        incremental: bool = True,
    ) -> int:
        """
        Submit a pipeline run as a background task.
        
        Args:
            db: AsyncSession for database operations
            run_id: Pipeline run ID
            run_date: Date for this run
            incremental: If True, only process new jobs
            
        Returns:
            run_id of submitted task
        """
        logger.info("submitting_background_task", run_id=run_id)
        
        task = asyncio.create_task(
            run_pipeline_task(
                db=db,
                run_id=run_id,
                run_date=run_date,
                incremental=incremental,
                log_context={"source": "background_manager"},
            )
        )
        
        self.tasks[run_id] = task
        
        def cleanup(t):
            logger.info("task_cleanup", run_id=run_id)
            try:
                self.results[run_id] = t.result()
            except Exception:
                pass
            if run_id in self.tasks:
                del self.tasks[run_id]
        
        task.add_done_callback(cleanup)
        
        return run_id
    
    async def get_status(self, run_id: int) -> Dict:
        """Get status of a previously submitted task."""
        if run_id in self.tasks:
            task = self.tasks[run_id]
            if task.done():
                try:
                    return {"run_id": run_id, "status": "COMPLETED", "result": task.result()}
                except Exception as e:
                    return {"run_id": run_id, "status": "FAILED", "error": str(e)}
            else:
                return {"run_id": run_id, "status": "RUNNING"}
        
        if run_id in self.results:
            return {"run_id": run_id, "status": "COMPLETED", "result": self.results[run_id]}
        
        return {"run_id": run_id, "status": "NOT_FOUND"}
    
    async def cancel(self, run_id: int) -> bool:
        """Cancel a background task."""
        if run_id in self.tasks and not self.tasks[run_id].done():
            self.tasks[run_id].cancel()
            logger.info("task_cancelled_via_manager", run_id=run_id)
            return True
        return False
    
    def get_active_tasks(self) -> Dict[int, str]:
        """Get all active tasks and their statuses."""
        return {
            run_id: "RUNNING" if not task.done() else "COMPLETED"
            for run_id, task in self.tasks.items()
        }


# Global task manager instance
task_manager = BackgroundTaskManager()
