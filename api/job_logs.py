"""Job Logs API endpoints"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Dict, List, Optional, Any
import logging

from database import get_db
from services.job_logs_service import JobLogsService
from api.auth import get_current_user
from models.users import User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/job-logs", tags=["job-logs"])


@router.get("/latest-errors")
async def get_latest_errors(
    limit: int = Query(3, description="Number of latest error records to fetch", ge=1, le=10),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get the latest error job records from the last 24 hours
    
    Args:
        limit: Number of records to fetch (default: 3, max: 10)
        current_user: Authenticated user
        db: Database session
        
    Returns:
        List of latest error job records from last 24 hours
    """
    try:
        job_logs_service = JobLogsService(db)
        
        # Filter for failed jobs from last 24 hours only
        filters = {
            "status": "FAILED",
            "last_hours": 24
        }
        
        result = job_logs_service.query_job_logs(
            filters=filters,
            limit=limit,
            offset=0,
            order_by="started_at",
            order_direction="desc"
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error", "Failed to fetch job logs"))
        
        return {
            "success": True,
            "records": result["records"],
            "total_errors": result["total_count"]
        }
        
    except Exception as e:
        logger.error(f"Error fetching latest error job logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/detail/{job_id}")
async def get_job_detail(
    job_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get detailed information about a specific job
    
    Args:
        job_id: ID of the job to fetch details for
        current_user: Authenticated user
        db: Database session
        
    Returns:
        Detailed job information
    """
    try:
        job_logs_service = JobLogsService(db)
        
        # Filter by specific job ID
        filters = {
            "id": job_id
        }
        
        result = job_logs_service.query_job_logs(
            filters=filters,
            limit=1,
            offset=0
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error", "Failed to fetch job details"))
        
        if not result["records"]:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return {
            "success": True,
            "record": result["records"][0]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching job detail for ID {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/")
async def get_job_logs(
    limit: int = Query(100, description="Number of records to fetch", ge=1, le=1000),
    offset: int = Query(0, description="Number of records to skip", ge=0),
    status: Optional[str] = Query(None, description="Filter by job status (SUCCESS, FAILED, IN_PROGRESS)"),
    job_type: Optional[str] = Query(None, description="Filter by job type (DELETE, ARCHIVE, OTHER)"),
    table_name: Optional[str] = Query(None, description="Filter by table name"),
    source: Optional[str] = Query(None, description="Filter by source (SCRIPT, CHATBOT)"),
    date_range: Optional[str] = Query(None, description="Filter by date range (today, yesterday, last_7_days, etc.)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get job logs with various filters
    
    Args:
        limit: Maximum number of records to return
        offset: Number of records to skip for pagination
        status: Filter by job status
        job_type: Filter by job type
        table_name: Filter by table name
        source: Filter by source
        date_range: Filter by date range
        current_user: Authenticated user
        db: Database session
        
    Returns:
        Paginated job logs with applied filters
    """
    try:
        job_logs_service = JobLogsService(db)
        
        # Build filters dictionary
        filters = {}
        if status:
            filters["status"] = status.upper()
        if job_type:
            filters["job_type"] = job_type.upper()
        if table_name:
            filters["table_name"] = table_name
        if source:
            filters["source"] = source.upper()
        if date_range:
            filters["date_range"] = date_range
        
        result = job_logs_service.query_job_logs(
            filters=filters if filters else None,
            limit=limit,
            offset=offset,
            order_by="started_at",
            order_direction="desc"
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error", "Failed to fetch job logs"))
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching job logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
async def get_job_summary(
    status: Optional[str] = Query(None, description="Filter by job status for summary"),
    date_range: Optional[str] = Query(None, description="Filter by date range for summary"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get summary statistics for job logs
    
    Args:
        status: Filter by job status for summary
        date_range: Filter by date range for summary
        current_user: Authenticated user
        db: Database session
        
    Returns:
        Summary statistics for job logs
    """
    try:
        job_logs_service = JobLogsService(db)
        
        # Build filters dictionary
        filters = {}
        if status:
            filters["status"] = status.upper()
        if date_range:
            filters["date_range"] = date_range
        
        result = job_logs_service.get_job_summary_stats(
            filters=filters if filters else None
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error", "Failed to fetch job summary"))
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching job summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))