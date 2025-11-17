"""Job Logger Service for logging chatbot operations"""
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
import logging

from models.job_logs import JobLogs

logger = logging.getLogger(__name__)

class JobLoggerService:
    """Service for logging chatbot operations to job_logs table"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def start_job_log(
        self,
        job_type: str,
        table_name: str,
        schema_name: Optional[str] = None,
        reason: Optional[str] = None,
        source: str = "CHATBOT"
    ) -> JobLogs:
        """
        Start a new job log entry
        
        Args:
            job_type: Type of operation (DELETE/ARCHIVE/OTHER)
            table_name: Target table name
            schema_name: Optional schema name
            reason: Initial reason/description
            source: Source of operation (CHATBOT/SCRIPT)
            
        Returns:
            JobLogs: Created job log entry
        """
        try:
            job_log = JobLogs(
                job_type=job_type.upper(),
                table_name=table_name,
                schema_name=schema_name,
                status="IN_PROGRESS",
                source=source.upper(),
                reason=reason,
                records_affected=0,
                started_at=func.current_timestamp()
            )
            
            self.db.add(job_log)
            self.db.flush()  # Get the ID without committing
            
            return job_log
            
        except Exception as e:
            logger.error(f"Error starting job log: {e}")
            self.db.rollback()
            raise
    
    def complete_job_log(
        self,
        job_log: JobLogs,
        status: str,
        records_affected: int = 0,
        reason: Optional[str] = None
    ) -> JobLogs:
        """
        Complete a job log entry
        
        Args:
            job_log: The job log entry to complete
            status: Final status (SUCCESS/FAILED)
            records_affected: Number of records affected
            reason: Final reason/error message
            
        Returns:
            JobLogs: Updated job log entry
        """
        try:
            job_log.status = status.upper()
            job_log.records_affected = records_affected
            job_log.finished_at = func.current_timestamp()
            
            if reason:
                job_log.reason = reason
            
            return job_log
            
        except Exception as e:
            logger.error(f"Error completing job log: {e}")
            raise
    
    def log_successful_operation(
        self,
        job_type: str,
        table_name: str,
        records_affected: int,
        reason: Optional[str] = None,
        schema_name: Optional[str] = None,
        source: str = "CHATBOT",
        records_skipped: int = 0
    ) -> JobLogs:
        """
        Log a complete successful operation in one call
        
        Args:
            job_type: Type of operation (DELETE/ARCHIVE/OTHER)
            table_name: Target table name
            records_affected: Number of records affected
            reason: Success message
            schema_name: Optional schema name
            source: Source of operation (CHATBOT/SCRIPT)
            records_skipped: Number of records skipped (for duplicate handling)
            
        Returns:
            JobLogs: Created and completed job log entry
        """
        try:
            # Enhanced reason with duplicate information if applicable
            enhanced_reason = reason
            if records_skipped > 0 and not enhanced_reason:
                enhanced_reason = f"Successfully {job_type.lower()}d {records_affected} records, skipped {records_skipped} duplicates"
            elif not enhanced_reason:
                enhanced_reason = f"Successfully {job_type.lower()}d {records_affected} records"
            
            job_log = JobLogs(
                job_type=job_type.upper(),
                table_name=table_name,
                schema_name=schema_name,
                status="SUCCESS",
                source=source.upper(),
                reason=enhanced_reason,
                records_affected=records_affected,
                started_at=func.current_timestamp(),
                finished_at=func.current_timestamp()
            )
            
            self.db.add(job_log)
            self.db.flush()
            
            return job_log
            
        except Exception as e:
            logger.error(f"Error logging successful operation: {e}")
            self.db.rollback()
            raise
    
    def log_failed_operation(
        self,
        job_type: str,
        table_name: str,
        error_message: str,
        schema_name: Optional[str] = None,
        source: str = "CHATBOT"
    ) -> JobLogs:
        """
        Log a failed operation in one call
        
        Args:
            job_type: Type of operation (DELETE/ARCHIVE/OTHER)
            table_name: Target table name
            error_message: Error description
            schema_name: Optional schema name
            source: Source of operation (CHATBOT/SCRIPT)
            
        Returns:
            JobLogs: Created and completed job log entry
        """
        try:
            job_log = JobLogs(
                job_type=job_type.upper(),
                table_name=table_name,
                schema_name=schema_name,
                status="FAILED",
                source=source.upper(),
                reason=error_message,
                records_affected=0,
                started_at=func.current_timestamp(),
                finished_at=func.current_timestamp()
            )
            
            self.db.add(job_log)
            self.db.flush()
            
            logger.error(f"Logged failed operation: ID={job_log.id}, Type={job_type}, Table={table_name}, Error={error_message}")
            return job_log
            
        except Exception as e:
            logger.error(f"Error logging failed operation: {e}")
            self.db.rollback()
            raise
    
    def update_job_log_progress(
        self,
        job_log: JobLogs,
        records_processed: int,
        reason: Optional[str] = None
    ) -> JobLogs:
        """
        Update job log with progress information
        
        Args:
            job_log: The job log entry to update
            records_processed: Current number of records processed
            reason: Updated reason/progress message
            
        Returns:
            JobLogs: Updated job log entry
        """
        try:
            job_log.records_affected = records_processed
            
            if reason:
                job_log.reason = reason
            
            return job_log
            
        except Exception as e:
            logger.error(f"Error updating job log progress: {e}")
            raise