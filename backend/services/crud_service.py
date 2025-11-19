"""Enhanced CRUD operations service with full DELETE functionality"""
from sqlalchemy.orm import Session
from sqlalchemy import text, func, and_, or_
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import logging

from models import (
    DSIActivities, DSITransactionLog, ArchiveDSIActivities, ArchiveDSITransactionLog
)
from services.auth_service import AuthService
from services.job_logger_service import JobLoggerService
from schemas import ParsedOperation

logger = logging.getLogger(__name__)

class ArchiveConfig:
    """Configuration for archive duplicate handling"""
    SKIP_DUPLICATES = True
    DUPLICATE_CHECK_STRATEGY = {
        "dsitransactionlog": ["GUID"],
        "dsiactivities": ["ActivityID", "PostedTime"]
    }

class CRUDService:
    """Comprehensive CRUD operations with safety mechanisms and duplicate handling"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
        self.auth_service = AuthService()
        self.job_logger = JobLoggerService(db_session)
    
    def _get_archive_table_name(self, table_name: str) -> str:
        """Get the correct archive table name for a given main table name"""
        if table_name == "dsiactivities":
            return "dsiactivitiesarchive"
        elif table_name == "dsitransactionlog":
            return "dsitransactionlogarchive" 
        elif table_name in ["dsiactivitiesarchive", "dsitransactionlogarchive"]:
            return table_name  # Already an archive table
        else:
            return f"{table_name}archive"  # Fallback for other tables
      
    async def execute_archive_operation(
        self, 
        operation: ParsedOperation, 
        user_id: str,
        reason: str,
        user_role: str = "Admin",
        confirmed: bool = False
    ) -> Dict[str, Any]:
        """Execute ARCHIVE operation (main → archive)"""
        try:
            # Verify permissions using provided user_role
            if not self.auth_service.check_permission(user_role, "ARCHIVE"):
                return {"success": False, "error": "Permission denied - Admin role required"}
            
            user_data = {"user_id": user_id, "role": user_role}
            
            # Validation
            if operation.validation_errors:
                return {"success": False, "error": f"Validation failed: {', '.join(operation.validation_errors)}"}
            
            # SAFETY CHECK: Enforce 7-day minimum archive age
            if "date_end" in operation.filters:
                from datetime import datetime, timedelta
                current_date = datetime.now()
                min_archive_date = current_date - timedelta(days=7)
                
                # Parse the date_end filter
                date_end_str = operation.filters["date_end"]
                try:
                    # Assuming YYYYMMDDHHMMSS format
                    if len(date_end_str) >= 8:
                        filter_date = datetime.strptime(date_end_str[:8], "%Y%m%d")
                        if filter_date > min_archive_date:
                            return {
                                "success": False, 
                                "error": f"Safety rule violation: Can only archive records older than 7 days. Current cutoff date {filter_date.strftime('%Y-%m-%d')} is too recent. Minimum allowed date: {min_archive_date.strftime('%Y-%m-%d')}"
                            }
                except ValueError:
                    logger.warning(f"Could not parse date_end for validation: {date_end_str}")
            
            # Preview first if not confirmed
            if not confirmed:
                preview = await self._preview_archive_operation(operation, user_id)
                # Only require confirmation if there are records to process
                if preview.get("preview_count", 0) > 0:
                    preview["requires_confirmation"] = True
                return preview
            
            # Execute archive operation
            main_model, archive_model = self._get_model_classes(operation.table)
            
            # Start transaction
            try:
                self.db.begin()
                
                # Start job log for CHATBOT operation
                job_log = self.job_logger.start_job_log(
                    job_type="ARCHIVE",
                    table_name=operation.table,
                    reason=f"Archive operation initiated by user {user_id}: {reason}",
                    source="CHATBOT"
                )
                
                # Execute archive with duplicate handling
                archived_count, deleted_count, skipped_count = await self._perform_archive(
                    operation, main_model, archive_model, user_id, reason
                )
                
                # Complete job log with enhanced information
                self.job_logger.complete_job_log(
                    job_log=job_log,
                    status="SUCCESS",
                    records_affected=archived_count,
                    reason=f"Archive completed - Archived: {archived_count}, Deleted: {deleted_count}, Skipped duplicates: {skipped_count}"
                )
                
                self.db.commit()
                
                return {
                    "success": True,
                    "operation": "ARCHIVE_WITH_DUPLICATE_HANDLING",
                    "table": operation.table,
                    "records_archived": archived_count,
                    "records_deleted": deleted_count,
                    "records_skipped": skipped_count,
                    "job_log_id": job_log.id,
                    "duplicate_handling": "enabled",
                    "message": f"Archive completed - Archived: {archived_count}, Deleted: {deleted_count}, Skipped duplicates: {skipped_count}"
                }
                
            except Exception as e:
                self.db.rollback()
                
                # Log failed operation in job_logs
                try:
                    self.job_logger.log_failed_operation(
                        job_type="ARCHIVE",
                        table_name=operation.table,
                        error_message=f"Archive operation failed for user {user_id}: {str(e)}",
                        source="CHATBOT"
                    )
                    self.db.commit()
                except Exception as log_error:
                    logger.error(f"Failed to log error to job_logs: {log_error}")
                
                raise e
                
        except Exception as e:
            logger.error(f"ARCHIVE operation failed: {e}")
            return {"success": False, "error": str(e)}
    
    async def execute_delete_operation(
        self, 
        operation: ParsedOperation, 
        user_id: str,
        reason: str,
        user_role: str = "Admin",
        confirmed: bool = False
    ) -> Dict[str, Any]:
        """Execute DELETE operation (archive only)"""
        try:
            # Verify permissions using provided user_role
            if not self.auth_service.check_permission(user_role, "DELETE"):
                return {"success": False, "error": "Permission denied - Admin role required"}
            
            user_data = {"user_id": user_id, "role": user_role}
            
            # Validation
            if operation.validation_errors:
                return {"success": False, "error": f"Validation failed: {', '.join(operation.validation_errors)}"}
            
            # DELETE can only target archive tables
            if not operation.is_archive_target:
                return {"success": False, "error": "DELETE operations can only target archive tables"}
            
            # SAFETY CHECK: Enforce minimum 30-day age for delete operations
            if "date_end" in operation.filters:
                current_date = datetime.now()
                min_delete_date = current_date - timedelta(days=30)
                
                # Parse the date_end filter
                date_end_str = operation.filters["date_end"]
                try:
                    # Assuming YYYYMMDDHHMMSS format
                    if len(date_end_str) >= 8:
                        filter_date = datetime.strptime(date_end_str[:8], "%Y%m%d")
                        if filter_date > min_delete_date:
                            return {
                                "success": False, 
                                "error": f"Safety rule violation: Can only delete archived records older than 30 days. Current cutoff date {filter_date.strftime('%Y-%m-%d')} is too recent. Minimum allowed date: {min_delete_date.strftime('%Y-%m-%d')}"
                            }
                except ValueError:
                    logger.warning(f"Could not parse date_end for delete validation: {date_end_str}")
            
            # Preview first if not confirmed
            if not confirmed:
                preview = await self._preview_delete_operation(operation, user_id)
                # Only require confirmation if there are records to process
                if preview.get("preview_count", 0) > 0:
                    preview["requires_confirmation"] = True
                return preview
            
            # Execute delete operation
            _, archive_model = self._get_model_classes(operation.table)
            
            try:
                self.db.begin()
                
                # Start job log for CHATBOT operation
                archive_table_name = self._get_archive_table_name(operation.table)
                job_log = self.job_logger.start_job_log(
                    job_type="DELETE",
                    table_name=archive_table_name,
                    reason=f"Delete operation initiated by user {user_id}: {reason}",
                    source="CHATBOT"
                )
                
                # Execute delete
                deleted_count = await self._perform_delete(operation, archive_model, user_id, reason)
                
                # Complete job log
                self.job_logger.complete_job_log(
                    job_log=job_log,
                    status="SUCCESS",
                    records_affected=deleted_count,
                    reason=f"Successfully deleted {deleted_count} records from {archive_table_name}"
                )
                
                self.db.commit()
                
                return {
                    "success": True,
                    "operation": "DELETE",
                    "table": archive_table_name,
                    "records_deleted": deleted_count,
                    "job_log_id": job_log.id,
                    "message": f"Successfully deleted {deleted_count} records from {archive_table_name}"
                }
                
            except Exception as e:
                self.db.rollback()
                
                # Log failed operation in job_logs
                try:
                    archive_table_name = self._get_archive_table_name(operation.table)
                    self.job_logger.log_failed_operation(
                        job_type="DELETE",
                        table_name=archive_table_name,
                        error_message=f"Delete operation failed for user {user_id}: {str(e)}",
                        source="CHATBOT"
                    )
                    self.db.commit()
                except Exception as log_error:
                    logger.error(f"Failed to log error to job_logs: {log_error}")
                raise e
                
        except Exception as e:
            logger.error(f"DELETE operation failed: {e}")
            return {"success": False, "error": str(e)}
    
    async def _preview_archive_operation(self, operation: ParsedOperation, user_id: str) -> Dict[str, Any]:
        """Preview archive operation without executing"""
        main_model, _ = self._get_model_classes(operation.table)
        

        
        query = self.db.query(main_model)
        query = self._apply_filters(query, operation, main_model)
        
        record_count = query.count()
        sample_records = query.limit(5).all()
        
        return {
            "success": True,
            "operation": "ARCHIVE_PREVIEW",
            "table": operation.table,
            "preview_count": record_count,
            "sample_records": [self._record_to_dict(record) for record in sample_records],
            "message": f"Preview: {record_count:,} records will be archived from {operation.table} to {self._get_archive_table_name(operation.table)}",
            "filters_applied": operation.filters,
            "safety_check": "Records will be copied to archive table before deletion from main table"
        }
    
    async def _preview_delete_operation(self, operation: ParsedOperation, user_id: str) -> Dict[str, Any]:
        """Preview delete operation without executing"""
        _, archive_model = self._get_model_classes(operation.table)
        
        query = self.db.query(archive_model)
        query = self._apply_filters(query, operation, archive_model)
        
        record_count = query.count()
        sample_records = query.limit(5).all()
        
        return {
            "success": True,
            "operation": "DELETE_PREVIEW",
            "table": operation.table,
            "preview_count": record_count,
            "sample_records": [self._record_to_dict(record) for record in sample_records],
            "message": f"WARNING: {record_count:,} records will be PERMANENTLY DELETED from {operation.table}",
            "filters_applied": operation.filters,
            "safety_warning": "This operation is IRREVERSIBLE. Records will be permanently removed."
        }
    
    async def _check_existing_records(self, operation: ParsedOperation, archive_model) -> set:
        """Check which records already exist in archive table to prevent duplicates"""
        try:
            if operation.table == "dsitransactionlog":
                # Use GUID for uniqueness check in transaction logs
                from sqlalchemy import select, and_
                
                # Build filter conditions to match what we're trying to archive
                where_conditions = []
                params = {}
                
                # Apply same filters as archive operation to get candidate GUIDs
                if "date_start" in operation.filters and "date_end" in operation.filters:
                    where_conditions.append("WhenReceived BETWEEN :date_start AND :date_end")
                    params["date_start"] = operation.filters["date_start"]
                    params["date_end"] = operation.filters["date_end"]
                elif "date_end" in operation.filters:
                    comparison_op = "<" if operation.filters.get("date_comparison") == "older_than" else "<="
                    where_conditions.append(f"WhenReceived {comparison_op} :date_end")
                    params["date_end"] = operation.filters["date_end"]
                
                if "user_id" in operation.filters:
                    where_conditions.append("UserID = :filter_user_id")
                    params["filter_user_id"] = operation.filters["user_id"]
                
                if "device_id" in operation.filters:
                    where_conditions.append("DeviceID = :device_id")
                    params["device_id"] = operation.filters["device_id"]
                
                where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
                
                # Get GUIDs from main table that match our archive criteria
                main_guids_query = text(f"SELECT GUID FROM dsitransactionlog WHERE {where_clause} AND GUID IS NOT NULL")
                main_guids_result = self.db.execute(main_guids_query, params).fetchall()
                candidate_guids = {row[0] for row in main_guids_result if row[0]}  # Filter out None values
                
                if not candidate_guids:
                    return set()
                
                # Check which of these GUIDs already exist in archive - use batched approach for large sets
                existing_guids = set()
                batch_size = 1000  # Process in batches to avoid query size limits
                
                for i in range(0, len(candidate_guids), batch_size):
                    batch = tuple(list(candidate_guids)[i:i+batch_size])
                    existing_guids_query = text("""
                        SELECT GUID FROM dsitransactionlogarchive 
                        WHERE GUID IN :candidate_guids
                    """)
                    existing_result = self.db.execute(existing_guids_query, {"candidate_guids": batch}).fetchall()
                    existing_guids.update({row[0] for row in existing_result if row[0]})
                
                logger.info(f"Duplicate check: Found {len(existing_guids)} existing GUIDs out of {len(candidate_guids)} candidates")
                return existing_guids
                
            elif operation.table == "dsiactivities":
                # Use ActivityID + PostedTime combination for uniqueness check in activities
                from sqlalchemy import text
                
                # Build filter conditions
                where_conditions = []
                params = {}
                
                if "date_start" in operation.filters and "date_end" in operation.filters:
                    where_conditions.append("PostedTime BETWEEN :date_start AND :date_end")
                    params["date_start"] = operation.filters["date_start"]
                    params["date_end"] = operation.filters["date_end"]
                elif "date_end" in operation.filters:
                    comparison_op = "<" if operation.filters.get("date_comparison") == "older_than" else "<="
                    where_conditions.append(f"PostedTime {comparison_op} :date_end")
                    params["date_end"] = operation.filters["date_end"]
                
                if "server_name" in operation.filters:
                    where_conditions.append("ServerName = :server_name")
                    params["server_name"] = operation.filters["server_name"]
                
                where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
                
                # Get ActivityID + PostedTime combinations from main table
                main_activities_query = text(f"""
                    SELECT ActivityID, PostedTime FROM dsiactivities WHERE {where_clause}
                """)
                main_activities_result = self.db.execute(main_activities_query, params).fetchall()
                candidate_activities = {(row[0], row[1]) for row in main_activities_result}
                
                if not candidate_activities:
                    return set()
                
                # Check which combinations already exist in archive
                # Build dynamic IN clause for tuples
                activity_conditions = []
                activity_params = {}
                for i, (activity_id, posted_time) in enumerate(candidate_activities):
                    activity_conditions.append(f"(ActivityID = :act_id_{i} AND PostedTime = :post_time_{i})")
                    activity_params[f"act_id_{i}"] = activity_id
                    activity_params[f"post_time_{i}"] = posted_time
                
                if activity_conditions:
                    existing_activities_query = text(f"""
                        SELECT ActivityID, PostedTime FROM dsiactivitiesarchive 
                        WHERE {' OR '.join(activity_conditions)}
                    """)
                    existing_result = self.db.execute(existing_activities_query, activity_params).fetchall()
                    return {(row[0], row[1]) for row in existing_result}
                
                return set()
            
            else:
                # For other tables, return empty set (no duplicate checking implemented)
                return set()
                
        except Exception as e:
            logger.error(f"Error checking existing records for duplicate prevention: {e}")
            # On error, return empty set to proceed with normal archiving
            return set()

    async def _perform_archive(
        self, 
        operation: ParsedOperation, 
        main_model, 
        archive_model, 
        user_id: str, 
        reason: str
    ) -> Tuple[int, int, int]:
        """Perform the actual archive operation with duplicate handling"""
        
        # Step 1: Check for existing records to prevent duplicates
        logger.info(f"Starting archive operation for table {operation.table}")
        logger.info(f"Checking for existing records in archive to prevent duplicates...")
        existing_records = await self._check_existing_records(operation, archive_model)
        skipped_count = len(existing_records)
        
        if skipped_count > 0:
            logger.info(f"Found {skipped_count} records that already exist in archive - will skip duplicates")
            if operation.table == "dsitransactionlog" and skipped_count <= 10:
                # Log first few GUIDs for debugging
                sample_guids = list(existing_records)[:5]
                logger.info(f"Sample existing GUIDs: {sample_guids}")
        
        # Build filter conditions for SQL
        where_conditions = []
        params = {"user_id": user_id, "reason": reason}
        
        # Date filters
        if "date_start" in operation.filters and "date_end" in operation.filters:
            time_field = "PostedTime" if operation.table == "dsiactivities" else "WhenReceived"
            where_conditions.append(f"{time_field} BETWEEN :date_start AND :date_end")
            params["date_start"] = operation.filters["date_start"]
            params["date_end"] = operation.filters["date_end"]
        elif "date_end" in operation.filters:
            time_field = "PostedTime" if operation.table == "dsiactivities" else "WhenReceived"
            # Check if this is an "older than" comparison (should use < instead of <=)
            if operation.filters.get("date_comparison") == "older_than":
                where_conditions.append(f"{time_field} < :date_end")
            else:
                where_conditions.append(f"{time_field} <= :date_end")
            params["date_end"] = operation.filters["date_end"]
        
        # Entity filters
        if "agent_name" in operation.filters:
            where_conditions.append("AgentName = :agent_name")
            params["agent_name"] = operation.filters["agent_name"]
        
        if "server_name" in operation.filters:
            where_conditions.append("ServerName = :server_name")
            params["server_name"] = operation.filters["server_name"]
        
        if "user_id" in operation.filters and operation.table == "dsitransactionlog":
            where_conditions.append("UserID = :filter_user_id")
            params["filter_user_id"] = operation.filters["user_id"]
        
        if "device_id" in operation.filters and operation.table == "dsitransactionlog":
            where_conditions.append("DeviceID = :device_id")
            params["device_id"] = operation.filters["device_id"]
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
                # Step 2: Add exclusion conditions to skip duplicates
        exclusion_conditions = []
        if existing_records:
            if operation.table == "dsitransactionlog":
                # Exclude records with GUIDs that already exist in archive
                if len(existing_records) > 0:
                    # Handle large sets by using batched exclusions
                    if len(existing_records) <= 1000:
                        exclusion_conditions.append("GUID NOT IN :existing_guids")
                        params["existing_guids"] = tuple(existing_records)
                    else:
                        # For very large sets, use NOT EXISTS instead
                        exclusion_conditions.append("""
                            NOT EXISTS (
                                SELECT 1 FROM dsitransactionlogarchive arch 
                                WHERE arch.GUID = dsitransactionlog.GUID
                            )
                        """)
            elif operation.table == "dsiactivities":
                # Exclude records with ActivityID + PostedTime combinations that already exist
                activity_exclusions = []
                for i, (activity_id, posted_time) in enumerate(existing_records):
                    activity_exclusions.append(f"NOT (ActivityID = :excl_act_id_{i} AND PostedTime = :excl_post_time_{i})")
                    params[f"excl_act_id_{i}"] = activity_id
                    params[f"excl_post_time_{i}"] = posted_time
                
                if activity_exclusions:
                    exclusion_conditions.append(f"({' AND '.join(activity_exclusions)})")        # Combine original filters with exclusion conditions
        final_where_clause = where_clause
        if exclusion_conditions:
            final_where_clause = f"({where_clause}) AND ({' AND '.join(exclusion_conditions)})"
        
        # Archive query - copy to archive table with explicit column mapping
        archive_table = self._get_archive_table_name(operation.table)
        main_table = operation.table
        
        # Get column names from main table (excluding the archive-specific columns)
        main_columns = []
        if operation.table == "dsiactivities":
            main_columns = [
                "ActivityID", "ActivityType", "TrackingID", "SecondaryTrackingID", 
                "AgentName", "ThreadID", "Description", "PostedTime", "PostedTimeUTC",
                "LineNumber", "FileName", "MethodName", "ServerName", "InstanceID",
                "IdenticalAlertCount", "AlertLevel", "DismissedBy", "DismissedDateTime",
                "LastIdenticalAlertDateTime", "EventID", "DefaultDescription", "ExceptionMessage"
            ]
        elif operation.table == "dsitransactionlog":
            main_columns = [
                "RecordStatus", "ProcessMethod", "TransactionType", "ServerName", "DeviceID", 
                "UserID", "DeviceLocalTime", "DeviceUTCTime", "DeviceSequenceID", "WhenReceived",
                "WhenProcessed", "WhenExtracted", "ElapsedTime", "AppID", "AppVersion", "AppItemID",
                "WorldHostID", "ConnectorID", "FunctionDefVersion", "FunctionCallID", "FunctionCallRC",
                "DataIn", "DataOut", "ErrorsOut", "SecurityID", "GUID", "UnitID", "PromotionLevelID",
                "EnvironmentID", "Marking", "OrgUnitID", "TrackingReference"
            ]
        
        if not main_columns:
            raise Exception(f"Unknown table for column mapping: {operation.table}")
        
        columns_list = ", ".join(main_columns)
        values_list = ", ".join(main_columns)
        
        # Step 3: Handle LIMIT for specific record counts
        limit_clause = ""
        order_clause = ""
        if "limit" in operation.filters:
            limit_value = operation.filters["limit"]
            if isinstance(limit_value, int) and limit_value > 0:
                # Add ORDER BY to get oldest records first, then LIMIT
                time_field = "PostedTime" if operation.table == "dsiactivities" else "WhenReceived"
                order_clause = f"ORDER BY {time_field} ASC"
                limit_clause = f"LIMIT {limit_value}"
                logger.info(f"Applying LIMIT {limit_value} to archive operation with {order_clause}")
        
        # Step 4: Archive only non-duplicate records with additional safety check
        if operation.table == "dsitransactionlog":
            # For transaction logs, add explicit EXISTS check to prevent GUID conflicts
            archive_query = text(f"""
                INSERT INTO {archive_table} 
                ({columns_list})
                SELECT {values_list}
                FROM {main_table} 
                WHERE ({final_where_clause}) 
                AND NOT EXISTS (
                    SELECT 1 FROM {archive_table} arch 
                    WHERE arch.GUID = {main_table}.GUID
                )
                AND {main_table}.GUID IS NOT NULL
                {order_clause}
                {limit_clause}
            """)
        else:
            # For activities, use the standard approach
            archive_query = text(f"""
                INSERT INTO {archive_table} 
                ({columns_list})
                SELECT {values_list}
                FROM {main_table} 
                WHERE {final_where_clause}
                {order_clause}
                {limit_clause}
            """)
        
        # Execute archive with duplicate exclusions and error handling
        archive_params = {k: v for k, v in params.items() if k not in ["user_id", "reason"]}
        try:
            result = self.db.execute(archive_query, archive_params)
            archived_count = result.rowcount
        except Exception as e:
            # If we still get a duplicate key error, it means our detection missed some records
            if "Duplicate entry" in str(e) and "GUID" in str(e):
                logger.warning(f"GUID duplicate detected during archive, implementing fallback strategy: {e}")
                
                # Fallback: Use a more conservative approach with row-by-row checking
                if operation.table == "dsitransactionlog":
                    # Get records to archive one by one and check each GUID
                    select_query = text(f"""
                        SELECT GUID FROM {main_table} 
                        WHERE {where_clause} AND GUID IS NOT NULL
                    """)
                    select_params = {k: v for k, v in params.items() if k not in ["user_id", "reason", "existing_guids"] and not k.startswith("excl_")}
                    
                    candidate_records = self.db.execute(select_query, select_params).fetchall()
                    safe_guids = []
                    
                    # Check each GUID individually
                    for record in candidate_records:
                        guid = record[0]
                        check_exists = text("SELECT COUNT(*) FROM dsitransactionlogarchive WHERE GUID = :guid")
                        exists_count = self.db.execute(check_exists, {"guid": guid}).scalar()
                        
                        if exists_count == 0:
                            safe_guids.append(guid)
                        else:
                            skipped_count += 1
                    
                    # Archive only the safe records
                    if safe_guids:
                        safe_archive_query = text(f"""
                            INSERT INTO {archive_table} 
                            ({columns_list})
                            SELECT {values_list}
                            FROM {main_table} 
                            WHERE {where_clause} AND GUID IN :safe_guids
                        """)
                        
                        safe_params = select_params.copy()
                        safe_params["safe_guids"] = tuple(safe_guids)
                        result = self.db.execute(safe_archive_query, safe_params)
                        archived_count = result.rowcount
                    else:
                        archived_count = 0
                        logger.warning("No safe records to archive after duplicate checking")
                else:
                    raise  # Re-raise for non-transaction tables
            else:
                raise  # Re-raise for other types of errors
        
        # Step 5: Clean source - delete only the records that were actually archived
        # Apply the same limit and ordering to ensure we delete exactly what was archived
        if limit_clause and order_clause:
            # For limited operations, we need to identify the exact records that were archived
            # Use a subquery to select the same records that were archived
            time_field = "PostedTime" if operation.table == "dsiactivities" else "WhenReceived"
            primary_key = "ActivityID" if operation.table == "dsiactivities" else "GUID"
            
            delete_query = text(f"""
                DELETE FROM {main_table} 
                WHERE {primary_key} IN (
                    SELECT {primary_key} FROM (
                        SELECT {primary_key}
                        FROM {main_table} 
                        WHERE {where_clause}
                        {order_clause}
                        {limit_clause}
                    ) AS limited_records
                )
            """)
        else:
            # For unlimited operations, use the original approach
            delete_query = text(f"""
                DELETE FROM {main_table} 
                WHERE {where_clause}
            """)
        
        # Use original params without exclusion conditions for delete
        delete_params = {}
        for key, value in params.items():
            if key not in ["user_id", "reason"] and not key.startswith("existing_") and not key.startswith("excl_"):
                delete_params[key] = value
        
        delete_result = self.db.execute(delete_query, delete_params)
        deleted_count = delete_result.rowcount
        
        # Step 5: Final validation - verify no conflicts occurred
        if operation.table == "dsitransactionlog" and archived_count > 0:
            # Quick check to ensure we didn't create any duplicates
            conflict_check = text("""
                SELECT COUNT(*) FROM dsitransactionlog m
                INNER JOIN dsitransactionlogarchive a ON m.GUID = a.GUID
                WHERE m.GUID IS NOT NULL
            """)
            conflict_count = self.db.execute(conflict_check).scalar()
            
            if conflict_count > 0:
                logger.warning(f"Detected {conflict_count} GUID conflicts after archive operation")
                # Don't fail the operation, but log for investigation
        
        logger.info(f"Archive completed - Archived: {archived_count}, Deleted from source: {deleted_count}, Skipped duplicates: {skipped_count}")
        
        return archived_count, deleted_count, skipped_count
    
    async def _perform_delete(
        self, 
        operation: ParsedOperation, 
        archive_model, 
        user_id: str, 
        reason: str
    ) -> int:
        """Perform the actual delete operation on archive table"""
        # Build query with filters
        query = self.db.query(archive_model)
        query = self._apply_filters(query, operation, archive_model)
        
        # Execute delete
        deleted_count = query.delete(synchronize_session=False)
        
        return deleted_count
    
    def _get_model_classes(self, table_name: str):
        """Get SQLAlchemy model classes for main and archive tables"""
        if table_name == "dsiactivities":
            return DSIActivities, ArchiveDSIActivities
        elif table_name == "dsitransactionlog":
            return DSITransactionLog, ArchiveDSITransactionLog
        elif table_name == "dsiactivitiesarchive":
            return ArchiveDSIActivities, ArchiveDSIActivities  # Archive table as both main and archive
        elif table_name == "dsitransactionlogarchive":
            return ArchiveDSITransactionLog, ArchiveDSITransactionLog  # Archive table as both main and archive
        else:
            raise ValueError(f"Unsupported table: {table_name}")
    
    def _apply_filters(self, query, operation: ParsedOperation, model_class):
        """Apply filters to SQLAlchemy query"""
        filters = operation.filters
        
        # Date filters
        if "date_start" in filters and "date_end" in filters:
            # Use PostedTime for activities tables, WhenReceived for transaction log tables
            time_field = "PostedTime" if ("activities" in operation.table) else "WhenReceived"
            query = query.filter(
                getattr(model_class, time_field).between(filters["date_start"], filters["date_end"])
            )
        elif "date_end" in filters:
            time_field = "PostedTime" if ("activities" in operation.table) else "WhenReceived"
            # Check if this is an "older than" comparison (should use < instead of <=)
            if filters.get("date_comparison") == "older_than":
                query = query.filter(getattr(model_class, time_field) < filters["date_end"])
            else:
                query = query.filter(getattr(model_class, time_field) <= filters["date_end"])
        
        # Entity filters
        if "agent_name" in filters:
            query = query.filter(model_class.AgentName == filters["agent_name"])
        
        if "server_name" in filters:
            query = query.filter(model_class.ServerName == filters["server_name"])
        
        if "user_id" in filters and hasattr(model_class, "UserID"):
            query = query.filter(model_class.UserID == filters["user_id"])
        
        if "device_id" in filters and hasattr(model_class, "DeviceID"):
            query = query.filter(model_class.DeviceID == filters["device_id"])
        
        # Apply limit for specific record counts (e.g., "archive oldest 300 records")
        if "limit" in filters:
            limit_value = filters["limit"]
            if isinstance(limit_value, int) and limit_value > 0:
                # For archive operations, order by date to get oldest records first
                time_field = "PostedTime" if ("activities" in operation.table) else "WhenReceived"
                query = query.order_by(getattr(model_class, time_field).asc()).limit(limit_value)
                logger.info(f"Applied record limit of {limit_value} to archive operation, ordering by {time_field}")
        
        return query
    
    def _record_to_dict(self, record) -> Dict:
        """Convert SQLAlchemy record to dictionary"""
        result = {}
        for column in record.__table__.columns:
            value = getattr(record, column.name)
            # Convert datetime to string for JSON serialization
            if isinstance(value, datetime):
                value = value.isoformat()
            result[column.name] = value
        return result