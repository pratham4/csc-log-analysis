"""DSI Transaction Statistics Service"""
from sqlalchemy.orm import Session
from sqlalchemy import text, func, and_, or_, desc
from models.transactions import DSITransactionLog, ArchiveDSITransactionLog
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any, Optional, Union
import re

logger = logging.getLogger(__name__)

class DSIStatsService:
    """Service for statistical analysis of DSI transaction logs"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def _parse_time_period(self, period_str: str) -> datetime:
        """Parse time period string to datetime cutoff"""
        period_str = period_str.lower().strip()
        now = datetime.now()
        
        # Handle different time period formats
        if 'last' in period_str:
            if 'day' in period_str:
                match = re.search(r'(\d+)\s*days?', period_str)
                if match:
                    days = int(match.group(1))
                    return now - timedelta(days=days)
                elif 'yesterday' in period_str:
                    return now - timedelta(days=1)
            elif 'week' in period_str:
                match = re.search(r'(\d+)\s*weeks?', period_str)
                if match:
                    weeks = int(match.group(1))
                    return now - timedelta(weeks=weeks)
            elif 'month' in period_str:
                match = re.search(r'(\d+)\s*months?', period_str)
                if match:
                    months = int(match.group(1))
                    return now - timedelta(days=months * 30)
        
        # Default to 5 days if not parseable
        return now - timedelta(days=5)
    
    def _format_db_datetime(self, dt: datetime) -> str:
        """Format datetime to database string format (YYYYMMDDHHMMSS)"""
        return dt.strftime('%Y%m%d%H%M%S')
    
    def _parse_db_datetime(self, db_time_str: str) -> datetime:
        """Parse database datetime string to datetime object"""
        if not db_time_str:
            return None
        try:
            # Handle YYYYMMDDHHMMSS format
            if len(db_time_str) >= 14:
                return datetime.strptime(db_time_str[:14], '%Y%m%d%H%M%S')
            # Handle YYYYMMDD format
            elif len(db_time_str) >= 8:
                return datetime.strptime(db_time_str[:8], '%Y%m%d')
        except ValueError:
            logger.warning(f"Could not parse datetime string: {db_time_str}")
        return None
    
    async def get_most_occurring_errors(
        self, 
        period: str = "last 5 days",
        instance_id: str = None,
        limit: int = 10
    ) -> Dict[str, Any]:
        """Get most occurring errors in the specified time period"""
        try:
            cutoff_date = self._parse_time_period(period)
            cutoff_str = self._format_db_datetime(cutoff_date)
            
            # Query both main and archive tables
            queries = []
            
            # Main table query
            main_query = self.db.query(
                DSITransactionLog.ErrorsOut,
                DSITransactionLog.DeviceID,
                func.count().label('error_count')
            ).filter(
                and_(
                    DSITransactionLog.ErrorsOut.isnot(None),
                    DSITransactionLog.ErrorsOut != '',
                    DSITransactionLog.WhenReceived >= cutoff_str
                )
            )
            
            if instance_id:
                main_query = main_query.filter(DSITransactionLog.DeviceID == instance_id)
            
            main_query = main_query.group_by(
                DSITransactionLog.ErrorsOut, 
                DSITransactionLog.DeviceID
            )
            
            # Archive table query
            archive_query = self.db.query(
                ArchiveDSITransactionLog.ErrorsOut,
                ArchiveDSITransactionLog.DeviceID,
                func.count().label('error_count')
            ).filter(
                and_(
                    ArchiveDSITransactionLog.ErrorsOut.isnot(None),
                    ArchiveDSITransactionLog.ErrorsOut != '',
                    ArchiveDSITransactionLog.WhenReceived >= cutoff_str
                )
            )
            
            if instance_id:
                archive_query = archive_query.filter(ArchiveDSITransactionLog.DeviceID == instance_id)
            
            archive_query = archive_query.group_by(
                ArchiveDSITransactionLog.ErrorsOut, 
                ArchiveDSITransactionLog.DeviceID
            )
            
            # Union and get results
            union_query = main_query.union(archive_query)
            results = union_query.order_by(desc('error_count')).limit(limit).all()
            
            # Format results
            errors = []
            for result in results:
                errors.append({
                    'error_message': result.ErrorsOut,
                    'instance_id': result.DeviceID,
                    'occurrence_count': result.error_count,
                    'error_preview': result.ErrorsOut[:100] + '...' if len(result.ErrorsOut) > 100 else result.ErrorsOut
                })
            
            return {
                'success': True,
                'period': period,
                'instance_filter': instance_id,
                'total_errors_found': len(errors),
                'errors': errors
            }
            
        except Exception as e:
            logger.error(f"Error getting most occurring errors: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def get_errors_for_instance_date(
        self,
        instance_id: str,
        date_str: str = "yesterday"
    ) -> Dict[str, Any]:
        """Get all errors for a specific instance on a specific date"""
        try:
            if date_str.lower() == "yesterday":
                target_date = datetime.now() - timedelta(days=1)
                start_time = target_date.replace(hour=0, minute=0, second=0)
                end_time = target_date.replace(hour=23, minute=59, second=59)
            else:
                # Try to parse the date string
                try:
                    target_date = datetime.strptime(date_str, '%Y-%m-%d')
                    start_time = target_date.replace(hour=0, minute=0, second=0)
                    end_time = target_date.replace(hour=23, minute=59, second=59)
                except ValueError:
                    return {
                        'success': False,
                        'error': f"Invalid date format: {date_str}. Use YYYY-MM-DD or 'yesterday'"
                    }
            
            start_str = self._format_db_datetime(start_time)
            end_str = self._format_db_datetime(end_time)
            
            # Query both tables
            main_results = self.db.query(DSITransactionLog).filter(
                and_(
                    DSITransactionLog.DeviceID == instance_id,
                    DSITransactionLog.ErrorsOut.isnot(None),
                    DSITransactionLog.ErrorsOut != '',
                    DSITransactionLog.WhenReceived >= start_str,
                    DSITransactionLog.WhenReceived <= end_str
                )
            ).order_by(DSITransactionLog.WhenReceived).all()
            
            archive_results = self.db.query(ArchiveDSITransactionLog).filter(
                and_(
                    ArchiveDSITransactionLog.DeviceID == instance_id,
                    ArchiveDSITransactionLog.ErrorsOut.isnot(None),
                    ArchiveDSITransactionLog.ErrorsOut != '',
                    ArchiveDSITransactionLog.WhenReceived >= start_str,
                    ArchiveDSITransactionLog.WhenReceived <= end_str
                )
            ).order_by(ArchiveDSITransactionLog.WhenReceived).all()
            
            # Combine and format results
            all_errors = []
            
            for record in main_results + archive_results:
                all_errors.append({
                    'record_id': record.RecordID,
                    'instance_id': record.DeviceID,
                    'user_id': record.UserID,
                    'when_received': record.WhenReceived,
                    'when_processed': record.WhenProcessed,
                    'function_call_id': record.FunctionCallID,
                    'error_message': record.ErrorsOut,
                    'app_id': record.AppID,
                    'elapsed_time': record.ElapsedTime,
                    'table_source': 'main' if record in main_results else 'archive'
                })
            
            # Sort by when_received
            all_errors.sort(key=lambda x: x['when_received'])
            
            return {
                'success': True,
                'instance_id': instance_id,
                'date': date_str,
                'total_errors': len(all_errors),
                'errors': all_errors
            }
            
        except Exception as e:
            logger.error(f"Error getting errors for instance and date: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def get_logs_around_error_time(
        self,
        instance_id: str,
        error_time: str,
        minutes_before: int = 1,
        minutes_after: int = 1
    ) -> Dict[str, Any]:
        """Get all logs around a specific error time for an instance"""
        try:
            # Parse the error time
            try:
                if len(error_time) == 14:  # YYYYMMDDHHMMSS
                    error_dt = datetime.strptime(error_time, '%Y%m%d%H%M%S')
                else:  # Try standard datetime format
                    error_dt = datetime.fromisoformat(error_time.replace('Z', '+00:00'))
            except ValueError:
                return {
                    'success': False,
                    'error': f"Invalid error time format: {error_time}"
                }
            
            start_time = error_dt - timedelta(minutes=minutes_before)
            end_time = error_dt + timedelta(minutes=minutes_after)
            
            start_str = self._format_db_datetime(start_time)
            end_str = self._format_db_datetime(end_time)
            
            # Query both tables for all logs in the time window
            main_results = self.db.query(DSITransactionLog).filter(
                and_(
                    DSITransactionLog.DeviceID == instance_id,
                    DSITransactionLog.WhenReceived >= start_str,
                    DSITransactionLog.WhenReceived <= end_str
                )
            ).order_by(DSITransactionLog.WhenReceived).all()
            
            archive_results = self.db.query(ArchiveDSITransactionLog).filter(
                and_(
                    ArchiveDSITransactionLog.DeviceID == instance_id,
                    ArchiveDSITransactionLog.WhenReceived >= start_str,
                    ArchiveDSITransactionLog.WhenReceived <= end_str
                )
            ).order_by(ArchiveDSITransactionLog.WhenReceived).all()
            
            # Format results
            all_logs = []
            for record in main_results + archive_results:
                all_logs.append({
                    'record_id': record.RecordID,
                    'instance_id': record.DeviceID,
                    'user_id': record.UserID,
                    'when_received': record.WhenReceived,
                    'function_call_id': record.FunctionCallID,
                    'function_call_rc': record.FunctionCallRC,
                    'error_message': record.ErrorsOut if record.ErrorsOut else None,
                    'app_id': record.AppID,
                    'elapsed_time': record.ElapsedTime,
                    'has_error': bool(record.ErrorsOut and record.ErrorsOut.strip()),
                    'table_source': 'main' if record in main_results else 'archive'
                })
            
            # Sort by when_received
            all_logs.sort(key=lambda x: x['when_received'])
            
            return {
                'success': True,
                'instance_id': instance_id,
                'error_time': error_time,
                'time_window': f"{minutes_before + minutes_after} minutes",
                'total_logs': len(all_logs),
                'logs': all_logs
            }
            
        except Exception as e:
            logger.error(f"Error getting logs around error time: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def get_users_with_most_errors(
        self,
        instance_id: str,
        period: str = "last 5 days",
        limit: int = 10
    ) -> Dict[str, Any]:
        """Get users who caused most errors for a specific instance"""
        try:
            cutoff_date = self._parse_time_period(period)
            cutoff_str = self._format_db_datetime(cutoff_date)
            
            # Query both tables
            main_query = self.db.query(
                DSITransactionLog.UserID,
                func.count().label('error_count')
            ).filter(
                and_(
                    DSITransactionLog.DeviceID == instance_id,
                    DSITransactionLog.ErrorsOut.isnot(None),
                    DSITransactionLog.ErrorsOut != '',
                    DSITransactionLog.WhenReceived >= cutoff_str
                )
            ).group_by(DSITransactionLog.UserID)
            
            archive_query = self.db.query(
                ArchiveDSITransactionLog.UserID,
                func.count().label('error_count')
            ).filter(
                and_(
                    ArchiveDSITransactionLog.DeviceID == instance_id,
                    ArchiveDSITransactionLog.ErrorsOut.isnot(None),
                    ArchiveDSITransactionLog.ErrorsOut != '',
                    ArchiveDSITransactionLog.WhenReceived >= cutoff_str
                )
            ).group_by(ArchiveDSITransactionLog.UserID)
            
            # Union and aggregate
            union_query = main_query.union(archive_query)
            results = union_query.order_by(desc('error_count')).limit(limit).all()
            
            users = []
            for result in results:
                users.append({
                    'user_id': result.UserID,
                    'error_count': result.error_count
                })
            
            return {
                'success': True,
                'instance_id': instance_id,
                'period': period,
                'total_users_with_errors': len(users),
                'users': users
            }
            
        except Exception as e:
            logger.error(f"Error getting users with most errors: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def get_logs_around_datetime(
        self,
        instance_id: str,
        target_datetime: str,
        minutes_before: int = 2,
        minutes_after: int = 2,
        user_id: str = None
    ) -> Dict[str, Any]:
        """Get all logs around a specific datetime for an instance, optionally filtered by user"""
        try:
            # Parse the target datetime
            try:
                if 'T' in target_datetime or ' ' in target_datetime:
                    # ISO format or similar
                    target_dt = datetime.fromisoformat(target_datetime.replace('Z', '+00:00').replace('T', ' '))
                else:
                    # Database format YYYYMMDDHHMMSS
                    target_dt = datetime.strptime(target_datetime[:14], '%Y%m%d%H%M%S')
            except ValueError:
                return {
                    'success': False,
                    'error': f"Invalid datetime format: {target_datetime}"
                }
            
            start_time = target_dt - timedelta(minutes=minutes_before)
            end_time = target_dt + timedelta(minutes=minutes_after)
            
            start_str = self._format_db_datetime(start_time)
            end_str = self._format_db_datetime(end_time)
            
            # Build queries with optional user filter
            main_filter = and_(
                DSITransactionLog.DeviceID == instance_id,
                DSITransactionLog.WhenReceived >= start_str,
                DSITransactionLog.WhenReceived <= end_str
            )
            
            archive_filter = and_(
                ArchiveDSITransactionLog.DeviceID == instance_id,
                ArchiveDSITransactionLog.WhenReceived >= start_str,
                ArchiveDSITransactionLog.WhenReceived <= end_str
            )
            
            if user_id:
                main_filter = and_(main_filter, DSITransactionLog.UserID == user_id)
                archive_filter = and_(archive_filter, ArchiveDSITransactionLog.UserID == user_id)
            
            # Execute queries
            main_results = self.db.query(DSITransactionLog).filter(main_filter).order_by(DSITransactionLog.WhenReceived).all()
            archive_results = self.db.query(ArchiveDSITransactionLog).filter(archive_filter).order_by(ArchiveDSITransactionLog.WhenReceived).all()
            
            # Format results
            all_logs = []
            for record in main_results + archive_results:
                all_logs.append({
                    'record_id': record.RecordID,
                    'instance_id': record.DeviceID,
                    'user_id': record.UserID,
                    'when_received': record.WhenReceived,
                    'function_call_id': record.FunctionCallID,
                    'function_call_rc': record.FunctionCallRC,
                    'app_id': record.AppID,
                    'error_message': record.ErrorsOut if record.ErrorsOut else None,
                    'data_in': record.DataIn[:200] + '...' if record.DataIn and len(record.DataIn) > 200 else record.DataIn,
                    'data_out': record.DataOut[:200] + '...' if record.DataOut and len(record.DataOut) > 200 else record.DataOut,
                    'elapsed_time': record.ElapsedTime,
                    'has_error': bool(record.ErrorsOut and record.ErrorsOut.strip()),
                    'table_source': 'main' if record in main_results else 'archive'
                })
            
            # Sort by when_received
            all_logs.sort(key=lambda x: x['when_received'])
            
            return {
                'success': True,
                'instance_id': instance_id,
                'user_filter': user_id,
                'target_datetime': target_datetime,
                'time_window': f"{minutes_before + minutes_after} minutes",
                'total_logs': len(all_logs),
                'logs': all_logs
            }
            
        except Exception as e:
            logger.error(f"Error getting logs around datetime: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def get_filtered_logs(
        self,
        instance_id: str = None,
        user_id: str = None,
        app_id: str = None,
        period: str = "last 7 days",
        has_errors_only: bool = False,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Get logs filtered by multiple criteria"""
        try:
            cutoff_date = self._parse_time_period(period)
            cutoff_str = self._format_db_datetime(cutoff_date)
            
            # Build filters
            main_filters = [DSITransactionLog.WhenReceived >= cutoff_str]
            archive_filters = [ArchiveDSITransactionLog.WhenReceived >= cutoff_str]
            
            if instance_id:
                main_filters.append(DSITransactionLog.DeviceID == instance_id)
                archive_filters.append(ArchiveDSITransactionLog.DeviceID == instance_id)
            
            if user_id:
                main_filters.append(DSITransactionLog.UserID == user_id)
                archive_filters.append(ArchiveDSITransactionLog.UserID == user_id)
            
            if app_id:
                main_filters.append(DSITransactionLog.AppID == app_id)
                archive_filters.append(ArchiveDSITransactionLog.AppID == app_id)
            
            if has_errors_only:
                main_filters.extend([
                    DSITransactionLog.ErrorsOut.isnot(None),
                    DSITransactionLog.ErrorsOut != ''
                ])
                archive_filters.extend([
                    ArchiveDSITransactionLog.ErrorsOut.isnot(None),
                    ArchiveDSITransactionLog.ErrorsOut != ''
                ])
            
            # Execute queries
            main_results = self.db.query(DSITransactionLog).filter(
                and_(*main_filters)
            ).order_by(desc(DSITransactionLog.WhenReceived)).limit(limit//2 if limit > 1 else 1).all()
            
            archive_results = self.db.query(ArchiveDSITransactionLog).filter(
                and_(*archive_filters)
            ).order_by(desc(ArchiveDSITransactionLog.WhenReceived)).limit(limit//2 if limit > 1 else 1).all()
            
            # Format results
            all_logs = []
            for record in main_results + archive_results:
                all_logs.append({
                    'record_id': record.RecordID,
                    'instance_id': record.DeviceID,
                    'user_id': record.UserID,
                    'when_received': record.WhenReceived,
                    'function_call_id': record.FunctionCallID,
                    'app_id': record.AppID,
                    'error_message': record.ErrorsOut[:100] + '...' if record.ErrorsOut and len(record.ErrorsOut) > 100 else record.ErrorsOut,
                    'elapsed_time': record.ElapsedTime,
                    'has_error': bool(record.ErrorsOut and record.ErrorsOut.strip()),
                    'table_source': 'main' if record in main_results else 'archive'
                })
            
            # Sort by when_received (newest first)
            all_logs.sort(key=lambda x: x['when_received'], reverse=True)
            
            return {
                'success': True,
                'filters': {
                    'instance_id': instance_id,
                    'user_id': user_id,
                    'app_id': app_id,
                    'period': period,
                    'errors_only': has_errors_only
                },
                'total_logs': len(all_logs),
                'logs': all_logs[:limit]  # Apply final limit
            }
            
        except Exception as e:
            logger.error(f"Error getting filtered logs: {e}")
            return {
                'success': False,
                'error': str(e)
            }