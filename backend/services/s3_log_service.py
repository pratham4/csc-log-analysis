"""
S3LogService: Handles S3 log retrieval and batch processing for unhealthy log detection.
"""
import boto3
from typing import List, Dict, Any
from services.log_analysis_service import LogAnalysisService
import os

class S3LogService:
    def __init__(self, bucket_name: str, aws_access_key_id: str = None, aws_secret_access_key: str = None, region_name: str = None):
        self.bucket_name = bucket_name
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.log_analysis_service = LogAnalysisService()

    def list_log_keys(self, prefix: str = '') -> List[str]:
        # List all log object keys in the bucket with optional prefix
        paginator = self.s3.get_paginator('list_objects_v2')
        keys = []
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get('Contents', []):
                keys.append(obj['Key'])
        return keys

    def fetch_logs(self, keys: List[str]) -> List[str]:
        # Download logs from S3 by keys
        logs = []
        for key in keys:
            obj = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            log_data = obj['Body'].read().decode('utf-8')
            logs.append(log_data)
        return logs

    def process_logs_in_batches(self, prefix: str = '', batch_size: int = 1000) -> List[Dict[str, Any]]:
        # Process logs in batches, classify healthy/unhealthy
        all_keys = self.list_log_keys(prefix)
        results = []
        for i in range(0, len(all_keys), batch_size):
            batch_keys = all_keys[i:i+batch_size]
            batch_logs = self.fetch_logs(batch_keys)
            batch_results = self.log_analysis_service.analyze_logs(batch_logs)
            results.extend(batch_results)
        return results
