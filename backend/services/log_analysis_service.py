"""
LogAnalysisService: Real-time log analysis for unhealthy log detection and pattern comparison.
"""
import os
import re
from typing import List, Dict, Any

ERROR_KEYWORDS = {
    'TIMEOUT': 0.9,
    'ERROR': 0.8,
    'FAILED': 0.7
}

class LogAnalysisService:
    def train_healthy_patterns(self, pattern_files: List[str]) -> int:
        """
        Train healthy log patterns from a list of file paths.
        Returns the number of patterns added.
        """
        new_patterns = []
        for fpath in pattern_files:
            if os.path.isfile(fpath):
                with open(fpath, 'r', encoding='utf-8') as f:
                    new_patterns.extend([line.strip() for line in f if line.strip()])
        self.healthy_patterns.extend(new_patterns)
        # Optionally, persist to healthy_patterns_dir
        if self.healthy_patterns_dir:
            with open(os.path.join(self.healthy_patterns_dir, 'trained_patterns.txt'), 'a', encoding='utf-8') as out:
                for pattern in new_patterns:
                    out.write(pattern + '\n')
        return len(new_patterns)
    def __init__(self, healthy_patterns_dir: str = 'healthy_patterns'):
        self.healthy_patterns_dir = healthy_patterns_dir
        self.healthy_patterns = self._load_healthy_patterns()

    def _load_healthy_patterns(self) -> List[str]:
        # Stub: Load healthy log patterns from local folder
        patterns = []
        if os.path.isdir(self.healthy_patterns_dir):
            for fname in os.listdir(self.healthy_patterns_dir):
                fpath = os.path.join(self.healthy_patterns_dir, fname)
                if os.path.isfile(fpath):
                    with open(fpath, 'r', encoding='utf-8') as f:
                        patterns.extend([line.strip() for line in f if line.strip()])
        return patterns

    def detect_unhealthy_log(self, log: str) -> Dict[str, Any]:
        # Detect unhealthy log by error keywords
        result = {'unhealthy': False, 'score': 0.0, 'matched_keywords': []}
        for keyword, score in ERROR_KEYWORDS.items():
            if re.search(keyword, log, re.IGNORECASE):
                result['unhealthy'] = True
                result['score'] = max(result['score'], score)
                result['matched_keywords'].append(keyword)
        return result

    def compare_with_healthy_patterns(self, log: str) -> bool:
        # Compare log against healthy patterns
        for pattern in self.healthy_patterns:
            if pattern in log:
                return True
        return False

    def analyze_logs(self, logs: List[str]) -> List[Dict[str, Any]]:
        # Analyze a batch of logs
        results = []
        for log in logs:
            unhealthy_result = self.detect_unhealthy_log(log)
            healthy_match = self.compare_with_healthy_patterns(log)
            results.append({
                'log': log,
                'unhealthy': unhealthy_result['unhealthy'],
                'score': unhealthy_result['score'],
                'matched_keywords': unhealthy_result['matched_keywords'],
                'healthy_match': healthy_match
            })
        return results

    # S3 monitoring stub (to be implemented)
    def monitor_s3_bucket(self):
        pass
