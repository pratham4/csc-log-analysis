// Log analysis TypeScript interfaces

export interface LogAnalysisSession {
  id: number;
  userId: string;
  startedAt: string;
  status: string;
}

export interface HealthyLogPattern {
  id: number;
  pattern: string;
  sourceFile?: string;
}

export interface UnhealthyLogAnalysis {
  id: number;
  sessionId: number;
  logText: string;
  detectedKeywords: string[];
  score: number;
  healthyMatch: boolean;
  analyzedAt: string;
}

export interface S3Status {
  logCount: number;
}

export interface TrainingStatus {
  count: number;
}

export interface LogAnalysisResult {
  log: string;
  unhealthy: boolean;
  score: number;
  matchedKeywords: string[];
  healthyMatch: boolean;
  suggestedSolution?: string;
}
