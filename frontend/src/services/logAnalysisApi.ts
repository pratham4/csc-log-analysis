import axios from 'axios';
import type { TrainingStatus, S3Status } from '../types/logAnalysis';

const BASE_URL = '/log-analysis';

export const logAnalysisApi = {
  getTrainingStatus: async (): Promise<TrainingStatus> => {
    const res = await axios.get(`${BASE_URL}/train-status`);
    return res.data;
  },
  getS3Status: async (bucketName: string): Promise<S3Status> => {
    const res = await axios.get(`${BASE_URL}/s3-status`, { params: { bucket_name: bucketName } });
    return res.data;
  },
  getAnalysisHistory: async (sessionId: number): Promise<{ session: number; results: string[] }> => {
    const res = await axios.get(`${BASE_URL}/analysis-history`, { params: { session_id: sessionId } });
    return res.data;
  },
  uploadHealthyLogs: async (files: FileList): Promise<{ success: boolean; patterns_added: number }> => {
    const formData = new FormData();
    Array.from(files).forEach(file => formData.append('files', file));
    const res = await axios.post(`${BASE_URL}/upload-healthy-logs`, formData);
    return res.data;
  },
  // Add more functions for log analysis queries as needed
};
