"""
Log Analysis API Endpoints for frontend integration
"""
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from services.log_analysis_service import LogAnalysisService
from services.s3_log_service import S3LogService
from database import get_db
from models.log_analysis import LogAnalysisSession, HealthyLogPattern, UnhealthyLogAnalysis
from typing import List
import os

router = APIRouter(prefix="/log-analysis", tags=["Log Analysis"])
log_analysis_service = LogAnalysisService()

@router.get("/train-status")
def get_train_status():
    # Return count of healthy patterns
    return {"count": len(log_analysis_service.healthy_patterns)}

@router.get("/s3-status")
def get_s3_status(bucket_name: str):
    # Return S3 log count
    s3_service = S3LogService(bucket_name)
    keys = s3_service.list_log_keys()
    return {"log_count": len(keys)}

@router.get("/analysis-history")
def get_analysis_history(session_id: int, db=Depends(get_db)):
    session = db.query(LogAnalysisSession).filter_by(id=session_id).first()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    analyses = db.query(UnhealthyLogAnalysis).filter_by(session_id=session_id).all()
    return {"session": session_id, "results": [a.log_text for a in analyses]}

@router.post("/upload-healthy-logs")
def upload_healthy_logs(files: List[UploadFile] = File(...)):
    # Save uploaded files and train patterns
    saved_files = []
    for file in files:
        save_path = os.path.join(log_analysis_service.healthy_patterns_dir, file.filename)
        with open(save_path, "wb") as f:
            f.write(file.file.read())
        saved_files.append(save_path)
    count = log_analysis_service.train_healthy_patterns(saved_files)
    return {"success": True, "patterns_added": count}
