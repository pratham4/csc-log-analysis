"""
Log Analysis Models: Session, Healthy Patterns, Unhealthy Analysis
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Boolean, Float
from sqlalchemy.orm import relationship
from database import Base
import datetime

class LogAnalysisSession(Base):
    __tablename__ = 'log_analysis_sessions'
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String(64), nullable=True)
    started_at = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(String(32), default='active')
    # Relationship to unhealthy analyses
    analyses = relationship('UnhealthyLogAnalysis', back_populates='session')

class HealthyLogPattern(Base):
    __tablename__ = 'healthy_log_patterns'
    id = Column(Integer, primary_key=True, index=True)
    pattern = Column(Text, nullable=False)
    source_file = Column(String(128), nullable=True)

class UnhealthyLogAnalysis(Base):
    __tablename__ = 'unhealthy_log_analyses'
    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(Integer, ForeignKey('log_analysis_sessions.id'))
    log_text = Column(Text, nullable=False)
    detected_keywords = Column(String(128), nullable=True)
    score = Column(Float, default=0.0)
    healthy_match = Column(Boolean, default=False)
    analyzed_at = Column(DateTime, default=datetime.datetime.utcnow)
    session = relationship('LogAnalysisSession', back_populates='analyses')
