"""User model"""
from sqlalchemy import Column, Integer, String, Enum
from database import Base

class User(Base):
    __tablename__ = "users"
    
    id = Column("UserID", Integer, primary_key=True, autoincrement=True)
    username = Column("Username", String(50), unique=True, nullable=True)
    role = Column("Role", Enum('Admin', 'Monitor'), nullable=False)
    password_hash = Column("PasswordHash", String(255), nullable=True)
    
    # OAuth fields for Microsoft authentication
    oauth_provider = Column("OAuthProvider", String(50), nullable=True)  # 'microsoft' or None
    oauth_id = Column("OAuthID", String(255), nullable=True)  # Microsoft user ID
    email = Column("Email", String(255), nullable=True, unique=True)
    display_name = Column("DisplayName", String(255), nullable=True)