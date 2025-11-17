"""Authentication service"""
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from models.users import User
from services.microsoft_oauth_service import MicrosoftOAuthService
from typing import Optional, Dict, List
import hashlib
import jwt
import os
from datetime import datetime, timedelta, timezone
import logging
from passlib.context import CryptContext

logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class AuthService:
    def __init__(self):
        self.secret_key = os.getenv("SECRET_KEY", "your-secret-key-change-in-production-environment")
        self.algorithm = "HS256"
        self.token_expire_minutes = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
        self.microsoft_oauth = MicrosoftOAuthService()
    
    def hash_password(self, password: str) -> str:
        """Hash a password using bcrypt"""
        return pwd_context.hash(password)
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash"""
        return pwd_context.verify(plain_password, hashed_password)
    
    def authenticate_user(self, username: str, password: str, db: Session = None) -> Optional[Dict]:
        """Authenticate user and return user info with role"""
        try:
            if not db:
                from database import get_db
                db = next(get_db())
            
            # Password is required for authentication
            if not password:
                logger.warning(f"Authentication failed for {username}: No password provided")
                return None
                
            user = db.query(User).filter(User.username == username).first()
            
            if not user:
                logger.warning(f"Authentication failed for {username}: User not found")
                return None
            
            # User must have a password hash stored in database
            if not user.password_hash:
                logger.warning(f"Authentication failed for {username}: No password hash stored")
                return None
            
            # Verify password against stored hash
            if not self.verify_password(password, user.password_hash):
                logger.warning(f"Authentication failed for {username}: Invalid password")
                return None
            
            logger.info(f"User {username} authenticated successfully")
            
            return {
                "user_id": user.username,
                "username": user.username,
                "role": user.role,
                "permissions": self.get_role_permissions(user.role),
                "active": True
            }
            
        except Exception as e:
            logger.error(f"Authentication error for {username}: {e}")
            return None
    
    def get_role_permissions(self, role: str) -> Dict[str, bool]:
        """Get permissions for a role"""
        if role == "Admin":
            return {
                "select": True,
                "archive": True,
                "delete_archive": True,
                "confirm_operations": True
            }
        elif role == "Monitor":
            return {
                "select": True,
                "archive": False,
                "delete_archive": False,
                "confirm_operations": False
            }
        else:
            return {
                "select": False,
                "archive": False,
                "delete_archive": False,
                "confirm_operations": False
            }
    
    def check_permission(self, user_role: str, operation: str) -> bool:
        """Check if user role has permission for operation"""
        permissions = self.get_role_permissions(user_role)
        
        operation_map = {
            "SELECT": "select",
            "ARCHIVE": "archive",
            "DELETE": "delete_archive",
            "CONFIRM": "confirm_operations"
        }
        
        permission_key = operation_map.get(operation.upper())
        return permissions.get(permission_key, False)
    
    def create_access_token(self, user_data: dict) -> str:
        """Create JWT access token"""
        try:
            to_encode = user_data.copy()
            expire = datetime.now(timezone.utc) + timedelta(minutes=self.token_expire_minutes)
            to_encode.update({"exp": expire})
            
            return jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        except Exception as e:
            logger.error(f"Token creation error: {e}")
            return ""
    
    def verify_token(self, token: str) -> Optional[Dict]:
        """Verify JWT token and return user data"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("Token expired")
            return None
        except jwt.JWTError as e:
            logger.warning(f"Token verification failed: {e}")
            return None
    
    def get_user_from_token(self, token: str) -> Optional[Dict]:
        """Get user information from JWT token"""
        return self.verify_token(token)
    
    def get_all_users(self, db: Session = None) -> List[Dict]:
        """Get all users with their roles (Admin only)"""
        try:
            if not db:
                from database import get_db
                db = next(get_db())
                
            users = db.query(User).all()
            return [
                {
                    "id": user.id,
                    "username": user.username,
                    "role": user.role,
                    "active": True
                }
                for user in users
            ]
        except Exception as e:
            logger.error(f"Error fetching users: {e}")
            return []
    
    def create_user(self, username: str, password: str, role: str, db: Session) -> Dict:
        """Create a new user with hashed password"""
        try:
            # Check if username already exists
            existing_user = db.query(User).filter(User.username == username).first()
            if existing_user:
                return {
                    "success": False,
                    "error": f"Username '{username}' already exists"
                }
            
            # Validate role
            if role not in ["Admin", "Monitor"]:
                return {
                    "success": False,
                    "error": f"Invalid role '{role}'. Must be 'Admin' or 'Monitor'"
                }
            
            # Hash the password
            password_hash = self.hash_password(password)
            
            # Create new user
            new_user = User(
                username=username,
                password_hash=password_hash,
                role=role
            )
            
            db.add(new_user)
            db.commit()
            db.refresh(new_user)
            
            logger.info(f"User '{username}' created successfully with role '{role}'")
            
            return {
                "success": True,
                "user_id": new_user.id,
                "username": new_user.username,
                "role": new_user.role,
                "created_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error creating user '{username}': {e}")
            return {
                "success": False,
                "error": f"Failed to create user: {str(e)}"
            }
    
    # Microsoft OAuth Authentication Methods
    
    async def authenticate_microsoft_user(self, access_token: str, db: Session = None) -> Optional[Dict]:
        """Authenticate Microsoft OAuth user and return user info"""
        if not self.microsoft_oauth.is_configured():
            logger.error("Microsoft OAuth not configured")
            return None
            
        try:
            if not db:
                from database import get_db
                db = next(get_db())
            
            # Validate Microsoft token and get user info
            microsoft_user = await self.microsoft_oauth.validate_access_token(access_token)
            if not microsoft_user:
                logger.warning("Microsoft token validation failed")
                return None
            
            email = microsoft_user.get('email')
            microsoft_id = microsoft_user.get('id')
            
            if not email:
                logger.error("No email found in Microsoft user info")
                return None
            
            # Check if user exists in our database
            user = db.query(User).filter(
                or_(User.email == email, User.oauth_id == microsoft_id)
            ).first()
            
            # Determine role for this user
            assigned_role = self.microsoft_oauth.determine_user_role(microsoft_user)
            
            if not user:
                # Create new Microsoft user
                user = self.create_microsoft_user(microsoft_user, assigned_role, db)
                if not user:
                    return None
            else:
                # Update existing user info
                user = self.update_microsoft_user(user, microsoft_user, assigned_role, db)
            
            # Return authentication info
            return {
                "user_id": user.username,
                "username": user.username,
                "email": user.email,
                "display_name": user.display_name,
                "role": user.role,
                "permissions": self.get_role_permissions(user.role),
                "auth_provider": "microsoft",
                "active": True
            }
            
        except Exception as e:
            logger.error(f"Microsoft authentication error: {e}")
            return None
    
    def create_microsoft_user(self, microsoft_user: Dict, role: str, db: Session) -> Optional[User]:
        """Create a new user from Microsoft OAuth info"""
        try:
            email = microsoft_user.get('email')
            microsoft_id = microsoft_user.get('id')
            display_name = microsoft_user.get('display_name', '')
            
            # Generate username from email
            username = self.microsoft_oauth.create_username_from_email(email)
            
            # Ensure username is unique
            counter = 1
            original_username = username
            while db.query(User).filter(User.username == username).first():
                username = f"{original_username}_{counter}"
                counter += 1
            
            # Create new user
            new_user = User(
                username=username,
                email=email,
                display_name=display_name,
                role=role,
                oauth_provider="microsoft",
                oauth_id=microsoft_id,
                password_hash=None  # No password for OAuth users
            )
            
            db.add(new_user)
            db.commit()
            db.refresh(new_user)
            
            logger.info(f"Created new Microsoft user: {username} ({email}) with role {role}")
            return new_user
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error creating Microsoft user: {e}")
            return None
    
    def update_microsoft_user(self, user: User, microsoft_user: Dict, role: str, db: Session) -> User:
        """Update existing user with Microsoft OAuth info"""
        try:
            # Update user info from Microsoft
            user.email = microsoft_user.get('email', user.email)
            user.display_name = microsoft_user.get('display_name', user.display_name)
            user.oauth_provider = "microsoft"
            user.oauth_id = microsoft_user.get('id', user.oauth_id)
            
            # Update role if it has changed
            if user.role != role:
                logger.info(f"Updating user {user.username} role from {user.role} to {role}")
                user.role = role
            
            db.commit()
            db.refresh(user)
            
            return user
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error updating Microsoft user: {e}")
            return user
    
    def get_microsoft_auth_url(self) -> str:
        """Get Microsoft OAuth authorization URL"""
        if not self.microsoft_oauth.is_configured():
            raise ValueError("Microsoft OAuth not configured")
        return self.microsoft_oauth.get_authorization_url()
    
    async def exchange_microsoft_code(self, code: str) -> Optional[str]:
        """Exchange Microsoft authorization code for access token"""
        if not self.microsoft_oauth.is_configured():
            return None
        
        token_result = await self.microsoft_oauth.exchange_code_for_token(code)
        if token_result and 'access_token' in token_result:
            return token_result['access_token']
        return None
    
    def is_microsoft_oauth_enabled(self) -> bool:
        """Check if Microsoft OAuth is configured and enabled"""
        return self.microsoft_oauth.is_configured()