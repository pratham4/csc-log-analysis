"""Microsoft OAuth service for authentication"""
import os
import json
import aiohttp
import logging
from typing import Optional, Dict, List
from msal import ConfidentialClientApplication
import jwt
from datetime import datetime

logger = logging.getLogger(__name__)

class MicrosoftOAuthService:
    def __init__(self):
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        self.tenant_id = os.getenv("AZURE_TENANT_ID")
        self.redirect_uri = os.getenv("AZURE_REDIRECT_URI", "http://localhost:3000")
        
        # Microsoft Graph endpoints
        self.authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        self.graph_url = "https://graph.microsoft.com/v1.0"
        self.scope = ["User.Read"]
        
        # Role mapping configuration
        self.admin_domains = os.getenv("MICROSOFT_ADMIN_DOMAINS", "").split(",")
        self.admin_emails = os.getenv("MICROSOFT_ADMIN_EMAILS", "").split(",")
        self.default_role = os.getenv("DEFAULT_MICROSOFT_USER_ROLE", "Monitor")
        
        # Initialize MSAL client
        if self.client_id and self.client_secret and self.tenant_id:
            self.msal_app = ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=self.authority
            )
        else:
            logger.warning("Microsoft OAuth not configured - missing environment variables")
            self.msal_app = None
    
    def is_configured(self) -> bool:
        """Check if Microsoft OAuth is properly configured"""
        return self.msal_app is not None
    
    async def validate_access_token(self, access_token: str) -> Optional[Dict]:
        """Validate Microsoft access token and return user info"""
        if not access_token:
            return None
            
        try:
            # Get user info from Microsoft Graph
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            async with aiohttp.ClientSession() as session:
                # Get user profile
                async with session.get(f"{self.graph_url}/me", headers=headers) as response:
                    if response.status != 200:
                        logger.error(f"Failed to fetch user info: {response.status}")
                        return None
                    
                    user_data = await response.json()
                
                # Get user's groups (optional, for role determination)
                groups = []
                try:
                    async with session.get(f"{self.graph_url}/me/memberOf", headers=headers) as response:
                        if response.status == 200:
                            groups_data = await response.json()
                            groups = [group.get('displayName', '') for group in groups_data.get('value', [])]
                except Exception as e:
                    logger.warning(f"Could not fetch user groups: {e}")
                
                return {
                    'id': user_data.get('id'),
                    'email': user_data.get('mail') or user_data.get('userPrincipalName'),
                    'display_name': user_data.get('displayName'),
                    'given_name': user_data.get('givenName'),
                    'surname': user_data.get('surname'),
                    'groups': groups,
                    'tenant_id': self.tenant_id
                }
                
        except Exception as e:
            logger.error(f"Token validation error: {e}")
            return None
    
    def determine_user_role(self, user_info: Dict) -> str:
        """Determine user role based on Microsoft user information"""
        email = user_info.get('email', '').lower()
        groups = user_info.get('groups', [])
        
        # Check explicit admin emails
        if email in [admin.lower().strip() for admin in self.admin_emails if admin.strip()]:
            return 'Admin'
        
        # Check admin domains
        if email:
            domain = email.split('@')[-1] if '@' in email else ''
            if domain in [d.strip() for d in self.admin_domains if d.strip()]:
                return 'Admin'
        
        # Check Azure AD groups (you can customize these group names)
        admin_groups = ['cloud-inventory-admins', 'it-administrators', 'database-admins']
        if any(group.lower() in [g.lower() for g in admin_groups] for group in groups):
            return 'Admin'
        
        # Default role
        return self.default_role
    
    def get_authorization_url(self) -> str:
        """Get Microsoft OAuth authorization URL"""
        if not self.msal_app:
            raise ValueError("Microsoft OAuth not configured")
        
        auth_url = self.msal_app.get_authorization_request_url(
            scopes=self.scope,
            redirect_uri=self.redirect_uri
        )
        return auth_url
    
    async def exchange_code_for_token(self, code: str) -> Optional[Dict]:
        """Exchange authorization code for access token"""
        if not self.msal_app:
            return None
        
        try:
            result = self.msal_app.acquire_token_by_authorization_code(
                code=code,
                scopes=self.scope,
                redirect_uri=self.redirect_uri
            )
            
            if 'access_token' in result:
                return result
            else:
                logger.error(f"Token exchange failed: {result.get('error_description')}")
                return None
                
        except Exception as e:
            logger.error(f"Token exchange error: {e}")
            return None
    
    def create_username_from_email(self, email: str) -> str:
        """Create a username from Microsoft email"""
        if not email:
            return f"msuser_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Use email prefix as username, sanitize it
        username = email.split('@')[0].replace('.', '_').replace('-', '_')
        return username[:50]  # Limit length
    
    def format_user_info_for_auth(self, microsoft_user: Dict, role: str) -> Dict:
        """Format Microsoft user info for authentication response"""
        email = microsoft_user.get('email', '')
        username = self.create_username_from_email(email)
        
        return {
            "user_id": username,
            "username": username,
            "email": email,
            "display_name": microsoft_user.get('display_name', ''),
            "role": role,
            "auth_provider": "microsoft",
            "microsoft_id": microsoft_user.get('id'),
            "active": True
        }