"""
Authentication middleware for FastAPI with store context management.
Handles JWT validation, user extraction, and store scoping for RLS.
"""

import logging
from typing import Optional
from fastapi import Request, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from .auth_manager import get_auth_manager, AuthManager
from ..database.database import get_db_session
from ..database.models import User

logger = logging.getLogger(__name__)

# HTTP Bearer token scheme
security = HTTPBearer()

class AuthMiddleware:
    def __init__(self):
        self.auth_manager = get_auth_manager()
    
    async def get_current_user(
        self,
        credentials: HTTPAuthorizationCredentials = Depends(security),
        db: Session = Depends(get_db_session)
    ) -> User:
        """Extract and validate the current user from JWT token."""
        try:
            # Get the token from Authorization header
            token = credentials.credentials
            
            # Validate token and get user
            user = self.auth_manager.get_current_user(db, token)
            
            return user
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication failed"
            )
    
    async def get_optional_user(
        self,
        request: Request,
        db: Session = Depends(get_db_session)
    ) -> Optional[User]:
        """Get current user if token is provided, otherwise return None."""
        try:
            auth_header = request.headers.get("Authorization")
            if not auth_header or not auth_header.startswith("Bearer "):
                return None
            
            token = auth_header.replace("Bearer ", "")
            user = self.auth_manager.get_current_user(db, token)
            
            return user
            
        except Exception as e:
            logger.warning(f"Optional authentication failed: {e}")
            return None
    
    def require_role(self, required_role: str):
        """Decorator to require a specific role level."""
        def role_checker(user: User = Depends(self.get_current_user)) -> User:
            if not self.auth_manager.check_permission(user, required_role):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Insufficient permissions. Required: {required_role}"
                )
            return user
        return role_checker
    
    def get_store_context(self, user: User = Depends(get_current_user)) -> str:
        """Get the store context for the current user."""
        return str(user.store_id)

# Global middleware instance
auth_middleware = AuthMiddleware()

# Dependency functions for FastAPI
def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db_session)
) -> User:
    """FastAPI dependency to get the current authenticated user."""
    return auth_middleware.get_current_user(credentials, db)

def get_optional_user(
    request: Request,
    db: Session = Depends(get_db_session)
) -> Optional[User]:
    """FastAPI dependency to get the current user if authenticated."""
    return auth_middleware.get_optional_user(request, db)

def require_manager():
    """FastAPI dependency to require manager role or higher."""
    return auth_middleware.require_role("manager")

def require_store_owner():
    """FastAPI dependency to require store owner role."""
    return auth_middleware.require_role("store_owner")

def get_store_context(user: User = Depends(get_current_user)) -> str:
    """FastAPI dependency to get the store context."""
    return str(user.store_id)

class StoreContextManager:
    """Manager for setting store context in database sessions."""
    
    @staticmethod
    def set_store_context(db: Session, store_id: str):
        """Set the store context for RLS in PostgreSQL."""
        try:
            from sqlalchemy import text
            db.execute(text(f"SET app.store_id = '{store_id}'"))
        except Exception as e:
            logger.warning(f"Failed to set store context: {e}")
    
    @staticmethod
    def get_scoped_session(db: Session, store_id: str) -> Session:
        """Get a database session with store context set."""
        StoreContextManager.set_store_context(db, store_id)
        return db

# Store context manager instance
store_context = StoreContextManager()