"""
Authentication manager with JWT tokens, bcrypt hashing, and invite system.
Handles user authentication, authorization, and session management.
"""

import os
import secrets
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from passlib.context import CryptContext
from jose import JWTError, jwt
from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from ..database.models import User, Store, Invite

logger = logging.getLogger(__name__)

class AuthManager:
    def __init__(self):
        self.secret_key = os.getenv("JWT_SECRET_KEY", self._generate_secret_key())
        self.algorithm = "HS256"
        self.access_token_expire_minutes = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
        self.refresh_token_expire_days = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))
        self.invite_expire_hours = int(os.getenv("INVITE_EXPIRE_HOURS", "48"))
        
        # Password hashing
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        
        if self.secret_key.startswith("GENERATED_"):
            logger.warning("Using generated JWT secret key. Set JWT_SECRET_KEY environment variable for production.")
    
    def _generate_secret_key(self) -> str:
        """Generate a random secret key for development."""
        return f"GENERATED_{secrets.token_urlsafe(32)}"
    
    def hash_password(self, password: str) -> str:
        """Hash a password using bcrypt."""
        return self.pwd_context.hash(password)
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash."""
        return self.pwd_context.verify(plain_password, hashed_password)
    
    def create_access_token(self, data: Dict[str, Any]) -> str:
        """Create a JWT access token."""
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(minutes=self.access_token_expire_minutes)
        to_encode.update({"exp": expire, "type": "access"})
        
        return jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
    
    def create_refresh_token(self, data: Dict[str, Any]) -> str:
        """Create a JWT refresh token."""
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(days=self.refresh_token_expire_days)
        to_encode.update({"exp": expire, "type": "refresh"})
        
        return jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
    
    def verify_token(self, token: str, token_type: str = "access") -> Dict[str, Any]:
        """Verify and decode a JWT token."""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            if payload.get("type") != token_type:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid token type"
                )
            
            return payload
            
        except JWTError as e:
            logger.error(f"JWT verification failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials"
            )
    
    def authenticate_user(self, db: Session, email: str, password: str) -> Optional[User]:
        """Authenticate a user by email and password."""
        user = db.query(User).filter(User.email == email, User.is_active == True).first()
        
        if not user or not self.verify_password(password, user.password_hash):
            return None
        
        # Update last login
        user.last_login_at = datetime.utcnow()
        db.commit()
        
        return user
    
    def create_user_tokens(self, user: User) -> Dict[str, str]:
        """Create access and refresh tokens for a user."""
        token_data = {
            "sub": str(user.id),
            "email": user.email,
            "store_id": str(user.store_id),
            "role": user.role
        }
        
        access_token = self.create_access_token(token_data)
        refresh_token = self.create_refresh_token(token_data)
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer"
        }
    
    def get_current_user(self, db: Session, token: str) -> User:
        """Get the current user from a JWT token."""
        payload = self.verify_token(token)
        user_id = payload.get("sub")
        
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload"
            )
        
        user = db.query(User).filter(User.id == user_id, User.is_active == True).first()
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found or inactive"
            )
        
        return user
    
    def create_invite(self, db: Session, store_id: str, email: str, role: str, invited_by_id: str) -> Invite:
        """Create an invitation for a new user."""
        # Check if user already exists
        existing_user = db.query(User).filter(User.email == email).first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User with this email already exists"
            )
        
        # Check if there's already a pending invite
        existing_invite = db.query(Invite).filter(
            Invite.email == email,
            Invite.accepted_at.is_(None),
            Invite.expires_at > datetime.utcnow()
        ).first()
        
        if existing_invite:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Pending invitation already exists for this email"
            )
        
        # Create new invite
        invite_token = secrets.token_urlsafe(32)
        expires_at = datetime.utcnow() + timedelta(hours=self.invite_expire_hours)
        
        invite = Invite(
            store_id=store_id,
            email=email,
            role=role,
            invite_token=invite_token,
            invited_by=invited_by_id,
            expires_at=expires_at
        )
        
        db.add(invite)
        db.commit()
        db.refresh(invite)
        
        return invite
    
    def accept_invite(self, db: Session, invite_token: str, password: str) -> User:
        """Accept an invitation and create a new user."""
        invite = db.query(Invite).filter(
            Invite.invite_token == invite_token,
            Invite.accepted_at.is_(None),
            Invite.expires_at > datetime.utcnow()
        ).first()
        
        if not invite:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid or expired invitation"
            )
        
        # Check if user already exists (shouldn't happen, but safety check)
        existing_user = db.query(User).filter(User.email == invite.email).first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User already exists"
            )
        
        # Create new user
        user = User(
            store_id=invite.store_id,
            email=invite.email,
            password_hash=self.hash_password(password),
            role=invite.role
        )
        
        db.add(user)
        
        # Mark invite as accepted
        invite.accepted_at = datetime.utcnow()
        
        db.commit()
        db.refresh(user)
        
        return user
    
    def change_password(self, db: Session, user: User, old_password: str, new_password: str) -> bool:
        """Change a user's password."""
        if not self.verify_password(old_password, user.password_hash):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Incorrect current password"
            )
        
        user.password_hash = self.hash_password(new_password)
        db.commit()
        
        return True
    
    def create_store_and_owner(self, db: Session, store_name: str, owner_email: str, owner_password: str) -> tuple[Store, User]:
        """Create a new store with its owner (for initial setup)."""
        # Check if user already exists
        existing_user = db.query(User).filter(User.email == owner_email).first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User with this email already exists"
            )
        
        # Create store
        store = Store(name=store_name)
        db.add(store)
        db.flush()  # Get the store ID
        
        # Create owner user
        user = User(
            store_id=store.id,
            email=owner_email,
            password_hash=self.hash_password(owner_password),
            role="store_owner"
        )
        db.add(user)
        
        db.commit()
        db.refresh(store)
        db.refresh(user)
        
        return store, user
    
    def check_permission(self, user: User, required_role: str) -> bool:
        """Check if user has required permission level."""
        role_hierarchy = {
            "viewer": 1,
            "manager": 2,
            "store_owner": 3
        }
        
        user_level = role_hierarchy.get(user.role, 0)
        required_level = role_hierarchy.get(required_role, 999)
        
        return user_level >= required_level

# Global auth manager instance
auth_manager = AuthManager()

def get_auth_manager() -> AuthManager:
    """Get the global auth manager instance."""
    return auth_manager