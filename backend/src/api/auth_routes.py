"""
Authentication routes for the Wink platform.
Handles login, registration, invites, and password management.
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr

from ..auth.auth_manager import get_auth_manager, AuthManager
from ..auth.middleware import get_current_user, require_store_owner, get_store_context
from ..database.database import get_db_session
from ..database.models import User, Store, Invite
from ..services.email_service import send_invite_email

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["authentication"])

# Request/Response models
class LoginRequest(BaseModel):
    email: EmailStr
    password: str

class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user: Dict[str, Any]
    store: Dict[str, Any]

class RefreshRequest(BaseModel):
    refresh_token: str

class InviteRequest(BaseModel):
    email: EmailStr
    role: str = "manager"

class AcceptInviteRequest(BaseModel):
    invite_token: str
    password: str

class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str

class ForgotPasswordRequest(BaseModel):
    email: EmailStr

class ResetPasswordRequest(BaseModel):
    reset_token: str
    new_password: str

class CreateStoreRequest(BaseModel):
    store_name: str
    owner_email: EmailStr
    owner_password: str

@router.post("/login", response_model=LoginResponse)
async def login(
    request: LoginRequest,
    db: Session = Depends(get_db_session),
    auth: AuthManager = Depends(get_auth_manager)
):
    """Authenticate user and return JWT tokens."""
    user = auth.authenticate_user(db, request.email, request.password)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password"
        )
    
    # Create tokens
    tokens = auth.create_user_tokens(user)
    
    # Get store information
    store = db.query(Store).filter(Store.id == user.store_id).first()
    if not store:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Store not found"
        )
    
    return LoginResponse(
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
        user={
            "id": str(user.id),
            "email": user.email,
            "role": user.role,
            "last_login_at": user.last_login_at.isoformat() if user.last_login_at else None
        },
        store={
            "id": str(store.id),
            "name": store.name,
            "timezone": store.timezone
        }
    )

@router.post("/refresh")
async def refresh_token(
    request: RefreshRequest,
    db: Session = Depends(get_db_session),
    auth: AuthManager = Depends(get_auth_manager)
):
    """Refresh access token using refresh token."""
    try:
        # Verify refresh token
        payload = auth.verify_token(request.refresh_token, "refresh")
        user_id = payload.get("sub")
        
        # Get user
        user = db.query(User).filter(User.id == user_id, User.is_active == True).first()
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found"
            )
        
        # Create new tokens
        tokens = auth.create_user_tokens(user)
        
        return {
            "access_token": tokens["access_token"],
            "refresh_token": tokens["refresh_token"],
            "token_type": "bearer"
        }
        
    except Exception as e:
        logger.error(f"Token refresh failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token"
        )

@router.post("/invite")
async def create_invite(
    request: InviteRequest,
    background_tasks: BackgroundTasks,
    user: User = Depends(require_store_owner()),
    db: Session = Depends(get_db_session),
    auth: AuthManager = Depends(get_auth_manager)
):
    """Create an invitation for a new user (store owner only)."""
    invite = auth.create_invite(
        db=db,
        store_id=str(user.store_id),
        email=request.email,
        role=request.role,
        invited_by_id=str(user.id)
    )
    
    # Send invitation email in background
    background_tasks.add_task(
        send_invite_email,
        email=invite.email,
        invite_token=invite.invite_token,
        store_name=user.store.name,
        invited_by=user.email
    )
    
    return {
        "message": "Invitation sent successfully",
        "invite_id": str(invite.id),
        "email": invite.email,
        "expires_at": invite.expires_at.isoformat()
    }

@router.post("/accept-invite")
async def accept_invite(
    request: AcceptInviteRequest,
    db: Session = Depends(get_db_session),
    auth: AuthManager = Depends(get_auth_manager)
):
    """Accept an invitation and create a new user account."""
    user = auth.accept_invite(db, request.invite_token, request.password)
    
    # Create tokens for the new user
    tokens = auth.create_user_tokens(user)
    
    # Get store information
    store = db.query(Store).filter(Store.id == user.store_id).first()
    
    return LoginResponse(
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
        user={
            "id": str(user.id),
            "email": user.email,
            "role": user.role,
            "last_login_at": None
        },
        store={
            "id": str(store.id),
            "name": store.name,
            "timezone": store.timezone
        }
    )

@router.post("/change-password")
async def change_password(
    request: ChangePasswordRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
    auth: AuthManager = Depends(get_auth_manager)
):
    """Change the current user's password."""
    auth.change_password(db, user, request.old_password, request.new_password)
    
    return {"message": "Password changed successfully"}

@router.post("/forgot-password")
async def forgot_password(
    request: ForgotPasswordRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db_session)
):
    """Send password reset email."""
    user = db.query(User).filter(User.email == request.email, User.is_active == True).first()
    
    if user:
        # Create reset token (implementation would depend on your reset token strategy)
        # For now, we'll just log that we would send an email
        logger.info(f"Password reset requested for: {request.email}")
        
        # In a real implementation, you would:
        # 1. Generate a secure reset token
        # 2. Store it with expiration
        # 3. Send email with reset link
        
        background_tasks.add_task(
            send_password_reset_email,
            email=request.email,
            reset_token="placeholder_token"
        )
    
    # Always return success to prevent email enumeration
    return {"message": "If the email exists, a password reset link has been sent"}

@router.post("/reset-password")
async def reset_password(
    request: ResetPasswordRequest,
    db: Session = Depends(get_db_session),
    auth: AuthManager = Depends(get_auth_manager)
):
    """Reset password using reset token."""
    # This would be implemented with a proper reset token system
    # For now, return an error
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Password reset not implemented yet"
    )

@router.post("/create-store")
async def create_store(
    request: CreateStoreRequest,
    db: Session = Depends(get_db_session),
    auth: AuthManager = Depends(get_auth_manager)
):
    """Create a new store with owner (admin endpoint)."""
    # This endpoint should be protected or disabled in production
    if os.getenv("ALLOW_STORE_CREATION", "false").lower() != "true":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Store creation disabled"
        )
    
    store, user = auth.create_store_and_owner(
        db=db,
        store_name=request.store_name,
        owner_email=request.owner_email,
        owner_password=request.owner_password
    )
    
    return {
        "message": "Store and owner created successfully",
        "store_id": str(store.id),
        "owner_id": str(user.id)
    }

@router.get("/me")
async def get_current_user_info(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get current user information."""
    store = db.query(Store).filter(Store.id == user.store_id).first()
    
    return {
        "user": {
            "id": str(user.id),
            "email": user.email,
            "role": user.role,
            "last_login_at": user.last_login_at.isoformat() if user.last_login_at else None,
            "created_at": user.created_at.isoformat()
        },
        "store": {
            "id": str(store.id),
            "name": store.name,
            "timezone": store.timezone,
            "created_at": store.created_at.isoformat()
        }
    }

# Placeholder functions for email services
async def send_password_reset_email(email: str, reset_token: str):
    """Placeholder for password reset email."""
    logger.info(f"Would send password reset email to {email} with token {reset_token}")

# Import the actual email service
try:
    from ..services.email_service import send_invite_email
except ImportError:
    async def send_invite_email(email: str, invite_token: str, store_name: str, invited_by: str):
        """Placeholder for invite email."""
        logger.info(f"Would send invite email to {email} for store {store_name}")
        logger.info(f"Invite token: {invite_token}")
        logger.info(f"Invited by: {invited_by}")