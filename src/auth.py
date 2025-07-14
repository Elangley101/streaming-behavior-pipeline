"""
Authentication and Authorization Module
Provides JWT-based authentication and role-based access control.
"""

import os
import jwt
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from passlib.context import CryptContext
from pydantic import BaseModel

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Security scheme
security = HTTPBearer()


class User(BaseModel):
    """User model for authentication."""

    username: str
    email: str
    full_name: Optional[str] = None
    disabled: Optional[bool] = None
    roles: List[str] = ["user"]


class UserInDB(User):
    """User model with hashed password."""

    hashed_password: str


class Token(BaseModel):
    """Token response model."""

    access_token: str
    token_type: str
    expires_in: int


class TokenData(BaseModel):
    """Token data model."""

    username: Optional[str] = None
    roles: List[str] = []


# In-memory user database (remove or comment out for production)
# fake_users_db = {
#     "admin": {
#         "username": "admin",
#         "email": "admin@netflix-pipeline.com",
#         "full_name": "Admin User",
#         "hashed_password": pwd_context.hash("admin123"),
#         "disabled": False,
#         "roles": ["admin", "user"]
#     },
#     "analyst": {
#         "username": "analyst",
#         "email": "analyst@netflix-pipeline.com",
#         "full_name": "Data Analyst",
#         "hashed_password": pwd_context.hash("analyst123"),
#         "disabled": False,
#         "roles": ["analyst", "user"]
#     },
#     "viewer": {
#         "username": "viewer",
#         "email": "viewer@netflix-pipeline.com",
#         "full_name": "Dashboard Viewer",
#         "hashed_password": pwd_context.hash("viewer123"),
#         "disabled": False,
#         "roles": ["viewer"]
#     }
# }


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Hash a password."""
    return pwd_context.hash(password)


def get_user(username: str) -> Optional[UserInDB]:
    """Get user from database."""
    if username in fake_users_db:
        user_dict = fake_users_db[username]
        return UserInDB(**user_dict)
    return None


def authenticate_user(username: str, password: str) -> Optional[UserInDB]:
    """Authenticate a user."""
    user = get_user(username)
    if not user:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create a JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def verify_token(token: str) -> Optional[TokenData]:
    """Verify and decode a JWT token."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        roles: List[str] = payload.get("roles", [])
        if username is None:
            return None
        token_data = TokenData(username=username, roles=roles)
        return token_data
    except jwt.PyJWTError:
        return None


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> UserInDB:
    """Get current authenticated user."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    token_data = verify_token(credentials.credentials)
    if token_data is None:
        raise credentials_exception

    user = get_user(username=token_data.username)
    if user is None:
        raise credentials_exception

    return user


async def get_current_active_user(
    current_user: UserInDB = Depends(get_current_user),
) -> UserInDB:
    """Get current active user."""
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


def require_role(required_roles: List[str]):
    """Decorator to require specific roles."""

    def role_checker(current_user: UserInDB = Depends(get_current_active_user)):
        user_roles = set(current_user.roles)
        required_roles_set = set(required_roles)

        if not user_roles.intersection(required_roles_set):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Not enough permissions"
            )
        return current_user

    return role_checker


# Role-based dependencies
require_admin = require_role(["admin"])
require_analyst = require_role(["admin", "analyst"])
require_viewer = require_role(["admin", "analyst", "viewer"])


class AuthManager:
    """Authentication manager for the application."""

    def __init__(self):
        self.active_tokens = set()

    def login(self, username: str, password: str) -> Optional[Token]:
        """Authenticate user and return token."""
        user = authenticate_user(username, password)
        if not user:
            return None

        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={"sub": user.username, "roles": user.roles},
            expires_delta=access_token_expires,
        )

        self.active_tokens.add(access_token)

        return Token(
            access_token=access_token,
            token_type="bearer",
            expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        )

    def logout(self, token: str) -> bool:
        """Logout user by invalidating token."""
        if token in self.active_tokens:
            self.active_tokens.remove(token)
            return True
        return False

    def is_token_valid(self, token: str) -> bool:
        """Check if token is valid and active."""
        if token not in self.active_tokens:
            return False

        token_data = verify_token(token)
        return token_data is not None

    def get_user_permissions(self, username: str) -> List[str]:
        """Get user permissions based on roles."""
        user = get_user(username)
        if not user:
            return []

        # Define permissions for each role
        role_permissions = {
            "admin": ["read", "write", "delete", "admin"],
            "analyst": ["read", "write", "analytics"],
            "viewer": ["read", "dashboard"],
        }

        permissions = set()
        for role in user.roles:
            if role in role_permissions:
                permissions.update(role_permissions[role])

        return list(permissions)


# Global auth manager instance
auth_manager = AuthManager()


# Utility functions for API endpoints
def get_user_from_token(token: str) -> Optional[UserInDB]:
    """Get user from token without raising exceptions."""
    token_data = verify_token(token)
    if token_data:
        return get_user(token_data.username)
    return None


def has_permission(user: UserInDB, permission: str) -> bool:
    """Check if user has specific permission."""
    permissions = auth_manager.get_user_permissions(user.username)
    return permission in permissions


def log_auth_event(event_type: str, username: str, success: bool, details: str = ""):
    """Log authentication events for audit."""
    timestamp = datetime.utcnow().isoformat()
    log_entry = {
        "timestamp": timestamp,
        "event_type": event_type,
        "username": username,
        "success": success,
        "details": details,
        "ip_address": "unknown",  # Would be extracted from request in real implementation
    }

    # In production, this would go to a proper logging system
    print(f"AUTH_LOG: {log_entry}")


# Example usage in FastAPI endpoints:
"""
@app.post("/login", response_model=Token)
async def login(username: str, password: str):
    token = auth_manager.login(username, password)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )
    log_auth_event("login", username, True)
    return token

@app.get("/protected")
async def protected_route(current_user: UserInDB = Depends(require_admin)):
    return {"message": "This is a protected route", "user": current_user.username}

@app.get("/analytics")
async def analytics_route(current_user: UserInDB = Depends(require_analyst)):
    return {"message": "Analytics data", "user": current_user.username}
"""
