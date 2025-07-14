"""
Rate Limiting and API Protection Module
Provides rate limiting, request throttling, and API protection features.
"""

import time
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Any
from collections import defaultdict, deque
from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse
import redis
import json


class RateLimiter:
    """Rate limiter implementation with sliding window."""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis_client = redis_client
        self.local_cache = defaultdict(lambda: deque())
        self.use_redis = redis_client is not None

    def _get_client_identifier(self, request: Request) -> str:
        """Get unique identifier for the client."""
        # Try to get real IP address
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()

        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip

        # Fallback to client host
        return request.client.host if request.client else "unknown"

    def _get_user_identifier(self, request: Request) -> Optional[str]:
        """Get user identifier from request (if authenticated)."""
        # Extract user from JWT token or session
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            # In a real implementation, you'd decode the JWT here
            # For now, we'll use a simple approach
            return f"user_{hash(token) % 10000}"
        return None

    def _get_rate_limit_key(self, request: Request, endpoint: str) -> str:
        """Generate rate limit key for the request."""
        client_id = self._get_client_identifier(request)
        user_id = self._get_user_identifier(request)

        if user_id:
            return f"rate_limit:{user_id}:{endpoint}"
        else:
            return f"rate_limit:{client_id}:{endpoint}"

    def _get_rate_limits(self, endpoint: str) -> Tuple[int, int]:
        """Get rate limits for an endpoint (requests, window_seconds)."""
        # Define rate limits for different endpoints
        rate_limits = {
            "/health": (100, 60),  # 100 requests per minute
            "/analytics": (30, 60),  # 30 requests per minute
            "/users": (50, 60),  # 50 requests per minute
            "/shows": (50, 60),  # 50 requests per minute
            "/events": (100, 60),  # 100 requests per minute
            "default": (20, 60),  # 20 requests per minute
        }

        return rate_limits.get(endpoint, rate_limits["default"])

    def _check_rate_limit_redis(
        self, key: str, max_requests: int, window_seconds: int
    ) -> bool:
        """Check rate limit using Redis."""
        current_time = time.time()
        window_start = current_time - window_seconds

        # Use Redis pipeline for atomic operations
        pipe = self.redis_client.pipeline()

        # Remove old entries
        pipe.zremrangebyscore(key, 0, window_start)

        # Count current requests
        pipe.zcard(key)

        # Add current request
        pipe.zadd(key, {str(current_time): current_time})

        # Set expiration
        pipe.expire(key, window_seconds)

        results = pipe.execute()
        current_requests = results[1]

        return current_requests < max_requests

    def _check_rate_limit_local(
        self, key: str, max_requests: int, window_seconds: int
    ) -> bool:
        """Check rate limit using local cache."""
        current_time = time.time()
        window_start = current_time - window_seconds

        # Remove old entries
        while self.local_cache[key] and self.local_cache[key][0] < window_start:
            self.local_cache[key].popleft()

        # Check if under limit
        if len(self.local_cache[key]) >= max_requests:
            return False

        # Add current request
        self.local_cache[key].append(current_time)
        return True

    def check_rate_limit(self, request: Request, endpoint: str) -> bool:
        """Check if request is within rate limits."""
        key = self._get_rate_limit_key(request, endpoint)
        max_requests, window_seconds = self._get_rate_limits(endpoint)

        if self.use_redis:
            return self._check_rate_limit_redis(key, max_requests, window_seconds)
        else:
            return self._check_rate_limit_local(key, max_requests, window_seconds)

    def get_rate_limit_info(self, request: Request, endpoint: str) -> Dict[str, Any]:
        """Get rate limit information for the request."""
        key = self._get_rate_limit_key(request, endpoint)
        max_requests, window_seconds = self._get_rate_limits(endpoint)

        if self.use_redis:
            current_time = time.time()
            window_start = current_time - window_seconds

            # Get current request count
            self.redis_client.zremrangebyscore(key, 0, window_start)
            current_requests = self.redis_client.zcard(key)
        else:
            current_time = time.time()
            window_start = current_time - window_seconds

            # Remove old entries and count current
            while self.local_cache[key] and self.local_cache[key][0] < window_start:
                self.local_cache[key].popleft()
            current_requests = len(self.local_cache[key])

        return {
            "current_requests": current_requests,
            "max_requests": max_requests,
            "window_seconds": window_seconds,
            "remaining_requests": max(0, max_requests - current_requests),
            "reset_time": current_time + window_seconds,
        }


class APIProtection:
    """API protection and security features."""

    def __init__(self, rate_limiter: RateLimiter):
        self.rate_limiter = rate_limiter
        self.blocked_ips = set()
        self.suspicious_ips = defaultdict(int)
        self.max_suspicious_attempts = 10

    def check_suspicious_activity(self, request: Request) -> bool:
        """Check for suspicious activity patterns."""
        client_ip = self._get_client_identifier(request)

        # Check if IP is blocked
        if client_ip in self.blocked_ips:
            return False

        # Check for suspicious patterns
        user_agent = request.headers.get("User-Agent", "")
        if not user_agent or user_agent == "python-requests":
            self.suspicious_ips[client_ip] += 1

        # Check for too many requests in short time
        if self.suspicious_ips[client_ip] > self.max_suspicious_attempts:
            self.blocked_ips.add(client_ip)
            return False

        return True

    def _get_client_identifier(self, request: Request) -> str:
        """Get client identifier (same as rate limiter)."""
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()

        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip

        return request.client.host if request.client else "unknown"

    def log_request(self, request: Request, response_time: float, status_code: int):
        """Log request for monitoring and analysis."""
        client_ip = self._get_client_identifier(request)
        user_agent = request.headers.get("User-Agent", "")

        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "client_ip": client_ip,
            "method": request.method,
            "path": request.url.path,
            "status_code": status_code,
            "response_time": response_time,
            "user_agent": user_agent,
            "headers": dict(request.headers),
        }

        # In production, this would go to a proper logging system
        print(f"REQUEST_LOG: {json.dumps(log_entry)}")


class RateLimitMiddleware:
    """FastAPI middleware for rate limiting."""

    def __init__(self, rate_limiter: RateLimiter, api_protection: APIProtection):
        self.rate_limiter = rate_limiter
        self.api_protection = api_protection

    async def __call__(self, request: Request, call_next):
        start_time = time.time()

        # Check for suspicious activity
        if not self.api_protection.check_suspicious_activity(request):
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"detail": "Access denied due to suspicious activity"},
            )

        # Check rate limit
        endpoint = request.url.path
        if not self.rate_limiter.check_rate_limit(request, endpoint):
            rate_limit_info = self.rate_limiter.get_rate_limit_info(request, endpoint)

            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "detail": "Rate limit exceeded",
                    "rate_limit_info": rate_limit_info,
                },
                headers={
                    "X-RateLimit-Limit": str(rate_limit_info["max_requests"]),
                    "X-RateLimit-Remaining": str(rate_limit_info["remaining_requests"]),
                    "X-RateLimit-Reset": str(int(rate_limit_info["reset_time"])),
                },
            )

        # Process request
        response = await call_next(request)

        # Calculate response time
        response_time = time.time() - start_time

        # Log request
        self.api_protection.log_request(request, response_time, response.status_code)

        # Add rate limit headers
        rate_limit_info = self.rate_limiter.get_rate_limit_info(request, endpoint)
        response.headers["X-RateLimit-Limit"] = str(rate_limit_info["max_requests"])
        response.headers["X-RateLimit-Remaining"] = str(
            rate_limit_info["remaining_requests"]
        )
        response.headers["X-RateLimit-Reset"] = str(int(rate_limit_info["reset_time"]))

        return response


# Utility functions for manual rate limiting
def rate_limit_decorator(max_requests: int, window_seconds: int):
    """Decorator for manual rate limiting on specific endpoints."""

    def decorator(func):
        async def wrapper(*args, **kwargs):
            # This would be implemented based on your specific needs
            # For now, it's a placeholder
            return await func(*args, **kwargs)

        return wrapper

    return decorator


def get_rate_limit_info(request: Request, endpoint: str) -> Dict[str, Any]:
    """Get rate limit information for debugging."""
    # This would use the global rate limiter instance
    # For now, return mock data
    return {
        "current_requests": 5,
        "max_requests": 30,
        "window_seconds": 60,
        "remaining_requests": 25,
        "reset_time": time.time() + 60,
    }


# Global instances
try:
    redis_client = redis.Redis(host="localhost", port=6379, db=0)
    rate_limiter = RateLimiter(redis_client)
except:
    rate_limiter = RateLimiter()  # Use local cache

api_protection = APIProtection(rate_limiter)
rate_limit_middleware = RateLimitMiddleware(rate_limiter, api_protection)

# Example usage in FastAPI app:
"""
from fastapi import FastAPI
from src.rate_limiter import rate_limit_middleware

app = FastAPI()

# Add middleware
app.middleware("http")(rate_limit_middleware)

@app.get("/analytics")
@rate_limit_decorator(max_requests=30, window_seconds=60)
async def get_analytics():
    return {"data": "analytics data"}
"""
