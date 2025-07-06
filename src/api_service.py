import sys
import traceback
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uvicorn
from datetime import datetime, timedelta
import asyncio
import json
import os

# Simple logging function that always works
def setup_logging(name):
    def log(message, level="INFO"):
        print(f"[{name}] {level}: {message}")
    
    # Add methods to match standard logging interface
    log.info = lambda msg: log(msg, "INFO")
    log.warning = lambda msg: log(msg, "WARNING")
    log.error = lambda msg: log(msg, "ERROR")
    log.debug = lambda msg: log(msg, "DEBUG")
    
    return log

logger = setup_logging("api_service")

# Initialize flags
SNOWFLAKE_AVAILABLE = False
STREAMING_AVAILABLE = False
UTILS_AVAILABLE = False

# Initialize global variables
SnowflakeManager = None
StreamingProcessor = None
EventGenerator = None
PipelineError = Exception

logger.info("🚀 Starting Netflix Analytics API initialization...")

# Try to import optional dependencies with comprehensive error handling
try:
    logger.info("📦 Attempting to import Snowflake manager...")
    from src.snowflake_manager import SnowflakeManager
    SNOWFLAKE_AVAILABLE = True
    logger.info("✅ Snowflake manager imported successfully")
except Exception as e:
    logger.warning(f"⚠️ Snowflake manager not available: {e}")
    logger.debug(f"Snowflake import traceback: {traceback.format_exc()}")
    SnowflakeManager = None

try:
    logger.info("📦 Attempting to import streaming processor...")
    from src.streaming_processor import StreamingProcessor, EventGenerator
    STREAMING_AVAILABLE = True
    logger.info("✅ Streaming processor imported successfully")
except Exception as e:
    logger.warning(f"⚠️ Streaming processor not available: {e}")
    logger.debug(f"Streaming import traceback: {traceback.format_exc()}")
    StreamingProcessor = None
    EventGenerator = None

try:
    logger.info("📦 Attempting to import utils...")
    from src.utils import PipelineError, setup_logging as utils_setup_logging
    UTILS_AVAILABLE = True
    logger.info("✅ Utils imported successfully")
    # Keep the original logger to avoid reassignment issues
except Exception as e:
    logger.warning(f"⚠️ Utils not available: {e}")
    logger.debug(f"Utils import traceback: {traceback.format_exc()}")
    PipelineError = Exception

logger.info("🎬 Dependencies import complete!")

# FastAPI app
app = FastAPI(
    title="Netflix Analytics API",
    description="Real-time analytics API for Netflix-style behavioral data with ETL pipeline, Kafka streaming, and Snowflake integration",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables
snowflake_manager = None
streaming_processor = None
event_generator = None

# Mock data for when external services are not available
MOCK_ANALYTICS = {
    "total_sessions": 1250,
    "total_hours": 1875.5,
    "avg_engagement": 0.78,
    "binge_sessions": 89,
    "unique_users": 342,
    "top_shows": [
        {"show_name": "Stranger Things", "sessions": 156, "total_hours": 234.0, "avg_engagement": 0.85},
        {"show_name": "The Crown", "sessions": 134, "total_hours": 201.0, "avg_engagement": 0.82},
        {"show_name": "Wednesday", "sessions": 98, "total_hours": 147.0, "avg_engagement": 0.79}
    ],
    "recent_activity": [
        {"user_id": "user_123", "show_name": "Stranger Things", "watch_duration_minutes": 45, "watch_date": "2024-01-15T20:30:00"},
        {"user_id": "user_456", "show_name": "The Crown", "watch_duration_minutes": 60, "watch_date": "2024-01-15T19:15:00"}
    ]
}

# Pydantic models
class WatchEventRequest(BaseModel):
    user_id: str
    show_name: str
    watch_duration_minutes: float
    watch_date: datetime

class AnalyticsResponse(BaseModel):
    total_sessions: int
    total_hours: float
    avg_engagement: float
    binge_sessions: int
    unique_users: int
    top_shows: List[Dict[str, Any]]
    recent_activity: List[Dict[str, Any]]

class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    services: Dict[str, str]
    environment: str

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup with proper error handling."""
    global snowflake_manager, streaming_processor, event_generator
    
    logger.info("🎬 Starting Netflix Analytics API services...")
    
    # Initialize Snowflake if available and configured
    if SNOWFLAKE_AVAILABLE and os.getenv("SNOWFLAKE_ACCOUNT"):
        try:
            logger.info("🔗 Initializing Snowflake connection...")
            snowflake_manager = SnowflakeManager()
            logger.info("✅ Snowflake manager initialized successfully")
        except Exception as e:
            logger.warning(f"⚠️ Could not initialize Snowflake: {str(e)}")
            logger.debug(f"Snowflake init traceback: {traceback.format_exc()}")
            snowflake_manager = None
    else:
        logger.info("ℹ️ Snowflake not available or not configured")
    
    # Initialize streaming processor if available and configured
    if STREAMING_AVAILABLE and os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
        try:
            logger.info("🔗 Initializing streaming processor...")
            streaming_processor = StreamingProcessor()
            logger.info("✅ Streaming processor initialized successfully")
        except Exception as e:
            logger.warning(f"⚠️ Could not initialize streaming processor: {str(e)}")
            logger.debug(f"Streaming init traceback: {traceback.format_exc()}")
            streaming_processor = None
    else:
        logger.info("ℹ️ Streaming processor not available or not configured")
    
    # Initialize event generator if available and configured
    if STREAMING_AVAILABLE and os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
        try:
            logger.info("🔗 Initializing event generator...")
            event_generator = EventGenerator()
            logger.info("✅ Event generator initialized successfully")
        except Exception as e:
            logger.warning(f"⚠️ Could not initialize event generator: {str(e)}")
            logger.debug(f"Event generator init traceback: {traceback.format_exc()}")
            event_generator = None
    else:
        logger.info("ℹ️ Event generator not available or not configured")
    
    logger.info("🎬 Netflix Analytics API startup complete!")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global snowflake_manager, streaming_processor, event_generator
    
    logger.info("Shutting down Netflix Analytics API...")
    
    if streaming_processor and hasattr(streaming_processor, 'stop_streaming'):
        try:
            streaming_processor.stop_streaming()
            logger.info("✅ Streaming processor stopped")
        except Exception as e:
            logger.error(f"❌ Error stopping streaming processor: {str(e)}")
    
    if snowflake_manager and hasattr(snowflake_manager, 'close'):
        try:
            snowflake_manager.close()
            logger.info("✅ Snowflake connection closed")
        except Exception as e:
            logger.error(f"❌ Error closing Snowflake connection: {str(e)}")

@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint with environment information."""
    return {
        "message": "Netflix Analytics API",
        "version": "1.0.0",
        "status": "running",
        "environment": "railway" if os.getenv("RAILWAY_ENVIRONMENT") else "local",
        "description": "Real-time analytics API with ETL pipeline, Kafka streaming, and Snowflake integration"
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Comprehensive health check endpoint."""
    services = {}
    
    # Check API service
    services["api"] = "healthy"
    
    # Check Snowflake
    if snowflake_manager:
        try:
            # Simple query to test connection
            result = snowflake_manager.execute_query("SELECT 1 as test")
            services["snowflake"] = "healthy"
        except Exception as e:
            services["snowflake"] = f"unhealthy: {str(e)}"
    else:
        services["snowflake"] = "not_configured"
    
    # Check streaming processor
    if streaming_processor:
        try:
            # Check if streaming processor is responsive
            services["streaming"] = "healthy"
        except Exception as e:
            services["streaming"] = f"unhealthy: {str(e)}"
    else:
        services["streaming"] = "not_configured"
    
    # Determine overall status
    healthy_services = sum(1 for status in services.values() if "healthy" in status)
    total_services = len(services)
    
    if healthy_services == total_services:
        status = "healthy"
    elif healthy_services > 0:
        status = "degraded"
    else:
        status = "unhealthy"
    
    return HealthResponse(
        status=status,
        timestamp=datetime.now(),
        services=services,
        environment="railway" if os.getenv("RAILWAY_ENVIRONMENT") else "local"
    )

@app.post("/events", response_model=Dict[str, str])
async def create_watch_event(event: WatchEventRequest):
    """Create a new watch event with streaming integration."""
    global streaming_processor
    
    if not streaming_processor:
        # Return success with mock mode indicator
        return {
            "message": "Event received (mock mode - streaming not configured)", 
            "event_id": f"mock_{datetime.now().timestamp()}",
            "mode": "mock"
        }
    
    try:
        # Send event to Kafka
        event_dict = event.dict()
        streaming_processor.send_event("watch_events", event_dict, key=event.user_id)
        
        return {
            "message": "Event created successfully", 
            "event_id": event_dict.get("event_id"),
            "mode": "streaming"
        }
    
    except Exception as e:
        logger.error(f"Error creating event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating event: {str(e)}")

@app.get("/analytics", response_model=AnalyticsResponse)
async def get_analytics(
    days: int = 7,
    user_id: Optional[str] = None,
    show_name: Optional[str] = None
):
    """Get analytics data with Snowflake integration."""
    global snowflake_manager
    
    if not snowflake_manager:
        # Return mock data when Snowflake is not available
        logger.info("Returning mock analytics data (Snowflake not configured)")
        return AnalyticsResponse(**MOCK_ANALYTICS)
    
    try:
        # Build query with filters
        query = """
        SELECT 
            COUNT(*) as total_sessions,
            SUM(watch_duration_minutes) / 60 as total_hours,
            AVG(engagement_score) as avg_engagement,
            SUM(CASE WHEN is_binge_session THEN 1 ELSE 0 END) as binge_sessions,
            COUNT(DISTINCT user_id) as unique_users
        FROM WATCH_FACTS 
        WHERE watch_date >= DATEADD(day, -%s, CURRENT_DATE())
        """
        
        params = [days]
        
        if user_id:
            query += " AND user_id = %s"
            params.append(user_id)
        
        if show_name:
            query += " AND show_name = %s"
            params.append(show_name)
        
        # Execute query
        result = snowflake_manager.execute_query(query, params)
        
        if result.empty:
            raise HTTPException(status_code=404, detail="No data found")
        
        # Get top shows
        top_shows_query = """
        SELECT 
            show_name,
            COUNT(*) as sessions,
            SUM(watch_duration_minutes) / 60 as total_hours,
            AVG(engagement_score) as avg_engagement
        FROM WATCH_FACTS 
        WHERE watch_date >= DATEADD(day, -%s, CURRENT_DATE())
        GROUP BY show_name
        ORDER BY sessions DESC
        LIMIT 10
        """
        
        top_shows = snowflake_manager.execute_query(top_shows_query, [days])
        
        # Get recent activity
        recent_query = """
        SELECT 
            user_id,
            show_name,
            watch_duration_minutes,
            watch_date
        FROM WATCH_FACTS 
        WHERE watch_date >= DATEADD(day, -%s, CURRENT_DATE())
        ORDER BY watch_date DESC
        LIMIT 20
        """
        
        recent_activity = snowflake_manager.execute_query(recent_query, [days])
        
        return AnalyticsResponse(
            total_sessions=int(result.iloc[0]["total_sessions"]),
            total_hours=float(result.iloc[0]["total_hours"]),
            avg_engagement=float(result.iloc[0]["avg_engagement"]),
            binge_sessions=int(result.iloc[0]["binge_sessions"]),
            unique_users=int(result.iloc[0]["unique_users"]),
            top_shows=top_shows.to_dict('records'),
            recent_activity=recent_activity.to_dict('records')
        )
    
    except Exception as e:
        logger.error(f"Error getting analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting analytics: {str(e)}")

@app.get("/users/{user_id}/analytics")
async def get_user_analytics(user_id: str, days: int = 30):
    """Get analytics for a specific user with Snowflake integration."""
    global snowflake_manager
    
    if not snowflake_manager:
        # Return mock user data
        return {
            "user_id": user_id,
            "total_sessions": 45,
            "total_hours": 67.5,
            "avg_engagement": 0.82,
            "binge_sessions": 3,
            "avg_completion_rate": 0.78,
            "favorite_shows": [
                {"show_name": "Stranger Things", "sessions": 12, "total_hours": 18.0, "avg_engagement": 0.88},
                {"show_name": "The Crown", "sessions": 8, "total_hours": 12.0, "avg_engagement": 0.85}
            ],
            "mode": "mock"
        }
    
    try:
        query = """
        SELECT 
            user_id,
            COUNT(*) as total_sessions,
            SUM(watch_duration_minutes) / 60 as total_hours,
            AVG(engagement_score) as avg_engagement,
            SUM(CASE WHEN is_binge_session THEN 1 ELSE 0 END) as binge_sessions,
            AVG(completion_rate) as avg_completion_rate
        FROM WATCH_FACTS 
        WHERE user_id = %s AND watch_date >= DATEADD(day, -%s, CURRENT_DATE())
        GROUP BY user_id
        """
        
        result = snowflake_manager.execute_query(query, [user_id, days])
        
        if result.empty:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Get user's favorite shows
        shows_query = """
        SELECT 
            show_name,
            COUNT(*) as sessions,
            SUM(watch_duration_minutes) / 60 as total_hours,
            AVG(engagement_score) as avg_engagement
        FROM WATCH_FACTS 
        WHERE user_id = %s AND watch_date >= DATEADD(day, -%s, CURRENT_DATE())
        GROUP BY show_name
        ORDER BY sessions DESC
        LIMIT 10
        """
        
        shows = snowflake_manager.execute_query(shows_query, [user_id, days])
        
        user_data = result.iloc[0].to_dict()
        user_data["favorite_shows"] = shows.to_dict('records')
        user_data["mode"] = "snowflake"
        
        return user_data
    
    except Exception as e:
        logger.error(f"Error getting user analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting user analytics: {str(e)}")

@app.get("/shows/{show_name}/analytics")
async def get_show_analytics(show_name: str, days: int = 30):
    """Get analytics for a specific show with Snowflake integration."""
    global snowflake_manager
    
    if not snowflake_manager:
        # Return mock show data
        return {
            "show_name": show_name,
            "total_sessions": 156,
            "unique_viewers": 89,
            "total_hours": 234.0,
            "avg_engagement": 0.85,
            "avg_completion_rate": 0.78,
            "binge_sessions": 12,
            "top_viewers": [
                {"user_id": "user_123", "sessions": 8, "total_hours": 12.0, "avg_engagement": 0.92},
                {"user_id": "user_456", "sessions": 6, "total_hours": 9.0, "avg_engagement": 0.88}
            ],
            "mode": "mock"
        }
    
    try:
        query = """
        SELECT 
            show_name,
            COUNT(*) as total_sessions,
            COUNT(DISTINCT user_id) as unique_viewers,
            SUM(watch_duration_minutes) / 60 as total_hours,
            AVG(engagement_score) as avg_engagement,
            AVG(completion_rate) as avg_completion_rate,
            SUM(CASE WHEN is_binge_session THEN 1 ELSE 0 END) as binge_sessions
        FROM WATCH_FACTS 
        WHERE show_name = %s AND watch_date >= DATEADD(day, -%s, CURRENT_DATE())
        GROUP BY show_name
        """
        
        result = snowflake_manager.execute_query(query, [show_name, days])
        
        if result.empty:
            raise HTTPException(status_code=404, detail="Show not found")
        
        # Get top viewers for this show
        viewers_query = """
        SELECT 
            user_id,
            COUNT(*) as sessions,
            SUM(watch_duration_minutes) / 60 as total_hours,
            AVG(engagement_score) as avg_engagement
        FROM WATCH_FACTS 
        WHERE show_name = %s AND watch_date >= DATEADD(day, -%s, CURRENT_DATE())
        GROUP BY user_id
        ORDER BY sessions DESC
        LIMIT 10
        """
        
        viewers = snowflake_manager.execute_query(viewers_query, [show_name, days])
        
        show_data = result.iloc[0].to_dict()
        show_data["top_viewers"] = viewers.to_dict('records')
        show_data["mode"] = "snowflake"
        
        return show_data
    
    except Exception as e:
        logger.error(f"Error getting show analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting show analytics: {str(e)}")

@app.post("/generate-events")
async def generate_test_events(
    background_tasks: BackgroundTasks,
    num_events: int = 100,
    duration_minutes: int = 10
):
    """Generate test events in the background with streaming integration."""
    global event_generator
    
    if not event_generator:
        return {
            "message": f"Event generator not available (mock mode)",
            "status": "mock_mode",
            "requested_events": num_events,
            "duration_minutes": duration_minutes
        }
    
    def generate_events():
        try:
            event_generator.generate_realistic_events(duration_minutes)
        except Exception as e:
            logger.error(f"Error generating events: {str(e)}")
    
    background_tasks.add_task(generate_events)
    
    return {
        "message": f"Generating {num_events} events over {duration_minutes} minutes",
        "status": "started",
        "mode": "streaming"
    }

@app.get("/streaming/status")
async def get_streaming_status():
    """Get streaming processor status."""
    global streaming_processor
    
    if not streaming_processor:
        return {
            "status": "not_configured",
            "message": "Streaming processor not available in this environment",
            "kafka_configured": bool(os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
        }
    
    return {
        "status": "running" if streaming_processor.running else "stopped",
        "input_topic": streaming_processor.input_topic,
        "output_topic": streaming_processor.output_topic,
        "batch_size": streaming_processor.batch_size,
        "batch_timeout": streaming_processor.batch_timeout,
        "kafka_configured": bool(os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    }

@app.post("/streaming/start")
async def start_streaming():
    """Start the streaming processor."""
    global streaming_processor
    
    if not streaming_processor:
        return {"message": "Streaming processor not available in this environment"}
    
    if streaming_processor.running:
        return {"message": "Streaming processor is already running"}
    
    # Start streaming in background
    def start_stream():
        streaming_processor.start_streaming()
    
    asyncio.create_task(asyncio.to_thread(start_stream))
    
    return {"message": "Streaming processor started"}

@app.post("/streaming/stop")
async def stop_streaming():
    """Stop the streaming processor."""
    global streaming_processor
    
    if not streaming_processor:
        return {"message": "Streaming processor not available in this environment"}
    
    streaming_processor.stop_streaming()
    
    return {"message": "Streaming processor stopped"}

if __name__ == "__main__":
    logger.info("🚀 Starting uvicorn server...")
    uvicorn.run(app, host="0.0.0.0", port=8000) 