from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uvicorn
from datetime import datetime, timedelta
import asyncio
import json

from src.snowflake_manager import SnowflakeManager
from src.streaming_processor import StreamingProcessor, EventGenerator
from src.utils import PipelineError, setup_logging

logger = setup_logging("api_service")

# FastAPI app
app = FastAPI(
    title="Netflix Analytics API",
    description="Real-time analytics API for Netflix-style behavioral data",
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

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    global snowflake_manager, streaming_processor, event_generator
    
    try:
        # Initialize Snowflake manager
        snowflake_manager = SnowflakeManager()
        logger.info("Snowflake manager initialized")
    except Exception as e:
        logger.warning(f"Could not initialize Snowflake: {str(e)}")
    
    try:
        # Initialize streaming processor
        streaming_processor = StreamingProcessor()
        logger.info("Streaming processor initialized")
    except Exception as e:
        logger.warning(f"Could not initialize streaming processor: {str(e)}")
    
    try:
        # Initialize event generator
        event_generator = EventGenerator()
        logger.info("Event generator initialized")
    except Exception as e:
        logger.warning(f"Could not initialize event generator: {str(e)}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global snowflake_manager, streaming_processor, event_generator
    
    if streaming_processor:
        streaming_processor.stop_streaming()
    
    if snowflake_manager:
        snowflake_manager.close()

@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint."""
    return {
        "message": "Netflix Analytics API",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    services = {}
    
    # Check Snowflake
    if snowflake_manager:
        try:
            # Simple query to test connection
            result = snowflake_manager.execute_query("SELECT 1 as test")
            services["snowflake"] = "healthy"
        except Exception as e:
            services["snowflake"] = f"unhealthy: {str(e)}"
    else:
        services["snowflake"] = "not_initialized"
    
    # Check streaming processor
    if streaming_processor:
        services["streaming"] = "healthy"
    else:
        services["streaming"] = "not_initialized"
    
    return HealthResponse(
        status="healthy" if all("healthy" in status for status in services.values()) else "degraded",
        timestamp=datetime.now(),
        services=services
    )

@app.post("/events", response_model=Dict[str, str])
async def create_watch_event(event: WatchEventRequest):
    """Create a new watch event."""
    global streaming_processor
    
    if not streaming_processor:
        raise HTTPException(status_code=503, detail="Streaming processor not available")
    
    try:
        # Send event to Kafka
        event_dict = event.dict()
        streaming_processor.send_event("watch_events", event_dict, key=event.user_id)
        
        return {"message": "Event created successfully", "event_id": event_dict.get("event_id")}
    
    except Exception as e:
        logger.error(f"Error creating event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating event: {str(e)}")

@app.get("/analytics", response_model=AnalyticsResponse)
async def get_analytics(
    days: int = 7,
    user_id: Optional[str] = None,
    show_name: Optional[str] = None
):
    """Get analytics data."""
    global snowflake_manager
    
    if not snowflake_manager:
        raise HTTPException(status_code=503, detail="Snowflake not available")
    
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
            engagement_score,
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
    """Get analytics for a specific user."""
    global snowflake_manager
    
    if not snowflake_manager:
        raise HTTPException(status_code=503, detail="Snowflake not available")
    
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
        
        return user_data
    
    except Exception as e:
        logger.error(f"Error getting user analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting user analytics: {str(e)}")

@app.get("/shows/{show_name}/analytics")
async def get_show_analytics(show_name: str, days: int = 30):
    """Get analytics for a specific show."""
    global snowflake_manager
    
    if not snowflake_manager:
        raise HTTPException(status_code=503, detail="Snowflake not available")
    
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
    """Generate test events in the background."""
    global event_generator
    
    if not event_generator:
        raise HTTPException(status_code=503, detail="Event generator not available")
    
    def generate_events():
        try:
            event_generator.generate_realistic_events(duration_minutes)
        except Exception as e:
            logger.error(f"Error generating events: {str(e)}")
    
    background_tasks.add_task(generate_events)
    
    return {
        "message": f"Generating {num_events} events over {duration_minutes} minutes",
        "status": "started"
    }

@app.get("/streaming/status")
async def get_streaming_status():
    """Get streaming processor status."""
    global streaming_processor
    
    if not streaming_processor:
        return {"status": "not_initialized"}
    
    return {
        "status": "running" if streaming_processor.running else "stopped",
        "input_topic": streaming_processor.input_topic,
        "output_topic": streaming_processor.output_topic,
        "batch_size": streaming_processor.batch_size,
        "batch_timeout": streaming_processor.batch_timeout
    }

@app.post("/streaming/start")
async def start_streaming():
    """Start the streaming processor."""
    global streaming_processor
    
    if not streaming_processor:
        raise HTTPException(status_code=503, detail="Streaming processor not available")
    
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
        raise HTTPException(status_code=503, detail="Streaming processor not available")
    
    streaming_processor.stop_streaming()
    
    return {"message": "Streaming processor stopped"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 