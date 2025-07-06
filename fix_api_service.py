#!/usr/bin/env python3
"""
Fix the API service logging issue.
"""

def fix_api_service():
    """Fix the API service logging issue."""
    
    # Read the current file
    with open('src/api_service.py', 'r') as f:
        content = f.read()
    
    # Replace the problematic logging setup
    old_logging = '''# Simple logging function that always works
def setup_logging(name):
    def log(message):
        print(f"[{name}] {message}")
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

logger.info("🚀 Starting Netflix Analytics API initialization...")'''

    new_logging = '''# Simple logging function that always works
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

logger.info("🚀 Starting Netflix Analytics API initialization...")'''
    
    # Replace the content
    content = content.replace(old_logging, new_logging)
    
    # Write the fixed file
    with open('src/api_service.py', 'w') as f:
        f.write(content)
    
    print("✅ API service logging issue fixed!")

if __name__ == "__main__":
    fix_api_service() 