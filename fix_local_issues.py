#!/usr/bin/env python3
"""
Fix local file permission and API service issues.
"""

import os
import subprocess

def fix_local_issues():
    """Fix local file permission and API service issues."""
    
    print("🔧 Fixing local issues...")
    
    # 1. Create data directory with proper permissions
    print("📁 Creating data directory...")
    os.makedirs('data/raw', exist_ok=True)
    subprocess.run(['chmod', '755', 'data/raw'])
    
    # 2. Create sample data file
    print("📄 Creating sample data file...")
    sample_data = """user_id,show_name,watch_duration_minutes,watch_date
user_001,Stranger Things,45,2024-01-15 20:30:00
user_002,The Crown,60,2024-01-15 19:15:00
user_003,Wednesday,30,2024-01-15 21:00:00
user_004,Bridgerton,90,2024-01-15 18:45:00
user_005,The Witcher,75,2024-01-15 22:15:00
user_006,Squid Game,120,2024-01-15 23:00:00
user_007,Black Mirror,45,2024-01-16 20:00:00
user_008,The Umbrella Academy,90,2024-01-16 21:30:00
user_009,You,60,2024-01-16 22:15:00
user_010,Sex Education,30,2024-01-16 19:45:00"""
    
    with open('data/raw/watch_logs.csv', 'w') as f:
        f.write(sample_data)
    
    subprocess.run(['chmod', '644', 'data/raw/watch_logs.csv'])
    
    # 3. Fix API service logging issue
    print("🔧 Fixing API service logging...")
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
    
    content = content.replace(old_logging, new_logging)
    
    with open('src/api_service.py', 'w') as f:
        f.write(content)
    
    print("✅ All issues fixed!")
    print("📊 Sample data created with 10 records")
    print("🔧 API service logging fixed")
    print("📁 Directory permissions set correctly")

if __name__ == "__main__":
    fix_local_issues() 