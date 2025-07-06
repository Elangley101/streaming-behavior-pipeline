#!/usr/bin/env python3
"""
Replace the logging setup with a simpler, more robust version.
"""

def replace_logging_setup():
    """Replace the logging setup with a simpler version."""
    
    # Read the current file
    with open('src/api_service.py', 'r') as f:
        content = f.read()
    
    # Define the new logging setup
    new_logging_setup = '''# Simple logging setup
import logging

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(name)s] %(levelname)s: %(message)s'
)

logger = logging.getLogger("api_service")

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
    
    # Find the old logging setup and replace it
    import_lines = []
    lines = content.split('\n')
    in_imports = True
    old_logging_start = -1
    old_logging_end = -1
    
    for i, line in enumerate(lines):
        if in_imports and line.strip() and not line.startswith('import') and not line.startswith('from'):
            in_imports = False
            old_logging_start = i
            break
        if in_imports:
            import_lines.append(line)
    
    # Find where the old logging setup ends
    for i in range(old_logging_start, len(lines)):
        if 'logger.info("🚀 Starting Netflix Analytics API initialization...")' in lines[i]:
            old_logging_end = i + 1
            break
    
    # Reconstruct the file
    new_content = '\n'.join(import_lines) + '\n\n' + new_logging_setup + '\n\n'
    
    # Add the rest of the file after the logging setup
    if old_logging_end < len(lines):
        new_content += '\n'.join(lines[old_logging_end:])
    
    # Write the fixed file
    with open('src/api_service.py', 'w') as f:
        f.write(new_content)
    
    print("✅ Logging setup replaced with simple, robust version!")

if __name__ == "__main__":
    replace_logging_setup() 