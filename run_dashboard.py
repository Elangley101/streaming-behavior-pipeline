#!/usr/bin/env python3
"""
Simple script to run the dashboard from the project root.
This avoids import issues on EC2.
"""

import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Now import and run the dashboard
from dashboard import main

if __name__ == "__main__":
    main() 