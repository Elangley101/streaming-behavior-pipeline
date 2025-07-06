#!/usr/bin/env python3
"""
Fix the indentation error in the API service logging setup.
"""

def fix_logging_indentation():
    """Fix the indentation error in the logging setup."""
    
    # Read the current file
    with open('src/api_service.py', 'r') as f:
        lines = f.readlines()
    
    # Find and fix the logging setup
    fixed_lines = []
    in_logging_section = False
    logging_start = -1
    
    for i, line in enumerate(lines):
        if 'def setup_logging(name):' in line:
            in_logging_section = True
            logging_start = i
            fixed_lines.append(line)
        elif in_logging_section and 'return log' in line:
            in_logging_section = False
            fixed_lines.append(line)
        elif in_logging_section and 'log.info = lambda' in line:
            # Fix the indentation for lambda assignments
            fixed_lines.append('    log.info = lambda msg: log(msg, "INFO")\n')
        elif in_logging_section and 'log.warning = lambda' in line:
            fixed_lines.append('    log.warning = lambda msg: log(msg, "WARNING")\n')
        elif in_logging_section and 'log.error = lambda' in line:
            fixed_lines.append('    log.error = lambda msg: log(msg, "ERROR")\n')
        elif in_logging_section and 'log.debug = lambda' in line:
            fixed_lines.append('    log.debug = lambda msg: log(msg, "DEBUG")\n')
        else:
            fixed_lines.append(line)
    
    # Write the fixed file
    with open('src/api_service.py', 'w') as f:
        f.writelines(fixed_lines)
    
    print("✅ Logging indentation fixed!")

if __name__ == "__main__":
    fix_logging_indentation() 