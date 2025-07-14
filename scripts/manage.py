#!/usr/bin/env python3
import argparse
import subprocess
import sys

parser = argparse.ArgumentParser(description="Manage Netflix Analytics Pipeline tasks.")
subparsers = parser.add_subparsers(dest="command")

subparsers.add_parser("load-data", help="Load sample data into Snowflake")
subparsers.add_parser("dbt-run", help="Run DBT models")
subparsers.add_parser("dashboard", help="Launch main analytics dashboard")
subparsers.add_parser("sql-dashboard", help="Launch SQL dashboard")
subparsers.add_parser("dq-dashboard", help="Launch data quality dashboard")
subparsers.add_parser("test", help="Run all tests")
subparsers.add_parser("format", help="Format code with black")

args = parser.parse_args()

if args.command == "load-data":
    subprocess.run([sys.executable, "load_snowflake_data.py"])
elif args.command == "dbt-run":
    subprocess.run(["dbt", "run"])
elif args.command == "dashboard":
    subprocess.run(["streamlit", "run", "src/dashboard.py"])
elif args.command == "sql-dashboard":
    subprocess.run(["streamlit", "run", "src/sql_dashboard.py", "--server.port", "8502"])
elif args.command == "dq-dashboard":
    subprocess.run(["streamlit", "run", "src/data_quality_dashboard.py", "--server.port", "8503"])
elif args.command == "test":
    subprocess.run(["pytest", "tests/"])
elif args.command == "format":
    subprocess.run(["black", "src/", "tests/"])
else:
    parser.print_help() 