#!/usr/bin/env python3
"""
Netflix Behavior Pipeline - Demo Script
This script demonstrates the key capabilities of the data engineering pipeline.
"""

import subprocess
import sys
import os
from pathlib import Path

def print_header():
    """Print a nice header for the demo."""
    print("=" * 80)
    print("🎬 Netflix Behavior Pipeline - Demo")
    print("=" * 80)
    print("This project demonstrates modern data engineering best practices:")
    print("✅ End-to-end data pipeline (ETL + Real-time)")
    print("✅ Modern tech stack (FastAPI, Streamlit, dbt, Snowflake)")
    print("✅ Production-ready architecture")
    print("✅ Interactive analytics dashboard")
    print("✅ Data quality monitoring")
    print("✅ Comprehensive testing")
    print("=" * 80)

def check_dependencies():
    """Check if required dependencies are installed."""
    print("🔍 Checking dependencies...")
    
    # Core dependencies (required for demo)
    core_packages = ['streamlit', 'pandas', 'plotly', 'fastapi']
    
    # Optional dependencies (enhance functionality)
    optional_packages = ['dbt-core', 'snowflake-connector-python']
    
    missing_core = []
    missing_optional = []
    
    # Check core dependencies
    for package in core_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} - missing")
            missing_core.append(package)
    
    # Check optional dependencies
    for package in optional_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✅ {package} (optional)")
        except ImportError:
            print(f"⚠️  {package} - optional, not installed")
            missing_optional.append(package)
    
    if missing_core:
        print(f"\n❌ Missing core packages: {', '.join(missing_core)}")
        print("Run: pip install -r requirements.txt")
        return False
    
    if missing_optional:
        print(f"\n💡 Optional packages not installed: {', '.join(missing_optional)}")
        print("These are not required for the demo but enhance functionality.")
    
    print("✅ All core dependencies installed!")
    return True

def show_project_structure():
    """Show the project structure."""
    print("\n📁 Project Structure:")
    print("streaming-behavior-pipeline/")
    print("├── src/                    # Application code")
    print("│   ├── dashboard.py        # Interactive analytics dashboard")
    print("│   ├── api_service.py      # FastAPI REST API")
    print("│   └── ...")
    print("├── models/                 # dbt data models")
    print("│   ├── staging/           # Raw data models")
    print("│   ├── intermediate/      # Transformation models")
    print("│   └── marts/             # Business-ready models")
    print("├── tests/                 # Unit and integration tests")
    print("├── config/                # Configuration files")
    print("├── docs/                  # Documentation")
    print("└── deployment/            # Deployment configs")

def show_key_features():
    """Show key features of the project."""
    print("\n🚀 Key Features:")
    
    features = [
        "Real-time streaming with Apache Kafka",
        "Batch ETL processing with data quality checks",
        "Interactive Streamlit dashboard with multiple views",
        "RESTful API with FastAPI",
        "Data modeling with dbt and Snowflake",
        "Docker containerization",
        "Comprehensive testing suite",
        "Production-ready monitoring and logging",
        "Cloud deployment ready (AWS/GCP)",
        "Data quality monitoring and alerting"
    ]
    
    for i, feature in enumerate(features, 1):
        print(f"{i:2d}. {feature}")

def show_tech_stack():
    """Show the technology stack."""
    print("\n🛠️  Technology Stack:")
    
    tech_stack = {
        "Backend": ["FastAPI", "Python 3.11+", "uvicorn"],
        "Frontend": ["Streamlit", "Plotly", "Altair"],
        "Data Processing": ["Pandas", "NumPy", "PyArrow"],
        "Data Warehouse": ["Snowflake", "dbt"],
        "Streaming": ["Apache Kafka"],
        "Containerization": ["Docker", "docker-compose"],
        "Monitoring": ["Prometheus", "Structured Logging"],
        "Testing": ["pytest", "dbt tests"],
        "Deployment": ["Kubernetes", "Cloud Platforms"]
    }
    
    for category, technologies in tech_stack.items():
        print(f"  {category}: {', '.join(technologies)}")

def show_business_value():
    """Show the business value of the project."""
    print("\n💼 Business Value:")
    
    value_points = [
        "Real-time user behavior analytics",
        "Content performance insights",
        "User engagement scoring and segmentation",
        "Binge watching pattern detection",
        "Data-driven content recommendations",
        "Operational monitoring and alerting",
        "Scalable architecture for growth",
        "Cost-effective cloud deployment"
    ]
    
    for point in value_points:
        print(f"  • {point}")

def show_career_impact():
    """Show how this project impacts career prospects."""
    print("\n🎯 Career Impact:")
    
    impact_points = [
        "Demonstrates end-to-end data engineering skills",
        "Shows modern technology stack proficiency",
        "Proves production-ready thinking",
        "Exhibits business value creation",
        "Portfolio-worthy project for interviews",
        "Relevant to Netflix, Amazon, Disney+ type companies",
        "Shows cloud-native architecture understanding",
        "Demonstrates testing and quality assurance skills"
    ]
    
    for point in impact_points:
        print(f"  • {point}")

def show_next_steps():
    """Show next steps for the project."""
    print("\n🔮 Next Steps & Enhancements:")
    
    enhancements = [
        "Add Apache Airflow for orchestration",
        "Implement authentication and authorization",
        "Add machine learning models for recommendations",
        "Integrate Great Expectations for data quality",
        "Set up CI/CD pipeline automation",
        "Add Infrastructure as Code (Terraform)",
        "Implement data catalog and lineage tracking",
        "Add advanced monitoring (Grafana, distributed tracing)"
    ]
    
    for i, enhancement in enumerate(enhancements, 1):
        print(f"{i:2d}. {enhancement}")

def main():
    """Main demo function."""
    print_header()
    
    if not check_dependencies():
        print("\n❌ Please install missing dependencies before running the demo.")
        sys.exit(1)
    
    show_project_structure()
    show_key_features()
    show_tech_stack()
    show_business_value()
    show_career_impact()
    show_next_steps()
    
    print("\n" + "=" * 80)
    print("🎉 Demo complete! This project showcases professional data engineering skills.")
    print("💡 Run 'streamlit run src/dashboard.py' to see the interactive dashboard.")
    print("📚 Check the documentation in ARCHITECTURE.md and DEPLOYMENT.md")
    print("=" * 80)

if __name__ == "__main__":
    main() 