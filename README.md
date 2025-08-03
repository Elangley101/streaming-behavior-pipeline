# Netflix-Style Streaming Analytics Pipeline

[![Build Status](https://github.com/<your-username>/<your-repo>/actions/workflows/ci.yml/badge.svg)](https://github.com/<your-username>/<your-repo>/actions)
[![Test Coverage](https://img.shields.io/badge/coverage-90%25-brightgreen)](https://github.com/<your-username>/<your-repo>/actions)

## 🚀 Project Overview
A modern, end-to-end data engineering pipeline that ingests, transforms, and analyzes streaming behavior data (inspired by Netflix). Built with Snowflake, DBT, and Streamlit, this project demonstrates best practices in data modeling, quality, automation, and analytics delivery.

## 💡 Business Value
- **Understand user engagement** and content performance for a streaming platform.
- **Enable data-driven decisions** for content, marketing, and product teams.
- **Showcase robust, production-ready data engineering skills.**

## 🏗️ Architecture
```mermaid
graph TD
    A[Raw Data (User Watch Sessions)] --> B[Snowflake: USER_WATCH_SESSIONS]
    B --> C[DBT: Transformations]
    C --> D1[FACT_USER_WATCH_SESSIONS]
    C --> D2[DIM_USERS]
    C --> D3[DIM_SHOWS]
    C --> D4[MART_CONTENT_ANALYTICS]
    C --> D5[MART_USER_ANALYTICS]
    D1 & D2 & D3 & D4 & D5 --> E[Streamlit Dashboards]
    C --> F[Data Quality Dashboard]
    C --> G[Data Lineage Tracking]
```

## 📊 Dashboards
- **Interactive Analytics Dashboard:** `src/dashboard.py` - Features multiple views:
  - **Main Analytics:** Comprehensive user behavior and content performance analytics
  - **SQL Analytics:** Sample SQL queries and database analytics
  - **Data Quality:** Data quality monitoring and alerts

## 🛠️ Setup Instructions
1. **Clone the repo:**
   ```sh
   git clone <your-repo-url>
   cd streaming-behavior-pipeline
   ```
2. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```
3. **Configure environment:**
   - Copy `.env.example` to `.env` and fill in your Snowflake credentials.
   - Ensure `profiles.yml` is set for your Snowflake account.
4. **Load sample data:**
   ```sh
   python load_snowflake_data.py
   ```
5. **Run DBT models:**
   ```sh
   dbt run
   ```
6. **Launch dashboard:**
   ```sh
   streamlit run src/dashboard.py
   ```
   The dashboard includes multiple views accessible via the sidebar dropdown.

## 🔄 Pipeline Flow
1. **Raw data** is loaded into `USER_WATCH_SESSIONS` (Snowflake).
2. **DBT** transforms raw data into facts, dims, and marts.
3. **Dashboards** (Streamlit) provide analytics, data quality, and lineage insights.
4. **Automated tests** and data quality checks ensure trust and reliability.

## 📊 Dashboards
- **Main Analytics:** `src/dashboard.py`
- **SQL/DBT Analytics:** `src/sql_dashboard.py`
- **Data Quality:** `src/data_quality_dashboard.py`

## ☁️ Cloud & Deployment Notes
- Designed for Snowflake, but can be adapted for BigQuery, Redshift, or Databricks.
- Docker Compose included for local orchestration.
- See `DEPLOYMENT.md` for cloud deployment tips.

## 🔒 Secrets & Security
- `.env` and credentials are gitignored.
- Never commit secrets to version control.

## 🧪 Testing & Data Quality
- Unit tests in `tests/`
- DBT model tests for nulls, uniqueness, relationships
- Data quality dashboard with advanced checks (nulls, duplicates, ranges, anomalies, referential integrity)

## 🧬 Data Lineage
- Visualize data flow and transformations in the dashboard.

## 🏆 Project Summary
This project demonstrates:
- Modern data engineering (Snowflake, DBT, Streamlit)
- Data quality, testing, and automation
- Cloud-readiness and production best practices
- Business impact through analytics

---

*For more details, see the data dictionary, architecture diagram, and code comments throughout the repo.*