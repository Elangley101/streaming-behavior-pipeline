# Makefile for Netflix Analytics Pipeline

.PHONY: setup load-data dbt-run dashboard sql-dashboard dq-dashboard test format

setup:
	pip install -r requirements.txt

load-data:
	python load_snowflake_data.py

dbt-run:
	dbt run

dashboards: dashboard sql-dashboard dq-dashboard

dashboard:
	streamlit run src/dashboard.py

sql-dashboard:
	streamlit run src/sql_dashboard.py --server.port 8502

dq-dashboard:
	streamlit run src/data_quality_dashboard.py --server.port 8503

test:
	pytest tests/

format:
	black src/ tests/ 