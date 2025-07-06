.PHONY: help install test lint clean docker-build docker-run docker-stop generate-data api dashboard pipeline all

help: ## Show this help message
	@echo "Streamlytics - Real-Time Streaming Analytics Pipeline"
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install Python dependencies
	pip install -r requirements.txt

test: ## Run tests
	pytest tests/ -v --cov=src --cov-report=term-missing

lint: ## Run code linting
	black --check src/ tests/
	isort --check-only src/ tests/
	flake8 src/ tests/ --max-line-length=88 --extend-ignore=E203,W503

format: ## Format code
	black src/ tests/
	isort src/ tests/

clean: ## Clean up generated files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov

docker-build: ## Build Docker image
	docker build -t streamlytics:latest .

docker-run: ## Start all services with Docker Compose
	docker-compose up -d

docker-stop: ## Stop all services
	docker-compose down

docker-logs: ## Show logs from all services
	docker-compose logs -f

generate-data: ## Generate sample data
	python src/generate_sample_data.py

api: ## Run API service locally
	python src/api_service.py

dashboard: ## Run dashboard locally
	streamlit run src/dashboard.py

pipeline: ## Run ETL pipeline
	python src/etl_runner.py

setup: ## Complete setup (install, generate data, start services)
	$(MAKE) install
	$(MAKE) generate-data
	$(MAKE) docker-run
	@echo "✅ Streamlytics is ready!"
	@echo "📊 Dashboard: http://localhost:8502"
	@echo "🔌 API Docs: http://localhost:8000/docs"
	@echo "📈 Kafka UI: http://localhost:8080"

all: clean install test lint docker-build ## Run all checks and build
	@echo "✅ All checks passed!"

demo: ## Run a complete demo
	$(MAKE) setup
	@echo "🎬 Demo is running! Check the dashboard at http://localhost:8502" 