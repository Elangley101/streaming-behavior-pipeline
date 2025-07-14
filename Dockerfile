# Use Python 3.11 slim image for better DBT compatibility
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install setuptools to ensure distutils is available (backup fix)
RUN pip install --no-cache-dir setuptools

# Copy application code
COPY . .

# Install dbt packages (temporarily disabled due to packages.yml issues)
# RUN dbt deps

# Create necessary directories and set permissions
RUN mkdir -p data/raw data/processed logs && \
    chmod 755 data/raw data/processed logs

# Create sample data file during build
RUN echo "user_id,show_name,watch_duration_minutes,watch_date" > data/raw/watch_logs.csv && \
    echo "user_001,Stranger Things,45,2024-01-15 20:30:00" >> data/raw/watch_logs.csv && \
    echo "user_002,The Crown,60,2024-01-15 19:15:00" >> data/raw/watch_logs.csv && \
    echo "user_003,Wednesday,30,2024-01-15 21:00:00" >> data/raw/watch_logs.csv && \
    echo "user_004,Bridgerton,90,2024-01-15 18:45:00" >> data/raw/watch_logs.csv && \
    echo "user_005,The Witcher,75,2024-01-15 22:15:00" >> data/raw/watch_logs.csv && \
    echo "user_006,Squid Game,120,2024-01-15 23:00:00" >> data/raw/watch_logs.csv && \
    echo "user_007,Black Mirror,45,2024-01-16 20:00:00" >> data/raw/watch_logs.csv && \
    echo "user_008,The Umbrella Academy,90,2024-01-16 21:30:00" >> data/raw/watch_logs.csv && \
    echo "user_009,You,60,2024-01-16 22:15:00" >> data/raw/watch_logs.csv && \
    echo "user_010,Sex Education,30,2024-01-16 19:45:00" >> data/raw/watch_logs.csv && \
    chmod 644 data/raw/watch_logs.csv

# Expose ports
EXPOSE 8000 8501

# Create entrypoint script
RUN echo '#!/bin/bash\n\
if [ "$1" = "api" ]; then\n\
    exec python src/api_service.py\n\
elif [ "$1" = "dashboard" ]; then\n\
    exec streamlit run src/dashboard.py --server.port 8501 --server.address 0.0.0.0\n\
elif [ "$1" = "sql-dashboard" ]; then\n\
    exec streamlit run src/sql_dashboard.py --server.port 8501 --server.address 0.0.0.0\n\
elif [ "$1" = "data-quality-dashboard" ]; then\n\
    exec streamlit run src/data_quality_dashboard.py --server.port 8501 --server.address 0.0.0.0\n\
elif [ "$1" = "pipeline" ]; then\n\
    exec python src/etl_runner.py\n\
else\n\
    echo "Usage: docker run <image> [api|dashboard|sql-dashboard|data-quality-dashboard|pipeline]"\n\
    exit 1\n\
fi' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Default command
CMD ["api"] 