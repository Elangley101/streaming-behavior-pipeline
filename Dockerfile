# Use Python 3.11 slim image
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

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p data/raw data/processed logs

# Expose ports
EXPOSE 8000 8501

# Create entrypoint script
RUN echo '#!/bin/bash\n\
if [ "$1" = "api" ]; then\n\
    exec python src/api_service.py\n\
elif [ "$1" = "dashboard" ]; then\n\
    exec streamlit run src/dashboard.py --server.port 8501 --server.address 0.0.0.0\n\
elif [ "$1" = "pipeline" ]; then\n\
    exec python src/etl_runner.py\n\
else\n\
    echo "Usage: docker run <image> [api|dashboard|pipeline]"\n\
    exit 1\n\
fi' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Default command
CMD ["api"] 