# 🚀 Netflix Analytics Pipeline - Deployment Guide

This guide will help you deploy the Netflix-style behavioral data pipeline with all its components.

## 📋 Prerequisites

- Docker and Docker Compose installed
- Python 3.11+ (for local development)
- Snowflake account (optional, for data warehousing)
- Git

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Kafka Stream  │    │   Snowflake     │
│   (CSV, API)    │───▶│   Processing    │───▶│   Data Warehouse│
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   FastAPI       │
                       │   REST API      │
                       └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   Streamlit     │
                       │   Dashboard     │
                       └─────────────────┘
```

## 🐳 Quick Start with Docker

### 1. Clone and Setup

```bash
git clone <your-repo-url>
cd netflix-behavior-pipeline
```

### 2. Environment Configuration

Create a `.env` file in the root directory:

```bash
# Snowflake Configuration (Optional)
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=NETFLIX_ANALYTICS
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=ACCOUNTADMIN

# Pipeline Configuration
LOG_LEVEL=INFO
BATCH_SIZE=10000
BINGE_WATCH_THRESHOLD_MINUTES=120
COMPLETION_RATE_THRESHOLD=0.8
```

### 3. Start All Services

```bash
docker-compose up -d
```

This will start:
- **Zookeeper** (port 2181)
- **Kafka** (port 9092)
- **Kafka UI** (port 8080) - Monitor Kafka topics
- **Netflix Analytics API** (port 8000)
- **Dashboard** (port 8501)
- **ETL Pipeline** (background service)

### 4. Access Services

- **Dashboard**: http://localhost:8501
- **API Documentation**: http://localhost:8000/docs
- **Kafka UI**: http://localhost:8080
- **API Health Check**: http://localhost:8000/health

## 🔧 Local Development Setup

### 1. Python Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Start Kafka (Required for streaming)

```bash
docker-compose up -d zookeeper kafka kafka-ui
```

### 3. Run Individual Components

#### API Service
```bash
python src/api_service.py
```

#### Dashboard
```bash
streamlit run src/dashboard.py
```

#### ETL Pipeline
```bash
python src/etl_runner.py --snowflake
```

#### Generate Sample Data
```bash
python src/generate_sample_data.py
```

## 📊 Using the Dashboard

### 1. Data Source Selection
- **Sample Data**: Uses generated data for demonstration
- **Snowflake**: Connects to your Snowflake instance
- **Parquet Files**: Uses processed data files

### 2. Filters
- **Date Range**: Select time period for analysis
- **User Filter**: Filter by specific users
- **Show Filter**: Filter by specific shows

### 3. Analytics Tabs
- **Overview**: General metrics and distributions
- **User Analytics**: User engagement and behavior patterns
- **Content Analytics**: Show performance and completion rates
- **Time Patterns**: Hourly and daily viewing patterns

## 🔌 API Endpoints

### Health Check
```bash
curl http://localhost:8000/health
```

### Get Analytics
```bash
curl "http://localhost:8000/analytics?days=7"
```

### Create Watch Event
```bash
curl -X POST "http://localhost:8000/events" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_0001",
    "show_name": "Stranger Things",
    "watch_duration_minutes": 45.5,
    "watch_date": "2024-01-15T20:30:00"
  }'
```

### User Analytics
```bash
curl "http://localhost:8000/users/user_0001/analytics?days=30"
```

### Show Analytics
```bash
curl "http://localhost:8000/shows/Stranger%20Things/analytics?days=30"
```

## 🔄 Streaming Data Processing

### 1. Start Streaming Processor
```bash
curl -X POST "http://localhost:8000/streaming/start"
```

### 2. Generate Test Events
```bash
curl -X POST "http://localhost:8000/generate-events?num_events=100&duration_minutes=10"
```

### 3. Monitor Kafka Topics
Visit http://localhost:8080 to see:
- `watch_events`: Raw incoming events
- `processed_events`: Transformed events
- `error_events`: Failed event processing

## 🗄️ Snowflake Integration

### 1. Database Setup
The pipeline automatically creates:
- `WATCH_FACTS`: Main fact table
- `USER_DIM`: User dimension table
- `SHOW_DIM`: Show dimension table

### 2. Data Loading
Data is automatically loaded to Snowflake when:
- Running the ETL pipeline with `--snowflake` flag
- Processing streaming events
- Using the API to create events

### 3. Query Examples

```sql
-- Get top shows by engagement
SELECT 
    show_name,
    AVG(engagement_score) as avg_engagement,
    COUNT(*) as total_sessions
FROM WATCH_FACTS 
WHERE watch_date >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY show_name
ORDER BY avg_engagement DESC;

-- Get binge watching patterns
SELECT 
    user_id,
    COUNT(*) as total_sessions,
    SUM(CASE WHEN is_binge_session THEN 1 ELSE 0 END) as binge_sessions
FROM WATCH_FACTS 
GROUP BY user_id
HAVING binge_sessions > 0;
```

## 🧪 Testing

### Run Unit Tests
```bash
pytest tests/
```

### Run with Coverage
```bash
pytest --cov=src tests/
```

### Integration Testing
```bash
# Start services
docker-compose up -d

# Run integration tests
python -m pytest tests/test_integration.py -v
```

## 📈 Monitoring and Logging

### Logs Location
- Application logs: `./logs/pipeline.log`
- Docker logs: `docker-compose logs -f netflix-analytics`

### Key Metrics
- Event processing rate
- Data quality metrics
- API response times
- Snowflake query performance

### Health Checks
```bash
# API health
curl http://localhost:8000/health

# Streaming status
curl http://localhost:8000/streaming/status
```

## 🔒 Security Considerations

### Environment Variables
- Never commit `.env` files to version control
- Use secure password management
- Rotate credentials regularly

### Network Security
- Use HTTPS in production
- Implement authentication for API endpoints
- Restrict database access

### Data Privacy
- Anonymize user data
- Implement data retention policies
- Follow GDPR compliance

## 🚀 Production Deployment

### 1. Cloud Deployment (AWS Example)

```bash
# Build and push to ECR
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin <account>.dkr.ecr.us-west-2.amazonaws.com
docker build -t netflix-analytics .
docker tag netflix-analytics:latest <account>.dkr.ecr.us-west-2.amazonaws.com/netflix-analytics:latest
docker push <account>.dkr.ecr.us-west-2.amazonaws.com/netflix-analytics:latest
```

### 2. Kubernetes Deployment
```bash
kubectl apply -f k8s/
```

### 3. CI/CD Pipeline
```bash
# Example GitHub Actions workflow
name: Deploy Pipeline
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Build and deploy
        run: |
          docker build -t netflix-analytics .
          # Deploy to your cloud platform
```

## 🛠️ Troubleshooting

### Common Issues

1. **Kafka Connection Failed**
   ```bash
   # Check if Kafka is running
   docker-compose ps
   # Restart Kafka
   docker-compose restart kafka
   ```

2. **Snowflake Connection Issues**
   - Verify credentials in `.env`
   - Check network connectivity
   - Ensure warehouse is running

3. **Dashboard Not Loading**
   - Check if Streamlit is running on correct port
   - Verify data source configuration
   - Check browser console for errors

### Performance Optimization

1. **Increase Kafka Partitions**
   ```bash
   # In Kafka UI, create topics with more partitions
   ```

2. **Optimize Snowflake Queries**
   - Use clustering keys
   - Implement query caching
   - Monitor warehouse usage

3. **Scale Services**
   ```bash
   # Scale API service
   docker-compose up -d --scale netflix-analytics=3
   ```

## 📚 Additional Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Snowflake Documentation](https://docs.snowflake.com/)

## 🤝 Support

For issues and questions:
1. Check the troubleshooting section
2. Review logs in `./logs/`
3. Open an issue in the repository
4. Contact the development team

---

**Happy Streaming! 🎬** 