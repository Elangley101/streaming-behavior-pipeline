#!/bin/bash

# Netflix Behavioral Data Pipeline - Production Deployment Script
# This script deploys the pipeline to production environments

set -e  # Exit on any error

# Configuration
PROJECT_NAME="netflix-behavior-pipeline"
DOCKER_REGISTRY="ghcr.io"
IMAGE_TAG="latest"
ENVIRONMENT="production"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check if Docker Compose is installed
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    # Check if kubectl is installed (for Kubernetes deployment)
    if ! command -v kubectl &> /dev/null; then
        log_warn "kubectl is not installed. Kubernetes deployment will be skipped."
    fi
    
    log_info "Prerequisites check completed."
}

# Build Docker image
build_image() {
    log_info "Building Docker image..."
    
    # Build the image
    docker build -t ${DOCKER_REGISTRY}/${PROJECT_NAME}:${IMAGE_TAG} .
    
    # Tag for production
    docker tag ${DOCKER_REGISTRY}/${PROJECT_NAME}:${IMAGE_TAG} ${DOCKER_REGISTRY}/${PROJECT_NAME}:production
    
    log_info "Docker image built successfully."
}

# Push to registry
push_image() {
    log_info "Pushing image to registry..."
    
    # Login to registry (if needed)
    if [ -n "$DOCKER_USERNAME" ] && [ -n "$DOCKER_PASSWORD" ]; then
        echo "$DOCKER_PASSWORD" | docker login ${DOCKER_REGISTRY} -u "$DOCKER_USERNAME" --password-stdin
    fi
    
    # Push images
    docker push ${DOCKER_REGISTRY}/${PROJECT_NAME}:${IMAGE_TAG}
    docker push ${DOCKER_REGISTRY}/${PROJECT_NAME}:production
    
    log_info "Images pushed successfully."
}

# Deploy to Docker Compose
deploy_docker_compose() {
    log_info "Deploying with Docker Compose..."
    
    # Create production environment file
    if [ ! -f .env.production ]; then
        log_warn "Creating .env.production from template..."
        cp env.example .env.production
        log_warn "Please update .env.production with production values before continuing."
        read -p "Press Enter after updating .env.production..."
    fi
    
    # Stop existing services
    docker-compose -f docker-compose.yml --env-file .env.production down
    
    # Pull latest images
    docker-compose -f docker-compose.yml --env-file .env.production pull
    
    # Start services
    docker-compose -f docker-compose.yml --env-file .env.production up -d
    
    # Wait for services to be ready
    log_info "Waiting for services to be ready..."
    sleep 30
    
    # Check service health
    check_service_health
    
    log_info "Docker Compose deployment completed."
}

# Deploy to Kubernetes
deploy_kubernetes() {
    if ! command -v kubectl &> /dev/null; then
        log_warn "Skipping Kubernetes deployment (kubectl not available)."
        return
    fi
    
    log_info "Deploying to Kubernetes..."
    
    # Create namespace if it doesn't exist
    kubectl create namespace ${PROJECT_NAME} --dry-run=client -o yaml | kubectl apply -f -
    
    # Create secrets for sensitive data
    create_kubernetes_secrets
    
    # Apply Kubernetes manifests
    kubectl apply -f k8s/ -n ${PROJECT_NAME}
    
    # Wait for deployments to be ready
    log_info "Waiting for Kubernetes deployments to be ready..."
    kubectl wait --for=condition=available --timeout=300s deployment/netflix-analytics-api -n ${PROJECT_NAME}
    kubectl wait --for=condition=available --timeout=300s deployment/netflix-dashboard -n ${PROJECT_NAME}
    
    log_info "Kubernetes deployment completed."
}

# Create Kubernetes secrets
create_kubernetes_secrets() {
    log_info "Creating Kubernetes secrets..."
    
    # Create secret for Snowflake credentials
    if [ -n "$SNOWFLAKE_ACCOUNT" ] && [ -n "$SNOWFLAKE_USER" ] && [ -n "$SNOWFLAKE_PASSWORD" ]; then
        kubectl create secret generic snowflake-secret \
            --from-literal=account="$SNOWFLAKE_ACCOUNT" \
            --from-literal=user="$SNOWFLAKE_USER" \
            --from-literal=password="$SNOWFLAKE_PASSWORD" \
            -n ${PROJECT_NAME} \
            --dry-run=client -o yaml | kubectl apply -f -
    else
        log_warn "Snowflake credentials not provided. Skipping secret creation."
    fi
}

# Check service health
check_service_health() {
    log_info "Checking service health..."
    
    # Check API health
    if curl -f http://localhost:8000/health > /dev/null 2>&1; then
        log_info "API service is healthy."
    else
        log_error "API service health check failed."
        return 1
    fi
    
    # Check dashboard
    if curl -f http://localhost:8502 > /dev/null 2>&1; then
        log_info "Dashboard service is healthy."
    else
        log_error "Dashboard service health check failed."
        return 1
    fi
    
    # Check Kafka UI
    if curl -f http://localhost:8080 > /dev/null 2>&1; then
        log_info "Kafka UI is accessible."
    else
        log_warn "Kafka UI health check failed."
    fi
    
    log_info "All health checks completed."
}

# Run tests
run_tests() {
    log_info "Running tests..."
    
    # Run unit tests
    python -m pytest tests/ -v --cov=src --cov-report=html
    
    # Run integration tests
    python -m pytest tests/test_integration.py -v
    
    log_info "Tests completed."
}

# Backup existing deployment
backup_deployment() {
    log_info "Creating backup of existing deployment..."
    
    BACKUP_DIR="backups/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$BACKUP_DIR"
    
    # Backup environment files
    if [ -f .env.production ]; then
        cp .env.production "$BACKUP_DIR/"
    fi
    
    # Backup Docker Compose logs
    docker-compose logs > "$BACKUP_DIR/docker-compose-logs.txt" 2>/dev/null || true
    
    # Backup Kubernetes resources (if applicable)
    if command -v kubectl &> /dev/null; then
        kubectl get all -n ${PROJECT_NAME} -o yaml > "$BACKUP_DIR/k8s-resources.yaml" 2>/dev/null || true
    fi
    
    log_info "Backup created in $BACKUP_DIR"
}

# Rollback deployment
rollback_deployment() {
    log_info "Rolling back deployment..."
    
    # Find latest backup
    LATEST_BACKUP=$(ls -t backups/ | head -1)
    
    if [ -z "$LATEST_BACKUP" ]; then
        log_error "No backup found for rollback."
        return 1
    fi
    
    log_info "Rolling back to backup: $LATEST_BACKUP"
    
    # Restore environment file
    if [ -f "backups/$LATEST_BACKUP/.env.production" ]; then
        cp "backups/$LATEST_BACKUP/.env.production" .
    fi
    
    # Restart services
    docker-compose -f docker-compose.yml --env-file .env.production down
    docker-compose -f docker-compose.yml --env-file .env.production up -d
    
    log_info "Rollback completed."
}

# Main deployment function
main() {
    log_info "Starting production deployment for $PROJECT_NAME..."
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-tests)
                SKIP_TESTS=true
                shift
                ;;
            --skip-build)
                SKIP_BUILD=true
                shift
                ;;
            --skip-push)
                SKIP_PUSH=true
                shift
                ;;
            --kubernetes)
                DEPLOY_K8S=true
                shift
                ;;
            --rollback)
                rollback_deployment
                exit 0
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo "Options:"
                echo "  --skip-tests     Skip running tests"
                echo "  --skip-build     Skip building Docker image"
                echo "  --skip-push      Skip pushing to registry"
                echo "  --kubernetes     Deploy to Kubernetes"
                echo "  --rollback       Rollback to previous deployment"
                echo "  --help           Show this help message"
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                exit 1
                ;;
        esac
        shift
    done
    
    # Check prerequisites
    check_prerequisites
    
    # Run tests (unless skipped)
    if [ "$SKIP_TESTS" != "true" ]; then
        run_tests
    fi
    
    # Create backup
    backup_deployment
    
    # Build image (unless skipped)
    if [ "$SKIP_BUILD" != "true" ]; then
        build_image
    fi
    
    # Push image (unless skipped)
    if [ "$SKIP_PUSH" != "true" ]; then
        push_image
    fi
    
    # Deploy based on target
    if [ "$DEPLOY_K8S" = "true" ]; then
        deploy_kubernetes
    else
        deploy_docker_compose
    fi
    
    log_info "Production deployment completed successfully!"
    log_info "Services available at:"
    log_info "  - API: http://localhost:8000"
    log_info "  - Dashboard: http://localhost:8502"
    log_info "  - Kafka UI: http://localhost:8080"
}

# Run main function with all arguments
main "$@" 