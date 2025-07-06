# EC2 Setup Commands - Netflix Behavioral Data Pipeline

## 🚀 **Complete EC2 Setup Guide**

### **Step 1: Update System**
```bash
sudo yum update -y
```

### **Step 2: Install Docker**
```bash
# Install Docker
sudo yum install -y docker

# Start Docker service
sudo systemctl start docker
sudo systemctl enable docker

# Add user to docker group
sudo usermod -a -G docker ec2-user

# Log out and back in, or run:
newgrp docker

# Verify Docker installation
docker --version
```

### **Step 3: Install Docker Compose**
```bash
# Download Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

# Make it executable
sudo chmod +x /usr/local/bin/docker-compose

# Verify installation
docker-compose --version
```

### **Step 4: Clone Your Repository**
```bash
# Clone your repository
git clone https://github.com/yourusername/netflix-behavior-pipeline.git
cd netflix-behavior-pipeline

# Verify files are present
ls -la
```

### **Step 5: Set Up Environment**
```bash
# Copy environment file
cp env.example .env

# Edit environment variables (optional)
nano .env

# Or view the file
cat .env
```

### **Step 6: Start the Application**
```bash
# Build and start all services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### **Step 7: Verify Services**
```bash
# Check if services are running
docker ps

# Test API health endpoint
curl http://localhost:8000/health

# Check dashboard
curl http://localhost:8502
```

## 🔧 **Troubleshooting Commands**

### **If Services Don't Start:**
```bash
# Check Docker status
sudo systemctl status docker

# Restart Docker if needed
sudo systemctl restart docker

# Check available memory
free -h

# Check disk space
df -h

# View detailed logs
docker-compose logs [service-name]
```

### **If Port Issues:**
```bash
# Check what's using ports
sudo netstat -tulpn | grep :8000
sudo netstat -tulpn | grep :8502
sudo netstat -tulpn | grep :8080

# Kill processes if needed
sudo kill -9 [PID]
```

### **If Permission Issues:**
```bash
# Fix Docker permissions
sudo chmod 666 /var/run/docker.sock

# Re-add user to docker group
sudo usermod -a -G docker ec2-user
newgrp docker
```

## 🌐 **Access Your Application**

### **Dashboard:**
```
http://your-ec2-public-ip:8502
```

### **API Documentation:**
```
http://your-ec2-public-ip:8000/docs
```

### **Kafka UI:**
```
http://your-ec2-public-ip:8080
```

### **Health Check:**
```
http://your-ec2-public-ip:8000/health
```

## 📊 **Monitoring Commands**

### **Check System Resources:**
```bash
# CPU and memory usage
htop

# Disk usage
df -h

# Network connections
netstat -tulpn
```

### **Check Application Status:**
```bash
# All containers
docker-compose ps

# Container logs
docker-compose logs -f [service-name]

# Resource usage
docker stats
```

### **Generate Test Data:**
```bash
# Generate sample events
curl -X POST "http://localhost:8000/generate-events?num_events=100&duration_minutes=5"
```

## 🔒 **Security Group Configuration**

Make sure your EC2 security group allows these ports:
- **22**: SSH
- **80**: HTTP
- **443**: HTTPS
- **8000**: FastAPI
- **8502**: Streamlit Dashboard
- **8080**: Kafka UI

## 💰 **Cost Optimization**

### **Stop Instance When Not Using:**
```bash
# From AWS Console or CLI
aws ec2 stop-instances --instance-ids i-1234567890abcdef0
```

### **Monitor Costs:**
- Set up AWS billing alerts
- Use AWS Cost Explorer
- Consider using t3.micro for demo (free tier)

## 🎯 **Next Steps After Deployment**

1. **Test all endpoints** and verify functionality
2. **Generate sample data** to populate the dashboard
3. **Record a demo video** showing the live application
4. **Update your LinkedIn post** with the live URL
5. **Share with your network** and potential employers 