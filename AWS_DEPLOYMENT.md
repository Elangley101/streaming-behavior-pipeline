# AWS Deployment Guide

## 🚀 Quick AWS Deployment (30 minutes)

### **Step 1: Create AWS Account**
1. Go to [aws.amazon.com](https://aws.amazon.com)
2. Click "Create an AWS Account"
3. Follow the signup process
4. **Free tier available** for 12 months

### **Step 2: Launch EC2 Instance**

#### **Instance Configuration:**
- **Instance Type**: t3.medium (2 vCPU, 4 GB RAM)
- **AMI**: Amazon Linux 2023
- **Storage**: 20 GB gp3
- **Security Group**: Allow ports 22, 80, 443, 8000, 8502, 8080

#### **Security Group Rules:**
```
Type        Protocol    Port Range    Source
SSH         TCP         22            Your IP
HTTP        TCP         80            Anywhere
HTTPS       TCP         443           Anywhere
Custom      TCP         8000          Anywhere (FastAPI)
Custom      TCP         8502          Anywhere (Dashboard)
Custom      TCP         8080          Anywhere (Kafka UI)
```

### **Step 3: Connect to EC2**

#### **Using SSH (Windows):**
```bash
# Download your .pem key file
# Open PowerShell and navigate to key location
ssh -i "your-key.pem" ec2-user@your-ec2-public-ip
```

#### **Using AWS Systems Manager (Recommended):**
1. Go to EC2 Console
2. Select your instance
3. Click "Connect" → "Session Manager"
4. Click "Connect"

### **Step 4: Install Docker on EC2**

```bash
# Update system
sudo yum update -y

# Install Docker
sudo yum install -y docker

# Start Docker service
sudo systemctl start docker
sudo systemctl enable docker

# Add user to docker group
sudo usermod -a -G docker ec2-user

# Log out and back in, or run:
newgrp docker

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

### **Step 5: Deploy Your Application**

```bash
# Clone your repository
git clone https://github.com/yourusername/netflix-behavior-pipeline.git
cd netflix-behavior-pipeline

# Create environment file
cp env.example .env

# Edit environment variables (optional)
nano .env

# Start the application
docker-compose up -d

# Check status
docker-compose ps
```

### **Step 6: Access Your Application**

#### **Dashboard:**
```
http://your-ec2-public-ip:8502
```

#### **API Documentation:**
```
http://your-ec2-public-ip:8000/docs
```

#### **Kafka UI:**
```
http://your-ec2-public-ip:8080
```

### **Step 7: Set Up Domain (Optional but Impressive)**

#### **Using Route 53:**
1. Register a domain (e.g., `yourname-data-pipeline.com`)
2. Create hosted zone
3. Add A record pointing to your EC2 IP

#### **Using Cloudflare (Free):**
1. Sign up at [cloudflare.com](https://cloudflare.com)
2. Add your domain
3. Point nameservers to Cloudflare
4. Add A record for your EC2 IP

### **Step 8: SSL Certificate (Optional)**

#### **Using Let's Encrypt:**
```bash
# Install certbot
sudo yum install -y certbot

# Get certificate
sudo certbot certonly --standalone -d yourdomain.com

# Configure nginx or load balancer for SSL
```

## 🔧 **Troubleshooting**

### **Common Issues:**

#### **Port Not Accessible:**
- Check security group rules
- Verify firewall settings
- Ensure application is running

#### **Docker Permission Issues:**
```bash
sudo chmod 666 /var/run/docker.sock
```

#### **Memory Issues:**
- Increase swap space
- Use larger instance type
- Optimize Docker memory limits

### **Monitoring Commands:**

```bash
# Check application status
docker-compose ps

# View logs
docker-compose logs -f

# Check system resources
htop
df -h
free -h

# Monitor network
netstat -tulpn
```

## 💰 **Cost Optimization**

### **Free Tier (12 months):**
- **EC2**: 750 hours/month of t3.micro
- **Data Transfer**: 15 GB/month
- **Total Cost**: ~$0/month

### **After Free Tier:**
- **t3.medium**: ~$30/month
- **Data Transfer**: ~$5-10/month
- **Total**: ~$35-40/month

### **Cost Reduction Tips:**
- Use t3.micro for demo (slower but free)
- Stop instance when not using
- Use spot instances for testing
- Set up billing alerts

## 🎯 **Production Considerations**

### **For Real Production:**
- **Load Balancer**: Application Load Balancer
- **Auto Scaling**: Scale based on demand
- **RDS**: Managed database
- **S3**: Data lake storage
- **CloudWatch**: Monitoring and alerting
- **VPC**: Network isolation

### **Security Best Practices:**
- Use IAM roles instead of access keys
- Enable VPC flow logs
- Regular security updates
- Encrypt data at rest and in transit
- Use secrets management

## 📊 **Performance Optimization**

### **EC2 Optimization:**
- Use gp3 volumes for better performance
- Enable enhanced networking
- Use placement groups for low latency
- Optimize instance type for workload

### **Docker Optimization:**
- Use multi-stage builds
- Optimize image layers
- Use .dockerignore
- Implement health checks

## 🚀 **Next Steps After Deployment**

1. **Test all endpoints** and verify functionality
2. **Create demo video** showing the live system
3. **Update README** with live demo URLs
4. **Share on LinkedIn** and data engineering communities
5. **Start applying** to remote Data Engineer roles

---

**Your live demo will be much more impressive than just code!** 🎉 