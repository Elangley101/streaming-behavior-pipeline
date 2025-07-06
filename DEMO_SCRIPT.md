# 🎬 Netflix Behavioral Data Pipeline - Demo Script

## 🎯 **Demo Overview (10-15 minutes)**

### **What We're Showing:**
- Real-time streaming data pipeline
- Live analytics dashboard
- API integration capabilities
- Kafka monitoring and management

---

## 📋 **Pre-Demo Setup (5 minutes)**

### **1. Generate Initial Data**
```bash
# Generate 1000 events for rich demo data
curl -X POST http://YOUR_EC2_PUBLIC_IP:8000/generate-events \
  -H "Content-Type: application/json" \
  -d '{"num_events": 1000, "duration_minutes": 2}'
```

### **2. Start Continuous Streaming**
```bash
# Start real-time event generation
curl -X POST http://YOUR_EC2_PUBLIC_IP:8000/streaming/start
```

### **3. Open All Interfaces**
- **Dashboard**: http://YOUR_EC2_PUBLIC_IP:8502
- **Kafka UI**: http://YOUR_EC2_PUBLIC_IP:8080
- **API**: http://YOUR_EC2_PUBLIC_IP:8000

---

## 🎬 **Demo Flow**

### **Opening (1 minute)**
*"Welcome! Today I'm demonstrating a Netflix-style behavioral data pipeline that processes real-time user viewing data. This is a production-ready streaming analytics platform built with modern technologies."*

### **Part 1: System Architecture (2 minutes)**

**What to show**: System overview
**What to say**: 
*"Our platform consists of three main components:*
- *A real-time data streaming pipeline using Kafka*
- *An interactive analytics dashboard*
- *A RESTful API for integration*

*Let me show you how they work together."*

### **Part 2: Data Pipeline (3 minutes)**

**Navigate to**: Kafka UI (Port 8080)

**What to show**:
- Topics list (watch_events)
- Live message flow
- Consumer groups
- Real-time metrics

**What to say**:
*"Here's our data pipeline in action. We're using Apache Kafka to stream user viewing events in real-time. Each message represents a user watching a show - you can see the data flowing through the system right now."*

**Demo actions**:
- Point to message count increasing
- Show message details
- Highlight consumer lag
- Show topic configuration

### **Part 3: Analytics Dashboard (4 minutes)**

**Navigate to**: Dashboard (Port 8502)

**What to show**:
- Real-time metrics
- Interactive charts
- Filtering capabilities
- Data visualization

**What to say**:
*"This is our analytics dashboard. It's consuming the streaming data and providing real-time insights. Notice how the metrics update as new events come in."*

**Demo actions**:
- Show total sessions increasing
- Filter by different shows
- Show user engagement patterns
- Interact with charts
- Demonstrate date range filtering

### **Part 4: API Integration (2 minutes)**

**Navigate to**: API (Port 8000)

**What to show**:
- Health endpoint
- Analytics endpoint
- Event generation
- API documentation

**What to say**:
*"Our platform also provides a RESTful API for integration with other systems. This allows external applications to access our analytics data."*

**Demo actions**:
- Show `/health` response
- Show `/analytics` response
- Generate more events live
- Demonstrate API capabilities

### **Part 5: Live Data Generation (2 minutes)**

**What to show**: Real-time event generation
**What to say**: *"Let me generate some live events to show you how the system responds in real-time."*

**Demo actions**:
```bash
# Generate 100 more events
curl -X POST http://YOUR_EC2_PUBLIC_IP:8000/generate-events \
  -H "Content-Type: application/json" \
  -d '{"num_events": 100, "duration_minutes": 1}'
```

- Watch dashboard update
- Show Kafka messages flowing
- Demonstrate real-time processing

### **Closing (1 minute)**

**What to say**:
*"This demonstrates a complete streaming analytics platform that can handle real-time behavioral data at scale. The system is production-ready and can be extended for various use cases beyond video streaming."*

**Key highlights**:
- Real-time processing
- Scalable architecture
- Interactive analytics
- API integration
- Monitoring capabilities

---

## 🎯 **Demo Tips**

### **Before Demo:**
- Test all URLs work
- Generate sufficient sample data
- Have backup commands ready
- Prepare your EC2 public IP

### **During Demo:**
- Keep it conversational
- Show live updates
- Demonstrate interactivity
- Highlight real-time aspects
- Be prepared for questions

### **Common Questions:**
- **"How does it scale?"** - Kafka handles millions of messages per second
- **"What about data storage?"** - Data can be persisted to databases
- **"Can it handle real data?"** - Yes, just change the data source
- **"What technologies are used?"** - Kafka, FastAPI, Streamlit, Docker

---

## 🚀 **Advanced Demo Features**

### **Show Different Data Sources:**
- Switch between sample data and file-based data
- Demonstrate data source flexibility

### **Performance Metrics:**
- Show system health
- Demonstrate monitoring capabilities

### **Integration Examples:**
- Show how external systems could connect
- Demonstrate API usage patterns

---

## 📊 **Demo Checklist**

- [ ] Generate initial data (1000+ events)
- [ ] Start continuous streaming
- [ ] Test all three interfaces
- [ ] Prepare demo script
- [ ] Have backup commands ready
- [ ] Test API endpoints
- [ ] Prepare answers for common questions

---

## 🎬 **Success Metrics**

A successful demo should:
- Show real-time data flow
- Demonstrate interactive analytics
- Highlight system integration
- Show scalability potential
- Engage the audience with live updates

**Your demo is ready to impress!** 🚀 