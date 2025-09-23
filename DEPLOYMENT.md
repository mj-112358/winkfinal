# WINK Retail Analytics Platform - Production Deployment Guide

## Overview

This guide covers the complete production deployment of the WINK Retail Analytics Platform on your **winkai.in** domain. The platform provides comprehensive retail analytics including footfall tracking, zone-based analytics, queue management, and AI-powered insights.

## Architecture

### Core Components
- **Backend API**: FastAPI server with person detection and analytics
- **Redis**: Real-time tracking and queue management
- **PostgreSQL**: Production database (optional upgrade from SQLite)
- **Frontend**: React-based dashboard interface
- **Camera Processors**: Scalable RTSP stream processing workers
- **Monitoring Stack**: Prometheus, Grafana, and Loki for observability

### Domain Configuration
- **Main Site**: `winkai.in` (Frontend)
- **API**: `api.winkai.in` (Backend)
- **Dashboard**: `dashboard.winkai.in` (Grafana)
- **Metrics**: `metrics.winkai.in` (Prometheus)

## Prerequisites

### Server Requirements
- **Minimum**: 8 CPU cores, 16GB RAM, 200GB SSD
- **Recommended**: 16 CPU cores, 32GB RAM, 500GB SSD
- **Operating System**: Ubuntu 20.04 LTS or CentOS 8+
- **GPU Support** (Optional): NVIDIA GPU with CUDA for faster detection

### Software Dependencies
- Docker Engine 20.10+
- Docker Compose 2.0+
- Domain name configured (winkai.in)
- SSL certificates (auto-generated via Let's Encrypt)

### External Services
1. **OpenAI API Key** (for AI insights)
2. **RTSP Camera Streams** (your store cameras)
3. **Email for SSL certificates** (admin@winkai.in)

## Installation Steps

### 1. Server Preparation

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Reboot to apply changes
sudo reboot
```

### 2. Deploy the Application

```bash
# Clone the repository
git clone <your-repo-url> wink-platform
cd wink-platform

# Set up environment variables
cp .env.example .env
nano .env  # Configure your settings (see Environment Configuration below)

# Create required directories
mkdir -p nginx/certs nginx/vhost.d nginx/html
mkdir -p monitoring backend/assets backend/models
mkdir -p database

# Set permissions
sudo chown -R $USER:$USER nginx/ monitoring/ backend/ database/

# Start the platform
docker-compose up -d

# Check status
docker-compose ps
```

### 3. Environment Configuration

Create a `.env` file in the root directory:

```bash
# Store Configuration
STORE_ID=your_store_id
STORE_NAME="Your Store Name"

# Database
POSTGRES_PASSWORD=your_secure_database_password

# OpenAI API for insights
OPENAI_API_KEY=sk-your-openai-api-key

# Hardware Configuration
MODEL_DEVICE=cuda  # or 'cpu' if no GPU
FRAME_RATE=12      # Frames per second for processing
DETECTION_INTERVAL=3  # Process every Nth frame for performance

# Monitoring
GRAFANA_PASSWORD=your_grafana_admin_password

# SSL/Domain
LETSENCRYPT_EMAIL=admin@winkai.in
```

### 4. DNS Configuration

Configure your DNS records to point to your server:

```
Type    Name                    Value
A       winkai.in              YOUR_SERVER_IP
A       www.winkai.in          YOUR_SERVER_IP
A       api.winkai.in          YOUR_SERVER_IP
A       dashboard.winkai.in    YOUR_SERVER_IP
A       metrics.winkai.in      YOUR_SERVER_IP
```

## Camera Setup and Zone Configuration

### 1. Adding RTSP Cameras

Use the API to add your cameras:

```bash
curl -X POST "https://api.winkai.in/api/cameras" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Entrance Camera",
    "rtsp_url": "rtsp://username:password@camera-ip:554/stream",
    "enabled": true
  }'
```

### 2. Zone Setup Process

**This is exactly how you'll set up zones as requested:**

1. **Take a screenshot** of your camera feed
2. **Upload the screenshot** via API:

```bash
curl -X POST "https://api.winkai.in/api/zones/screenshot" \
  -F "camera_id=1" \
  -F "file=@your_screenshot.png" \
  -F "img_width=1920" \
  -F "img_height=1080"
```

3. **Get polygon coordinates** from the screenshot (using your preferred method)
4. **Create zones** with those coordinates:

```bash
curl -X POST "https://api.winkai.in/api/zones" \
  -F "camera_id=1" \
  -F "name=Entrance Zone" \
  -F "ztype=entry" \
  -F "polygon_json=[[100,100],[400,100],[400,300],[100,300]]"
```

### Zone Types Available
- **entry**: Store entrance/exit tracking
- **queue**: Queue time monitoring
- **shelf**: Product interaction tracking
- **checkout**: Checkout area monitoring
- **general**: General footfall areas

### 3. Polygon Coordinate Mapping

The system automatically scales polygon coordinates from your screenshot dimensions to the actual video feed dimensions. This ensures accurate zone detection regardless of resolution differences.

## Detection Pipeline Explanation

### How Detection Works

1. **Person Detection**: YOLO model detects persons in video frames
2. **Unique ID Assignment**: Enhanced centroid tracker assigns persistent IDs to people based on:
   - Position proximity
   - Velocity prediction
   - Size consistency
3. **Zone Classification**: Each person's centroid is checked against all defined polygons
4. **Event Generation**: Events are triggered when people:
   - Enter zones (first detection in zone)
   - Exit zones (leaving zone boundary)
   - Stay in zones (continuous presence)
5. **Metrics Calculation**:
   - **Footfall**: Counted once per person per hour when entering "entry" zones
   - **Interactions**: Tracked when people enter "shelf" zones
   - **Queue Time**: Measured from entry to exit of "queue" zones
   - **Dwell Time**: Total time spent in any zone per person

### Person Tracking Details

- **Unique IDs**: Each person gets a unique ID that persists while they're in view
- **Cross-Camera Tracking**: Not implemented (each camera tracks independently)
- **ID Persistence**: IDs are maintained for 10 seconds after person leaves frame
- **Velocity Prediction**: Uses movement patterns to maintain tracking during brief occlusions

### Analytics Generation

- **Hourly Metrics**: Aggregated every hour automatically
- **Daily Metrics**: Calculated from hourly data with trend analysis
- **Spike Detection**: Statistical analysis identifies unusual patterns
- **AI Insights**: OpenAI analyzes patterns and provides recommendations

## Monitoring and Alerting

### Access Monitoring Dashboards

- **Grafana**: https://dashboard.winkai.in (admin/your_grafana_password)
- **Prometheus**: https://metrics.winkai.in

### Key Metrics to Monitor

1. **System Health**:
   - API response times
   - Database connections
   - Redis memory usage
   - Camera processor CPU/memory

2. **Business Metrics**:
   - Real-time people count
   - Hourly footfall trends
   - Queue wait times
   - Zone utilization rates

3. **Alerts**:
   - Camera disconnections
   - Spike detections
   - System resource alerts
   - Processing delays

### Setting Up Alerts

Configure alerts in Grafana:
1. Go to https://dashboard.winkai.in
2. Navigate to Alerting → Alert Rules
3. Create rules for critical metrics
4. Set up notification channels (email, Slack, etc.)

## API Usage Examples

### Get Real-time Metrics
```bash
curl "https://api.winkai.in/api/analytics/realtime"
```

### Get Comprehensive Analytics
```bash
curl -X POST "https://api.winkai.in/api/analytics/comprehensive" \
  -H "Content-Type: application/json" \
  -d '{"days": 30, "include_zones": true}'
```

### Spike Detection
```bash
curl "https://api.winkai.in/api/analytics/spikes?date=2024-01-15"
```

### Create Promotion Event
```bash
curl -X POST "https://api.winkai.in/api/events" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Weekend Sale",
    "event_type": "promotion",
    "start_date": "2024-01-15",
    "end_date": "2024-01-17",
    "description": "Special weekend promotion"
  }'
```

### Analyze Event Impact
```bash
curl -X POST "https://api.winkai.in/api/events/1/analyze"
```

## Performance Optimization

### Hardware Optimization

1. **GPU Acceleration** (Recommended):
   ```bash
   # Install NVIDIA Docker runtime
   distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
   curl -s -L https://nvidia.github.io/nvidia-docker/gpgkey | sudo apt-key add -
   curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.list | sudo tee /etc/apt/sources.list.d/nvidia-docker.list
   
   sudo apt-get update && sudo apt-get install -y nvidia-docker2
   sudo systemctl restart docker
   
   # Set MODEL_DEVICE=cuda in .env
   ```

2. **Scale Camera Processors**:
   ```bash
   # Scale to 4 processors
   docker-compose up -d --scale camera-processor=4
   ```

### Performance Tuning

1. **Detection Interval**: Increase `DETECTION_INTERVAL` to process fewer frames
2. **Frame Rate**: Adjust `FRAME_RATE` based on your needs vs. hardware capacity
3. **Database**: Switch to PostgreSQL for better performance (configured in docker-compose.yml)

## Backup and Recovery

### Database Backup
```bash
# SQLite backup
docker exec wink-backend cp /app/wink_store.db /app/assets/backup_$(date +%Y%m%d).db

# PostgreSQL backup
docker exec wink-postgres pg_dump -U wink_user wink_analytics > backup_$(date +%Y%m%d).sql
```

### Configuration Backup
```bash
# Backup all configurations
tar -czf wink_backup_$(date +%Y%m%d).tar.gz \
  .env docker-compose.yml nginx/ monitoring/ backend/assets/
```

## Security Considerations

### Network Security
- All services run on internal Docker networks
- Only necessary ports exposed through nginx-proxy
- SSL certificates automatically managed

### Data Security
- Database passwords stored in environment variables
- API keys encrypted in environment files
- Regular security updates via Docker image updates

### Access Control
- Grafana authentication required
- API access can be restricted by IP if needed
- Camera streams processed locally (no external transmission)

## Troubleshooting

### Common Issues

1. **Camera Connection Failed**:
   ```bash
   # Check camera processor logs
   docker logs wink-camera-processor-1
   
   # Test RTSP connection
   ffmpeg -i "rtsp://your-camera-url" -t 10 -f null -
   ```

2. **High CPU Usage**:
   - Increase `DETECTION_INTERVAL`
   - Reduce `FRAME_RATE`
   - Enable GPU acceleration

3. **Database Errors**:
   ```bash
   # Check database status
   docker exec wink-postgres pg_isready -U wink_user
   
   # View logs
   docker logs wink-postgres
   ```

4. **SSL Certificate Issues**:
   ```bash
   # Check acme-companion logs
   docker logs nginx-proxy-acme
   
   # Manually renew certificates
   docker exec nginx-proxy-acme /app/force_renew
   ```

### Log Monitoring
```bash
# View all logs
docker-compose logs -f

# Specific service logs
docker logs wink-backend -f
docker logs wink-redis -f
docker logs nginx-proxy -f
```

## Scaling for Multiple Stores

The platform supports multiple stores by setting different `STORE_ID` values:

```bash
# Store 1
STORE_ID=store_mumbai STORE_NAME="Mumbai Store" docker-compose up -d

# Store 2 (different directory)
STORE_ID=store_delhi STORE_NAME="Delhi Store" docker-compose up -d
```

Each store maintains separate data and analytics while sharing the same infrastructure.

## Support and Maintenance

### Regular Maintenance Tasks

1. **Weekly**:
   - Monitor disk usage
   - Check system health in Grafana
   - Review error logs

2. **Monthly**:
   - Update Docker images
   - Backup database
   - Review analytics performance

3. **Quarterly**:
   - Security updates
   - Performance optimization review
   - Capacity planning

### Getting Help

1. Check logs for specific error messages
2. Review monitoring dashboards for system health
3. Verify camera connections and zone configurations
4. Test API endpoints for functionality

## External Dependencies and Services Required

### Required External Services:
1. **OpenAI API** ($20-100/month depending on usage)
2. **Domain Registration** (winkai.in - already purchased)
3. **Server Hosting** (AWS/GCP/Azure - $100-500/month)

### Hardware Dependencies:
1. **RTSP-compatible IP cameras**
2. **Stable network connection** for camera streams
3. **Sufficient bandwidth** for video processing

### Optional Services:
1. **Email SMTP** for alert notifications
2. **Slack/Discord** webhooks for alerts
3. **External backup storage** (AWS S3, Google Drive)

This completes your production-ready WINK Retail Analytics Platform deployment. The system will automatically handle person detection, tracking, zone-based analytics, and provide comprehensive insights about your store's performance.