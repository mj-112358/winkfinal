# 🚀 Wink AI Platform Deployment Guide

## Quick Start (5 minutes to production!)

### Option 1: Simple VPS Deployment ⚡ **RECOMMENDED**

**Requirements**: Ubuntu 20.04+ VPS with your domain pointing to it

```bash
# 1. On your server, run the deployment script
curl -fsSL https://raw.githubusercontent.com/mj-112358/winkfinal/main/deploy_simple.sh | sudo bash

# 2. Create your first store
sudo -u wink /opt/wink/backend/venv/bin/python -c "
import os
os.chdir('/opt/wink/backend')
os.environ['ALLOW_STORE_CREATION'] = 'true'
from src.database.database import get_database
from src.auth.auth_manager import get_auth_manager
db = get_database()
auth = get_auth_manager()
with db.get_session() as session:
    store, user = auth.create_store_and_owner(session, 'Your Store Name', 'admin@winkai.in', 'your_secure_password')
    print(f'Store ID: {store.id}')
    print(f'Admin Email: {user.email}')
"

# 3. Add OpenAI API Key
sudo nano /opt/wink/backend/.env
# Set OPENAI_API_KEY=your-actual-key

# 4. Restart services
sudo systemctl restart wink-api
```

**Result**: 
- ✅ Frontend: https://winkai.in
- ✅ API: https://api.winkai.in
- ✅ SSL certificates auto-configured
- ✅ Auto-start on boot

---

### Option 2: Manual Setup (Step by Step)

If you prefer manual control:

#### 1. **Server Setup**
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install dependencies
sudo apt install -y python3.11 python3.11-venv nodejs npm postgresql redis-server nginx certbot
```

#### 2. **Database Setup**
```bash
sudo -u postgres createdb wink_analytics
sudo -u postgres psql -c "CREATE USER wink_user WITH PASSWORD 'secure_password';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE wink_analytics TO wink_user;"
```

#### 3. **Clone and Setup Backend**
```bash
git clone https://github.com/mj-112358/winkfinal.git /opt/wink
cd /opt/wink/backend
python3.11 -m venv venv
./venv/bin/pip install -r requirements_new.txt

# Configure environment
cp .env.production .env
# Edit .env with your settings
```

#### 4. **Run Migrations**
```bash
cd /opt/wink/backend
./venv/bin/python -c "from src.database.migrations import run_migrations; run_migrations()"
```

#### 5. **Setup SystemD Service**
```bash
sudo tee /etc/systemd/system/wink-api.service > /dev/null <<EOF
[Unit]
Description=Wink AI Analytics API
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/wink/backend
Environment=PATH=/opt/wink/backend/venv/bin
ExecStart=/opt/wink/backend/venv/bin/python src/main.py
Restart=always

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable wink-api
sudo systemctl start wink-api
```

#### 6. **Setup Frontend**
```bash
cd /opt/wink/frontend
npm install
npm run build
```

#### 7. **Configure Nginx**
```bash
sudo tee /etc/nginx/sites-available/wink > /dev/null <<EOF
server {
    listen 80;
    server_name api.winkai.in;
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
    }
}

server {
    listen 80;
    server_name winkai.in www.winkai.in;
    root /opt/wink/frontend/dist;
    index index.html;
    location / {
        try_files \$uri \$uri/ /index.html;
    }
}
EOF

sudo ln -s /etc/nginx/sites-available/wink /etc/nginx/sites-enabled/
sudo systemctl restart nginx
```

#### 8. **Setup SSL**
```bash
sudo certbot --nginx -d winkai.in -d www.winkai.in -d api.winkai.in
```

---

### Option 3: Docker Deployment

```bash
# Clone repository
git clone https://github.com/mj-112358/winkfinal.git
cd winkfinal

# Set environment variables
export JWT_SECRET_KEY=$(openssl rand -base64 32)
export OPENAI_API_KEY=your-openai-key

# Deploy with Docker
docker-compose -f docker-compose.prod.yml up -d

# Create first store
docker exec -it winkfinal_wink-api_1 python -c "
import os
os.environ['ALLOW_STORE_CREATION'] = 'true'
from src.database.database import get_database
from src.auth.auth_manager import get_auth_manager
db = get_database()
auth = get_auth_manager()
with db.get_session() as session:
    store, user = auth.create_store_and_owner(session, 'Your Store', 'admin@winkai.in', 'password')
    print(f'Store: {store.id}')
"
```

---

## 🔧 Environment Configuration

Create `.env` file in backend directory:

```bash
# Database
DATABASE_URL=postgresql://wink_user:password@localhost:5432/wink_analytics
ENABLE_RLS=true

# Authentication (CRITICAL - Change these!)
JWT_SECRET_KEY=your-super-secure-32-char-secret-here
OPENAI_API_KEY=sk-your-openai-api-key

# Redis
REDIS_URL=redis://localhost:6379

# Security
ALLOWED_ORIGINS=https://winkai.in,https://api.winkai.in
ALLOW_STORE_CREATION=false

# Camera Processing
YOLO_MODEL_PATH=models/yolov8n.pt
ENABLE_REID=true
```

---

## 🎯 Post-Deployment Checklist

### ✅ **Verify Deployment**
```bash
# Check API health
curl https://api.winkai.in/health

# Check frontend
curl https://winkai.in

# Check services
sudo systemctl status wink-api
sudo systemctl status nginx
sudo systemctl status postgresql
sudo systemctl status redis-server
```

### ✅ **Create Your First Store**
1. **Temporarily enable store creation**:
   ```bash
   sudo nano /opt/wink/backend/.env
   # Set ALLOW_STORE_CREATION=true
   sudo systemctl restart wink-api
   ```

2. **Create store via API**:
   ```bash
   curl -X POST https://api.winkai.in/api/auth/create-store \
     -H "Content-Type: application/json" \
     -d '{
       "store_name": "Your Store Name",
       "owner_email": "admin@winkai.in",
       "owner_password": "your_secure_password"
     }'
   ```

3. **Disable store creation**:
   ```bash
   sudo nano /opt/wink/backend/.env
   # Set ALLOW_STORE_CREATION=false
   sudo systemctl restart wink-api
   ```

### ✅ **Test Login**
1. Visit https://winkai.in
2. Login with your owner credentials
3. Add a test camera (use any RTSP URL or webcam URL)
4. Upload a screenshot and create zones
5. Generate insights

---

## 🚨 **Security Hardening**

```bash
# Firewall
sudo ufw allow 22/tcp    # SSH
sudo ufw allow 80/tcp    # HTTP
sudo ufw allow 443/tcp   # HTTPS
sudo ufw enable

# Secure PostgreSQL
sudo nano /etc/postgresql/*/main/pg_hba.conf
# Ensure only local connections allowed

# Secure Redis
sudo nano /etc/redis/redis.conf
# Set bind 127.0.0.1
# Set requirepass your-redis-password
```

---

## 📊 **Monitoring & Maintenance**

### **Service Management**
```bash
# Check status
sudo systemctl status wink-api

# View logs
sudo journalctl -u wink-api -f

# Restart service
sudo systemctl restart wink-api
```

### **Database Maintenance**
```bash
# Backup
pg_dump wink_analytics > backup_$(date +%Y%m%d).sql

# Monitor connections
sudo -u postgres psql -c "SELECT * FROM pg_stat_activity;"
```

### **SSL Renewal**
```bash
# Test renewal
sudo certbot renew --dry-run

# Auto-renewal is configured via cron
```

---

## 🎉 **You're Live!**

After successful deployment:

- **Frontend**: https://winkai.in (React app with authentication)
- **API**: https://api.winkai.in (FastAPI backend)
- **Admin**: Use your store owner credentials to login
- **Features**: 
  - ✅ Multi-tenant data isolation
  - ✅ RTSP camera auto-processing
  - ✅ Real-time person detection
  - ✅ Zone analytics
  - ✅ AI-powered insights
  - ✅ Secure authentication

## 🆘 **Troubleshooting**

### API won't start
```bash
sudo journalctl -u wink-api --no-pager -l
# Check database connection and environment variables
```

### Database connection failed
```bash
sudo systemctl status postgresql
sudo -u postgres psql -l  # List databases
```

### SSL issues
```bash
sudo certbot certificates  # Check certificate status
sudo nginx -t  # Test nginx config
```

### Frontend not loading
```bash
sudo nginx -t
ls -la /opt/wink/frontend/dist  # Check build files exist
```

---

**Need help?** Check the logs:
- API: `sudo journalctl -u wink-api -f`
- Nginx: `sudo tail -f /var/log/nginx/error.log`
- Database: `sudo journalctl -u postgresql -f`