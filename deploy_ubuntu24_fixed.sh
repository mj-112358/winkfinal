#!/bin/bash
# Ubuntu 24.04 VPS deployment script for Wink AI Platform - FIXED VERSION
set -e

echo "🚀 Starting Wink AI Platform deployment for Ubuntu 24.04..."

# Update system
sudo apt update && sudo apt upgrade -y

# Remove conflicting Node.js packages
sudo apt remove -y nodejs npm || true
sudo apt autoremove -y

# Install Node.js 18 from NodeSource
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt install -y nodejs

# Install other required packages
sudo apt install -y python3 python3-venv python3-pip python3-dev build-essential \
    postgresql postgresql-contrib redis-server nginx certbot \
    python3-certbot-nginx git curl wget software-properties-common \
    libpq-dev pkg-config

# Create application user
sudo useradd -m -s /bin/bash wink || true
sudo usermod -a -G sudo wink

# Create application directory
sudo mkdir -p /opt/wink
sudo chown wink:wink /opt/wink

# Clone repository
cd /opt/wink
sudo -u wink git clone https://github.com/mj-112358/winkfinal.git .

# Setup PostgreSQL
sudo systemctl start postgresql
sudo systemctl enable postgresql
sudo -u postgres createdb wink_analytics || true
sudo -u postgres psql -c "CREATE USER wink_user WITH PASSWORD 'secure_wink_password_2025';" || true
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE wink_analytics TO wink_user;" || true

# Setup Python environment
cd /opt/wink/backend
sudo -u wink python3 -m venv venv
sudo -u wink ./venv/bin/pip install --upgrade pip

# Install Python packages
echo "Installing Python packages..."
sudo -u wink ./venv/bin/pip install fastapi==0.104.1
sudo -u wink ./venv/bin/pip install uvicorn[standard]==0.24.0
sudo -u wink ./venv/bin/pip install sqlalchemy==2.0.23
sudo -u wink ./venv/bin/pip install psycopg2-binary==2.9.9
sudo -u wink ./venv/bin/pip install redis==5.0.1
sudo -u wink ./venv/bin/pip install python-multipart==0.0.6
sudo -u wink ./venv/bin/pip install python-jose[cryptography]==3.3.0
sudo -u wink ./venv/bin/pip install passlib[bcrypt]==1.7.4
sudo -u wink ./venv/bin/pip install opencv-python-headless==4.8.1.78
sudo -u wink ./venv/bin/pip install ultralytics==8.0.220
sudo -u wink ./venv/bin/pip install pillow==10.1.0
sudo -u wink ./venv/bin/pip install numpy==1.24.4
sudo -u wink ./venv/bin/pip install python-dotenv==1.0.0
sudo -u wink ./venv/bin/pip install pydantic==2.5.0
sudo -u wink ./venv/bin/pip install openai==1.3.7

# Setup environment
sudo -u wink cp .env.production .env

# Configure environment with proper escaping
sudo -u wink bash -c 'cat > .env << EOF
# Database
DATABASE_URL=postgresql://wink_user:secure_wink_password_2025@localhost:5432/wink_analytics
ENABLE_RLS=true

# Authentication
JWT_SECRET_KEY='$(openssl rand -base64 32)'
OPENAI_API_KEY=sk-placeholder

# Redis
REDIS_URL=redis://localhost:6379

# Security
ALLOWED_ORIGINS=https://api.winkai.in,https://winkai.in
ALLOW_STORE_CREATION=false

# Camera Processing
YOLO_MODEL_PATH=models/yolov8n.pt
ENABLE_REID=true
EOF'

# Run migrations
cd /opt/wink/backend
echo "Running database migrations..."
sudo -u wink ./venv/bin/python -c "from src.database.migrations import run_migrations; run_migrations()"

# Setup systemd service for backend
sudo tee /etc/systemd/system/wink-api.service > /dev/null <<EOF
[Unit]
Description=Wink AI Analytics API
After=network.target postgresql.service redis.service

[Service]
Type=simple
User=wink
WorkingDirectory=/opt/wink/backend
Environment=PATH=/opt/wink/backend/venv/bin
ExecStart=/opt/wink/backend/venv/bin/python src/main.py
Restart=always
RestartSec=3
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Enable and start services
sudo systemctl daemon-reload
sudo systemctl enable wink-api
sudo systemctl enable postgresql redis-server

# Setup frontend
cd /opt/wink/frontend
echo "Installing frontend dependencies..."
sudo -u wink npm install
echo "Building frontend..."
sudo -u wink npm run build

# Setup Nginx
sudo tee /etc/nginx/sites-available/wink > /dev/null <<'EOF'
# API Backend
server {
    listen 80;
    server_name api.winkai.in;
    
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_connect_timeout 300s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;
    }
}

# Frontend
server {
    listen 80;
    server_name winkai.in www.winkai.in;
    root /opt/wink/frontend/dist;
    index index.html;
    
    location / {
        try_files $uri $uri/ /index.html;
    }
    
    location /assets {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}
EOF

# Enable site
sudo ln -sf /etc/nginx/sites-available/wink /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl enable nginx
sudo systemctl restart nginx

# Start the API service
sudo systemctl start wink-api

# Setup firewall
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw --force enable

echo "✅ Deployment complete!"
echo ""
echo "🌐 Your Wink AI Platform is now available at:"
echo "   Frontend: http://winkai.in"
echo "   API: http://api.winkai.in"
echo "   Health: http://api.winkai.in/health"
echo ""
echo "📋 Next steps:"
echo "1. Point your domain to this server's IP (159.65.153.124)"
echo "2. Run SSL setup: sudo certbot --nginx -d winkai.in -d www.winkai.in -d api.winkai.in --email admin@winkai.in --agree-tos --non-interactive"
echo "3. Create your first store:"
echo '   sudo -u wink /opt/wink/backend/venv/bin/python -c "'
echo '   import os'
echo '   os.chdir("/opt/wink/backend")'
echo '   os.environ["ALLOW_STORE_CREATION"] = "true"'
echo '   from src.database.database import get_database'
echo '   from src.auth.auth_manager import get_auth_manager'
echo '   db = get_database()'
echo '   auth = get_auth_manager()'
echo '   with db.get_session() as session:'
echo '       store, user = auth.create_store_and_owner(session, "Your Store Name", "admin@winkai.in", "your_secure_password")'
echo '       print(f"Store ID: {store.id}")'
echo '       print(f"Admin Email: {user.email}")'
echo '   "'
echo "4. Add your OpenAI API key to /opt/wink/backend/.env"
echo "5. Restart the API: sudo systemctl restart wink-api"
echo ""
echo "🔧 Service commands:"
echo "   Status: sudo systemctl status wink-api"
echo "   Logs: sudo journalctl -u wink-api -f"
echo "   Restart: sudo systemctl restart wink-api"