#!/bin/bash
# Simple VPS deployment script for Wink AI Platform
# Run this on your Ubuntu 20.04+ server

set -e

echo "🚀 Starting Wink AI Platform deployment..."

# Update system
sudo apt update && sudo apt upgrade -y

# Install required packages
sudo apt install -y python3.11 python3.11-venv python3-pip nodejs npm postgresql postgresql-contrib redis-server nginx certbot python3-certbot-nginx git

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
sudo -u postgres createdb wink_analytics || true
sudo -u postgres psql -c "CREATE USER wink_user WITH PASSWORD 'secure_wink_password_2025';" || true
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE wink_analytics TO wink_user;" || true

# Setup Python environment
cd /opt/wink/backend
sudo -u wink python3.11 -m venv venv
sudo -u wink ./venv/bin/pip install -r requirements_new.txt

# Setup environment
sudo -u wink cp .env.production .env
sudo -u wink sed -i "s|postgresql://user:pass@host:port/db|postgresql://wink_user:secure_wink_password_2025@localhost:5432/wink_analytics|g" .env
sudo -u wink sed -i "s|your-super-secure-jwt-secret-key-here-change-this-in-production|$(openssl rand -base64 32)|g" .env
sudo -u wink sed -i "s|your-openai-api-key-here|${OPENAI_API_KEY:-sk-placeholder}|g" .env
sudo -u wink sed -i "s|https://yourdomain.com|https://api.winkai.in,https://winkai.in|g" .env

# Run migrations
cd /opt/wink/backend
sudo -u wink ./venv/bin/python -c "from src.database.migrations import run_migrations; run_migrations()"

# Create first store (you'll need to run this manually with your details)
echo "📝 After deployment, create your first store with:"
echo "sudo -u wink /opt/wink/backend/venv/bin/python -c \"
from src.database.database import get_database
from src.auth.auth_manager import get_auth_manager
import os
os.chdir('/opt/wink/backend')
os.environ['ALLOW_STORE_CREATION'] = 'true'
db = get_database()
auth = get_auth_manager()
with db.get_session() as session:
    store, user = auth.create_store_and_owner(session, 'Your Store Name', 'admin@yourdomain.com', 'your_secure_password')
    print(f'Store ID: {store.id}')
    print(f'Admin Email: {user.email}')
\""

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
sudo systemctl start wink-api
sudo systemctl enable postgresql redis-server

# Setup frontend
cd /opt/wink/frontend
sudo -u wink npm install
sudo -u wink npm run build

# Setup Nginx
sudo tee /etc/nginx/sites-available/wink > /dev/null <<EOF
# API Backend
server {
    listen 80;
    server_name api.winkai.in;
    
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
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
        try_files \$uri \$uri/ /index.html;
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

# Setup SSL certificates
echo "🔒 Setting up SSL certificates..."
sudo certbot --nginx -d winkai.in -d www.winkai.in -d api.winkai.in --non-interactive --agree-tos --email admin@winkai.in

# Setup firewall
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw --force enable

echo "✅ Deployment complete!"
echo ""
echo "🌐 Your Wink AI Platform is now available at:"
echo "   Frontend: https://winkai.in"
echo "   API: https://api.winkai.in"
echo "   Health: https://api.winkai.in/health"
echo ""
echo "📋 Next steps:"
echo "1. Create your first store (see command above)"
echo "2. Add your OpenAI API key to /opt/wink/backend/.env"
echo "3. Restart the API: sudo systemctl restart wink-api"
echo "4. Monitor logs: sudo journalctl -u wink-api -f"
echo ""
echo "🔧 Service commands:"
echo "   Status: sudo systemctl status wink-api"
echo "   Logs: sudo journalctl -u wink-api -f"
echo "   Restart: sudo systemctl restart wink-api"