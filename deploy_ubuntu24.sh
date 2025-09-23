#!/bin/bash
# Ubuntu 24.04 VPS deployment script for Wink AI Platform
set -e

echo "🚀 Starting Wink AI Platform deployment for Ubuntu 24.04..."

# Update system
sudo apt update && sudo apt upgrade -y

# Install required packages for Ubuntu 24.04
sudo apt install -y python3 python3-venv python3-pip python3-dev build-essential \
    nodejs npm postgresql postgresql-contrib redis-server nginx certbot \
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

# Install Python packages one by one to handle any conflicts
echo "Installing Python packages..."
sudo -u wink ./venv/bin/pip install fastapi
sudo -u wink ./venv/bin/pip install uvicorn
sudo -u wink ./venv/bin/pip install sqlalchemy
sudo -u wink ./venv/bin/pip install psycopg2-binary
sudo -u wink ./venv/bin/pip install redis
sudo -u wink ./venv/bin/pip install python-multipart
sudo -u wink ./venv/bin/pip install python-jose[cryptography]
sudo -u wink ./venv/bin/pip install passlib[bcrypt]
sudo -u wink ./venv/bin/pip install opencv-python-headless
sudo -u wink ./venv/bin/pip install ultralytics
sudo -u wink ./venv/bin/pip install pillow
sudo -u wink ./venv/bin/pip install numpy
sudo -u wink ./venv/bin/pip install python-dotenv
sudo -u wink ./venv/bin/pip install pydantic
sudo -u wink ./venv/bin/pip install openai

# Setup environment
sudo -u wink cp .env.production .env
sudo -u wink sed -i 's|postgresql://user:pass@host:port/db|postgresql://wink_user:secure_wink_password_2025@localhost:5432/wink_analytics|g' .env
sudo -u wink sed -i "s|your-super-secure-jwt-secret-key-here-change-this-in-production|$(openssl rand -base64 32)|g" .env
sudo -u wink sed -i "s|your-openai-api-key-here|\${OPENAI_API_KEY:-sk-placeholder}|g" .env
sudo -u wink sed -i 's|https://yourdomain.com|https://api.winkai.in,https://winkai.in|g' .env

# Run migrations
cd /opt/wink/backend
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
echo "   Frontend: http://winkai.in (will be https after SSL setup)"
echo "   API: http://api.winkai.in (will be https after SSL setup)"
echo "   Health: http://api.winkai.in/health"
echo ""
echo "📋 Next steps:"
echo "1. Point your domain to this server's IP (159.65.153.124)"
echo "2. Run SSL setup: sudo certbot --nginx -d winkai.in -d www.winkai.in -d api.winkai.in --email admin@winkai.in"
echo "3. Create your first store:"
echo "   sudo -u wink /opt/wink/backend/venv/bin/python -c \""
echo "   import os"
echo "   os.chdir('/opt/wink/backend')"
echo "   os.environ['ALLOW_STORE_CREATION'] = 'true'"
echo "   from src.database.database import get_database"
echo "   from src.auth.auth_manager import get_auth_manager"
echo "   db = get_database()"
echo "   auth = get_auth_manager()"
echo "   with db.get_session() as session:"
echo "       store, user = auth.create_store_and_owner(session, 'Your Store Name', 'admin@winkai.in', 'your_secure_password')"
echo "       print(f'Store ID: {store.id}')"
echo "       print(f'Admin Email: {user.email}')"
echo "   \""
echo "4. Add your OpenAI API key to /opt/wink/backend/.env"
echo "5. Restart the API: sudo systemctl restart wink-api"
echo ""
echo "🔧 Service commands:"
echo "   Status: sudo systemctl status wink-api"
echo "   Logs: sudo journalctl -u wink-api -f"
echo "   Restart: sudo systemctl restart wink-api"