#!/bin/bash

echo "🚀 WINK AI Production HTTPS Deployment Script"
echo "=============================================="
echo ""
echo "Run this script on your production server (206.189.141.6)"
echo ""

# Step 1: Remove hosts file entries that prevent Let's Encrypt validation
echo "Step 1: Removing local DNS overrides..."
sudo sed -i '/winkai\.in/d' /etc/hosts
echo "✅ Removed winkai.in entries from /etc/hosts"

# Step 2: Ensure ports are open
echo ""
echo "Step 2: Checking firewall ports..."
if command -v ufw &> /dev/null; then
    sudo ufw allow 80/tcp
    sudo ufw allow 443/tcp
    echo "✅ Opened ports 80 and 443 via ufw"
elif command -v firewall-cmd &> /dev/null; then
    sudo firewall-cmd --permanent --add-port=80/tcp
    sudo firewall-cmd --permanent --add-port=443/tcp
    sudo firewall-cmd --reload
    echo "✅ Opened ports 80 and 443 via firewall-cmd"
else
    echo "⚠️  Please ensure ports 80 and 443 are open in your cloud provider's security group"
fi

# Step 3: Clean up any existing certificates
echo ""
echo "Step 3: Cleaning up old certificates..."
rm -rf nginx/certs/* nginx/acme.sh/* 2>/dev/null
echo "✅ Cleaned up old certificates"

# Step 4: Stop services and restart fresh
echo ""
echo "Step 4: Restarting services..."
docker compose down
sleep 2
docker compose up -d

echo ""
echo "Step 5: Monitoring Let's Encrypt certificate generation..."
echo "This may take 2-3 minutes..."
echo ""

# Step 6: Monitor ACME logs
sleep 15
echo "ACME Logs:"
docker compose logs nginx-proxy-acme --tail 20

echo ""
echo "🎉 Production HTTPS deployment initiated!"
echo ""
echo "Next steps:"
echo "1. Wait 2-3 minutes for certificates to generate"
echo "2. Check status: docker compose logs nginx-proxy-acme"
echo "3. Visit https://winkai.in (should show trusted certificate)"
echo ""
echo "If you see 'Certificate issued successfully', HTTPS is ready!"