# 🚀 WINK AI Production HTTPS Deployment Guide

## Quick Start (Run on Production Server 206.189.141.6)

1. **Copy your project to the production server:**
   ```bash
   # On your local machine
   scp -r wink-full-code-pack root@206.189.141.6:/root/
   
   # SSH to production server
   ssh root@206.189.141.6
   cd /root/wink-full-code-pack
   ```

2. **Run the deployment script:**
   ```bash
   chmod +x deploy-production-https.sh
   ./deploy-production-https.sh
   ```

3. **Wait 2-3 minutes, then visit:**
   - https://winkai.in (Main WINK AI Dashboard)
   - https://api.winkai.in/healthz (API Health Check)
   - https://dashboard.winkai.in (Grafana Dashboard)
   - https://metrics.winkai.in (Prometheus Metrics)

## What the Script Does:

✅ **Removes `/etc/hosts` overrides** that block Let's Encrypt validation  
✅ **Opens firewall ports** 80 and 443  
✅ **Cleans old certificates** to start fresh  
✅ **Restarts services** with production Let's Encrypt  
✅ **Monitors certificate generation** in real-time  

## Expected Results:

After running the script, you should see:
```
[Tue Sep 23 08:XX:XX UTC 2025] Certificate issued successfully!
```

Then **https://winkai.in** will show:
- ✅ **Green padlock** (trusted certificate)
- ✅ **No browser warnings**
- ✅ **Beautiful WINK AI dashboard**

## Troubleshooting:

**If certificates fail to generate:**
```bash
# Check ACME logs
docker compose logs nginx-proxy-acme --tail 50

# Ensure DNS propagation
dig +short winkai.in  # Should return 206.189.141.6

# Test port connectivity
curl -I http://winkai.in  # Should connect
```

**If still getting warnings:**
```bash
# Force certificate regeneration
rm -rf nginx/certs/* nginx/acme.sh/*
docker compose restart nginx-proxy-acme
```

## Production URLs:

- **Main App**: https://winkai.in
- **API**: https://api.winkai.in
- **Dashboard**: https://dashboard.winkai.in  
- **Metrics**: https://metrics.winkai.in

All will have trusted SSL certificates from Let's Encrypt! 🔐✨