
WINK AI – Full Code Pack

Local dev:
  cd backend && cp .env.example .env && pip install -r requirements.txt && python -m src.dashboard.web_server
  cd frontend && npm install && npm run dev

Docker + TLS on VM:
  - point api.winkai.in to your VM IP
  - docker compose up -d --build

Use:
  - Cameras: add RTSP + names
  - Zones: upload screenshot (W,H), add polygons, preview overlay
  - Insights: weeks + promo/festival with dates
