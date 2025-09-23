
# Wink AI Analytics Platform

A production-ready multi-tenant retail analytics platform with real-time person detection, zone analytics, and AI-powered insights.

## Features

- **Multi-Tenant Architecture**: Complete data isolation between stores using Row-Level Security (RLS)
- **JWT Authentication**: Secure login system with invite-based user management
- **Real-Time Person Detection**: YOLO-based detection with person tracking and re-identification
- **Zone Analytics**: Point-in-polygon zone detection with footfall, dwell time, and interaction metrics
- **Auto-Scaling Processors**: Automatic RTSP processor startup when cameras are added
- **AI Insights**: OpenAI-powered analytics with promotional and festival spike detection
- **Production Ready**: PostgreSQL support, Redis caching, comprehensive logging, and health monitoring

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   Backend API   │    │   Database      │
│   React + TS    │───▶│   FastAPI       │───▶│   PostgreSQL    │
│   Multi-tenant  │    │   Multi-tenant  │    │   RLS Policies  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │ Camera Processor│    │     Redis       │
                       │ YOLO Detection  │───▶│ Real-time Data  │
                       │ Auto-scaling    │    │ Event Queue     │
                       └─────────────────┘    └─────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.11+
- Node.js 18+
- PostgreSQL 14+
- Redis 7+
- Docker (optional for processors)

### Installation

1. **Clone and setup backend:**
```bash
cd backend
cp .env.production .env  # Copy and configure environment
pip install -r requirements_new.txt
```

2. **Setup database:**
```bash
# Create PostgreSQL database
createdb wink_analytics

# Run migrations
python -c "from src.database.migrations import run_migrations; run_migrations()"
```

3. **Create initial store and owner:**
```bash
# Set ALLOW_STORE_CREATION=true in .env temporarily
python -c "
from src.database.database import get_database
from src.auth.auth_manager import get_auth_manager
db = get_database()
auth = get_auth_manager()
with db.get_session() as session:
    store, user = auth.create_store_and_owner(session, 'Your Store Name', 'admin@yourstore.com', 'secure_password')
    print(f'Store ID: {store.id}')
    print(f'Admin Email: {user.email}')
"
```

4. **Start backend:**
```bash
python src/main.py
```

5. **Setup frontend:**
```bash
cd ../frontend
npm install
npm run dev
```

### Environment Configuration

Key environment variables for production:

```bash
# Database
DATABASE_URL=postgresql://user:pass@host:port/db
ENABLE_RLS=true

# Authentication
JWT_SECRET_KEY=your-secret-key
OPENAI_API_KEY=your-openai-key

# Redis
REDIS_URL=redis://localhost:6379

# Security
ALLOWED_ORIGINS=https://yourdomain.com
ALLOW_STORE_CREATION=false
```

## How Login Works, How Data is Stored Per Store, and How Insights are Generated

### Authentication Flow

1. **User Registration via Invite:**
   - Store owner creates invite via `POST /api/auth/invite`
   - System generates secure invite token with expiration
   - Email sent with invite link (contains token)
   - User accepts invite via `POST /api/auth/accept-invite`
   - New user account created with bcrypt password hash

2. **Login Process:**
   - User submits email/password to `POST /api/auth/login`
   - System verifies password using bcrypt
   - JWT tokens generated (access + refresh) containing:
     ```json
     {
       "sub": "user_id",
       "email": "user@domain.com", 
       "store_id": "store_uuid",
       "role": "manager"
     }
     ```
   - Tokens returned to client with store information

3. **Request Authentication:**
   - Client includes `Authorization: Bearer <token>` header
   - Middleware validates JWT and extracts user/store context
   - User object injected into request dependencies

**Code Location**: `src/auth/auth_manager.py`, `src/auth/middleware.py`, `src/api/auth_routes.py`

### Multi-Tenant Data Storage

1. **Database Schema Design:**
   - Every table includes `store_id` column as UUID foreign key
   - Composite indexes on `(store_id, date)`, `(store_id, camera_id)`, etc.
   - Core entities: stores, users, cameras, zones, tracks, events, metrics_daily

2. **Row-Level Security (RLS) Enforcement:**
   ```sql
   -- Example RLS policy
   CREATE POLICY tenant_isolation ON cameras
   FOR ALL TO PUBLIC
   USING (store_id = current_setting('app.store_id')::uuid);
   ```
   - Enabled on all tenant tables when `ENABLE_RLS=true`
   - Middleware sets `app.store_id` session variable per request
   - Database automatically filters queries to current store

3. **Application-Level Isolation:**
   - All API endpoints filter by `store_id` from JWT token
   - Database sessions scoped to store context
   - Camera processors isolated by store in separate processes

4. **Data Flow Example:**
   ```
   Request → JWT Validation → Extract store_id → Set DB Context → Query Filtering
   ```

**Code Location**: `src/database/models.py`, `src/database/database.py`, `src/auth/middleware.py`

### Person Detection and Event Generation

1. **Camera Processor Lifecycle:**
   - Camera added via `POST /api/cameras` 
   - Background task auto-starts processor with RTSP URL
   - Processor runs YOLO detection on video frames
   - Person centroids tracked using centroid tracking algorithm
   - Zone intersection calculated using point-in-polygon

2. **Event Generation Pipeline:**
   ```
   RTSP Stream → YOLO Detection → Person Tracking → Zone Analysis → Event Generation
   ```
   - Events: `entry`, `exit`, `zone_entry`, `zone_exit`, `product_interaction`
   - Events stored in Redis queue + PostgreSQL table
   - Include person UUID, timestamp, zone info, dwell times

3. **Metrics Aggregation:**
   - Background job processes event queue every 5 minutes
   - Aggregates to daily metrics: footfall, unique visitors, dwell averages
   - Per-camera and per-zone breakdowns stored
   - Unique visitors calculated using person UUIDs (no cross-camera dedup)

**Code Location**: `src/services/processor_worker.py`, `src/services/camera_processor.py`

### AI Insights Generation

1. **Data Collection for Insights:**
   ```python
   # Weekly baseline metrics
   recent_metrics = get_daily_metrics(store_id, days=28)
   historical_metrics = get_daily_metrics(store_id, days=180) 
   
   # Promotional period comparison
   if promo_enabled:
       promo_metrics = get_metrics_for_period(promo_start, promo_end)
       baseline_metrics = get_metrics_before_period(promo_start, same_length)
   ```

2. **OpenAI Prompt Structure:**
   ```json
   {
     "store_performance": {
       "recent_period": "...",
       "comparison_period": "...",
       "trends": "..."
     },
     "promotional_analysis": {
       "promotion_metrics": "...",
       "baseline_comparison": "...",
       "lift_percentage": "..."
     },
     "zone_performance": {
       "per_zone_metrics": "...",
       "zone_efficiency": "..."
     }
   }
   ```

3. **Insight Generation Process:**
   - Triggered via `POST /api/insights/combined`
   - Collects store-scoped metrics and trends
   - Compares promotional periods vs baselines
   - Sends structured data to OpenAI GPT-4
   - Returns actionable insights with recommendations
   - Results cached in `insights` table

4. **Insight Content Examples:**
   - "Footfall increased 34% during Black Friday vs baseline week"
   - "Checkout zone showing 15% longer dwell times - consider staffing"
   - "Product interaction rate dropped 8% - review placement strategy"

**Code Location**: `src/api/insights_routes.py` (would be created), existing insights in `src/dashboard/web_server.py`

### Security Guarantees

1. **Tenant Isolation Verification:**
   ```python
   # Test that Store A cannot access Store B data
   def test_tenant_isolation():
       store_a_user = authenticate("user_a@store_a.com")
       store_b_cameras = api_call("/api/cameras", store_a_user.token)
       assert len(store_b_cameras) == 0  # Should see no Store B cameras
   ```

2. **Data Access Patterns:**
   - JWT tokens contain store_id scope
   - All queries automatically filtered by store_id
   - RLS provides database-level enforcement
   - No shared data between stores (cameras, zones, events, metrics)

3. **Verification Points:**
   - API endpoint tests for cross-tenant access attempts
   - Database queries logged and audited
   - JWT token validation on every request
   - Store context validation in middleware

**Code Location**: `tests/test_security.py` (would be created), security middleware in `src/auth/middleware.py`

## API Documentation

### Authentication Endpoints
- `POST /api/auth/login` - User authentication
- `POST /api/auth/refresh` - Token refresh
- `POST /api/auth/invite` - Create user invitation (store owner only)
- `POST /api/auth/accept-invite` - Accept invitation and create account
- `GET /api/auth/me` - Get current user info

### Camera Management
- `GET /api/cameras` - List store cameras
- `POST /api/cameras` - Add camera (auto-starts processor)
- `GET /api/cameras/{id}/health` - Camera status and processor info
- `POST /api/cameras/{id}/restart` - Restart camera processor
- `DELETE /api/cameras/{id}` - Remove camera and stop processor

### Zone Management
- `GET /api/zones` - List zones for store
- `POST /api/zones/screenshot` - Upload zone screenshot
- `POST /api/zones` - Create zone with polygon
- `GET /api/zones/overlay` - Get zone overlay image

### Analytics & Insights
- `GET /api/metrics/daily` - Daily aggregated metrics
- `GET /api/metrics/realtime` - Live camera counts
- `POST /api/insights/combined` - Generate AI insights
- `GET /api/analytics/spikes` - Detect promotional spikes

## Deployment

### Production Checklist

1. **Database Setup:**
   ```bash
   # Create production database
   createdb wink_analytics_prod
   
   # Enable RLS
   export ENABLE_RLS=true
   
   # Run migrations
   python -c "from src.database.migrations import run_migrations; run_migrations()"
   ```

2. **Security Configuration:**
   ```bash
   # Generate secure JWT secret
   export JWT_SECRET_KEY=$(openssl rand -base64 32)
   
   # Configure CORS
   export ALLOWED_ORIGINS="https://yourdomain.com"
   
   # Disable store creation
   export ALLOW_STORE_CREATION=false
   ```

3. **Container Deployment:**
   ```bash
   # Build application
   docker build -t wink-api:latest .
   
   # Run with proper environment
   docker run -d --name wink-api \
     --env-file .env.production \
     -p 8000:8000 wink-api:latest
   ```

### Migration Commands

```bash
# Migrate from legacy SQLite
export MIGRATE_FROM_LEGACY=true
export LEGACY_DB_PATH=/path/to/wink_store.db
python -c "from src.database.migrations import run_migrations; run_migrations()"

# Check schema version
python -c "from src.database.migrations import get_schema_version; print(get_schema_version())"
```

### Backup & Restore

```bash
# Backup
pg_dump wink_analytics > backup_$(date +%Y%m%d).sql

# Restore
createdb wink_analytics_restore
psql wink_analytics_restore < backup_20250923.sql
```

## Development

### Running Tests
```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-cov

# Run security tests
pytest tests/test_security.py -v

# Run with coverage
pytest --cov=src tests/
```

### Code Quality
```bash
# Format code
black src/
isort src/

# Lint
flake8 src/
```

### Adding New Stores
```bash
# Temporarily enable store creation
export ALLOW_STORE_CREATION=true

# Create via API
curl -X POST http://localhost:8000/api/auth/create-store \
  -H "Content-Type: application/json" \
  -d '{
    "store_name": "New Store",
    "owner_email": "owner@newstore.com", 
    "owner_password": "secure_password"
  }'
```

## Monitoring

### Health Checks
- `GET /health` - Application health status
- `GET /metrics` - Prometheus-compatible metrics
- Camera processor heartbeats in Redis

### Logging
- Structured JSON logging with loguru
- Request/response logging
- Database query logging (if SQL_DEBUG=true)
- Camera processor logs per camera

### Performance Monitoring
- Database query performance
- Camera processing frame rates
- API response times
- Event generation rates

## Troubleshooting

### Common Issues

1. **Camera Won't Connect:**
   - Check RTSP URL accessibility
   - Verify network connectivity
   - Check processor logs in `processors/{camera_id}/`

2. **Database Connection Failed:**
   - Verify DATABASE_URL format
   - Check PostgreSQL service status
   - Ensure database exists

3. **JWT Authentication Errors:**
   - Verify JWT_SECRET_KEY is set
   - Check token expiration
   - Validate CORS configuration

4. **RLS Policy Errors:**
   - Ensure ENABLE_RLS matches database setup
   - Check store_id session variable setting
   - Verify policy definitions

### Support

For technical support and bug reports, please contact the development team or check the application logs for detailed error information.

## License

Copyright © 2025 Wink AI Analytics Platform. All rights reserved.
