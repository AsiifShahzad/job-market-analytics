# 🤖 Automated ETL Pipeline Setup Guide

This guide explains how to set up the Prefect ETL pipeline for automatic job data fetching and processing.

## Overview

The ETL pipeline automatically:
1. ✅ Fetches jobs from Adzuna API (every 6 hours)
2. ✅ Cleans and normalizes data
3. ✅ Extracts skills using NLP
4. ✅ Saves to Neon PostgreSQL database
5. ✅ Updates your dashboard in real-time

## Quick Start (Local Development)

### 1. Install Dependencies
```bash
cd backend
pip install -r requirements.txt
pip install prefect
```

### 2. Run the Flow Manually (Test)
```bash
python -m src.flows.etl_flow
```

This will fetch 3 pages from Adzuna and save to your database.

## Production Setup (Render.com)

### Option A: Using Prefect Cloud (Recommended for Render)

**Step 1: Create Free Prefect Account**
```bash
# Sign up at https://app.prefect.cloud
# Login locally:
prefect cloud login
```

**Step 2: Create Work Pool**
```bash
prefect work-pool create --type cloud-run render-etl
```

**Step 3: Deploy the Flow**
```bash
cd backend
python deploy_etl.py
```

**Step 4: Start Worker on Render**

Create a new service on Render:
- **Name:** prefect-etl-worker
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `prefect worker start --pool render-etl`
- **Environment Variables:**
  - `PREFECT_API_URL`: Your Prefect Cloud API URL
  - `PREFECT_API_KEY`: Your Prefect Cloud API key

---

### Option B: Cron Job on Render (Simpler)

Create a cron job that calls your API endpoint every 6 hours:

```bash
# Use a service like AWS Lambda, GitHub Actions, or EasyCron
# Configure to POST to:
# https://job-market-analytics-p4sy.onrender.com/api/pipeline/run?pages=5
```

---

## Manual Operations

### Fetch New Jobs (One-Time)
```bash
curl -X POST "http://localhost:8000/api/pipeline/run?pages=5"
```

### Check Database Status
```bash
curl -X GET "http://localhost:8000/api/pipeline/status"
```

### Clear All Data (WARNING!)
```bash
curl -X DELETE "http://localhost:8000/api/pipeline/reset"
```

---

## Monitoring

**Local Development:**
```bash
# Terminal 1: Run backend
cd backend
python run.py

# Terminal 2: Test API
curl http://localhost:8000/api/jobs/search?limit=10
```

**Prefect Cloud:**
- Visit: https://app.prefect.cloud
- View all flow runs, logs, and schedules

---

## Troubleshooting

**Issue: "Adzuna credentials not found"**
- ✅ Solution: Ensure `ADZUNA_APP_ID` and `ADZUNA_API_KEY` are in `.env`

**Issue: "Database connection failed"**
- ✅ Solution: Check `DATABASE_URL` in `.env`
- ✅ Solution: Verify Neon database is accessible

**Issue: "No jobs fetched"**
- ✅ Solution: Check Adzuna API status
- ✅ Solution: Verify API credentials are valid
- ✅ Solution: Check network connectivity

---

## Architecture

```
┌─────────────┐
│  Adzuna API │ (50 jobs per page, 5+ pages)
└──────┬──────┘
       │
       ▼
┌──────────────┐
│   Prefect    │ (Orchestration & Scheduling)
│ - Fetch Jobs │
│ - Clean Data │
│ - NLP Skills │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Neon Postgres│ (Real-time Data)
│  - 200+ Jobs │
│  - 50+ Skills│
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Frontend   │ (Dashboard)
│   React UI   │ (Live Updates)
└──────────────┘
```

---

## Next Steps

1. ✅ Test pipeline locally: `python -m src.flows.etl_flow`
2. ✅ Deploy to Prefect Cloud: `python deploy_etl.py`
3. ✅ Configure worker on Render
4. ✅ Monitor production runs on Prefect Cloud
5. ✅ Dashboard automatically updates every 6 hours

---

**Questions?** Check the backend README or Prefect docs at https://docs.prefect.io
