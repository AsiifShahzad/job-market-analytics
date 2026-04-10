# 🤖 Automated ETL Pipeline Setup Guide

This guide explains how to set up automatic job data fetching and processing for your dashboard.

## Overview

The ETL pipeline automatically:
1. ✅ Fetches jobs from Adzuna API (every 6 hours)
2. ✅ Cleans and normalizes data
3. ✅ Extracts skills using NLP
4. ✅ Saves to Neon PostgreSQL database
5. ✅ Updates your dashboard in real-time

---

## 🚀 Production Setup (Render.com)

### ✅ Option A: GitHub Actions (RECOMMENDED - Free & Easy)

**What it does:** Automatically calls your Render API every 6 hours to fetch fresh data.

**Setup:**
1. ✅ Already created: [.github/workflows/etl-pipeline.yml](../../.github/workflows/etl-pipeline.yml)
2. Push to GitHub
3. GitHub Actions runs automatically on schedule
4. Your dashboard updates every 6 hours

**Features:**
- ✅ Completely free
- ✅ No external services needed
- ✅ Integrated with GitHub
- ✅ See logs in Actions tab
- ✅ Manual trigger available

**View runs:**
- Go to: https://github.com/AsiifShahzad/job-market-analytics/actions
- See all pipeline runs and logs

---

### Option B: EasyCron (Alternative)

If you prefer an external service, use **EasyCron**:

1. Go to: https://www.easycron.com/
2. Sign up (free tier available)
3. Create a new cron job:
   - **URL:** `https://job-market-analytics-p4sy.onrender.com/api/pipeline/run?pages=5`
   - **Method:** POST
   - **Cron Expression:** `0 */6 * * *` (every 6 hours)
4. Save and enable

---

### Option C: Using Prefect Cloud (Professional)

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

## 🧪 Local Development

### 1. Install Dependencies
```bash
cd backend
pip install -r requirements.txt
```

### 2. Run the Flow Manually (Test)

**Option A - Simple Test:**
```bash
python test_etl.py
```
Fetches 2 pages from Adzuna and shows results.

**Option B - Full ETL Flow:**
```bash
python -m src.flows.etl_flow
```
Fetches 3 pages using Prefect orchestration.

---

## 📡 Manual Operations

### Fetch New Jobs (One-Time)
```bash
curl -X POST "https://job-market-analytics-p4sy.onrender.com/api/pipeline/run?pages=5"
```

### Check Database Status
```bash
curl -X GET "https://job-market-analytics-p4sy.onrender.com/api/pipeline/status"
```

### Clear All Data (WARNING!)
```bash
curl -X DELETE "https://job-market-analytics-p4sy.onrender.com/api/pipeline/reset"
```

---

## 📊 Monitoring

**GitHub Actions:**
- Visit: https://github.com/AsiifShahzad/job-market-analytics/actions
- Click on "🤖 Automated ETL Pipeline"
- View all runs and logs

**EasyCron:**
- Visit: https://www.easycron.com/
- View execution history and logs

**Prefect Cloud:**
- Visit: https://app.prefect.cloud
- View all flow runs, logs, and schedules

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────┐
│         GitHub Actions / EasyCron       │
│     (Triggers every 6 hours)            │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│    Render Backend API Endpoint          │
│  POST /api/pipeline/run?pages=5         │
└────────────────┬────────────────────────┘
                 │
                 ▼
         ┌───────────────┐
         │  Adzuna API   │
         │ (50 jobs/pg)  │
         └───────┬───────┘
                 │
                 ▼
    ┌────────────────────────┐
    │  ETL Pipeline          │
    │ - Fetch               │
    │ - Clean               │
    │ - Extract Skills      │
    └────────────┬───────────┘
                 │
                 ▼
    ┌────────────────────────┐
    │ Neon PostgreSQL        │
    │ - Jobs                │
    │ - Skills              │
    │ - Job-Skill Links     │
    └────────────┬───────────┘
                 │
                 ▼
    ┌────────────────────────┐
    │ React Dashboard        │
    │ - Live Data           │
    │ - Auto-Updates        │
    └────────────────────────┘
```

---

## ✅ Recommended Setup (You're Here!)

**For your Render deployment:**

1. ✅ **Already done:** API endpoint ready at `https://job-market-analytics-p4sy.onrender.com/api/pipeline/run`
2. ✅ **Just created:** GitHub Actions workflow
3. 🔄 **Next step:** Push to GitHub
4. 🎉 **Result:** Dashboard auto-updates every 6 hours

---

## 🔄 How It Works

1. **Every 6 hours** → GitHub Actions trigger fires
2. **HTTP POST** → Calls your Render backend API
3. **Pipeline Runs** → Fetches 5 pages from Adzuna (~250 jobs)
4. **Data Cleaned** → Normalized, standardized
5. **Skills Extracted** → NLP processes job descriptions
6. **Database Updated** → New data saved to Neon
7. **Dashboard Refreshes** → Frontend auto-fetches new data

---

## 📋 Next Steps

- [ ] Push to GitHub: `git push origin main`
- [ ] Go to Actions tab: https://github.com/AsiifShahzad/job-market-analytics/actions
- [ ] Watch the first run execute
- [ ] Check your dashboard: https://job-market-analytics-p4sy.onrender.com
- [ ] Verify fresh data appears every 6 hours

---

## ❓ Troubleshooting

**Issue: "Action failed with status code"**
- ✅ Check your Render backend is running
- ✅ Verify API URL in workflow
- ✅ Check Adzuna credentials in `.env`

**Issue: "No data appears"**
- ✅ Verify database connection works
- ✅ Run manual test: `python test_etl.py`
- ✅ Check Render logs

**Issue: "Want to run manually?"**
- ✅ Go to Actions tab
- ✅ Click workflow
- ✅ Click "Run workflow" button

---

**Questions?** Check Prefect docs at https://docs.prefect.io or create an issue in GitHub!
