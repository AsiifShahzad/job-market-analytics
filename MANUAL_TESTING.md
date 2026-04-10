## 🧪 MANUAL TESTING GUIDE

### STEP 1: Verify Database Connection
```powershell
cd c:\Users\asiif\Downloads\Projects\job-market-analytics\backend
python init_db.py
```

**Expected Output:**
```
✅ Database connection successful! Server time: ...
✅ Database tables created successfully!
✅ Found 5 tables: job, skill, job_skill, pipeline_run, skill_snapshot
✓ Database initialization complete!
```

---

### STEP 2: Start the API Server
```powershell
cd c:\Users\asiif\Downloads\Projects\job-market-analytics\backend
uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
```

**Expected Output:**
```
INFO:     Started server process [12345]
INFO:     Uvicorn running on http://0.0.0.0:8000
INFO:     Application startup complete
```

✅ **Leave this running** - open a NEW PowerShell terminal for the next steps

---

### STEP 3: Check API Health
```powershell
Invoke-WebRequest -Uri "http://localhost:8000/api/health" -Method GET -UseBasicParsing | Select-Object -ExpandProperty Content
```

**Expected Output:**
```json
{
  "status": "healthy",
  "database": "unknown",
  "timestamp": "2026-04-10T20:33:32.371413",
  "cache_stats": {
    "total_entries": 0,
    "max_entries": 200,
    "utilization_percent": 0.0
  }
}
```

---

### STEP 4: Check Current Data Count (BEFORE pipeline)
```powershell
cd c:\Users\asiif\Downloads\Projects\job-market-analytics\backend
python verify_local_setup.py
```

**Expected Output:**
```
📊 Jobs in database: 120
📊 Skills in database: 0  
📊 Pipeline runs logged: 4
```

---

### STEP 5: Trigger Pipeline to Fetch New Jobs
```powershell
$response = Invoke-WebRequest -Uri "http://localhost:8000/api/pipeline/run?pages=2" -Method POST -UseBasicParsing
$response.Content | ConvertFrom-Json | ConvertTo-Json -Depth 3
```

**Expected Output:**
```json
{
  "status": "success",
  "message": "Pipeline completed successfully",
  "run_id": 5,
  "statistics": {
    "jobs_inserted": 85,
    "duplicates_skipped": 15,
    "skills_extracted": 0,
    "errors": 0
  },
  "timestamp": "2026-04-11T01:45:32.123456"
}
```

⏳ **This takes 30-60 seconds - wait for completion**

---

### STEP 6: Verify Data Was Inserted
```powershell
python verify_local_setup.py
```

**Expected Changes:**
```
📊 Jobs in database: 205  (increased from 120)
📊 Pipeline runs logged: 5  (increased from 4)
```

---

### STEP 7: Test Jobs Endpoint
```powershell
$response = Invoke-WebRequest -Uri "http://localhost:8000/api/jobs/search?limit=3" -Method GET -UseBasicParsing
$response.Content | ConvertFrom-Json | ConvertTo-Json -Depth 2
```

**Expected Output:**
```json
{
  "data": [
    {
      "id": "5695783861",
      "title": "Senior Software Engineer, Platform",
      "company": "Anduril Industries",
      "location": "South Boston, Suffolk County",
      "salary_min": 242372.14,
      "salary_max": 242372.14,
      "description": "Anduril Industries is a defense technology company..."
    },
    ...
  ],
  "pagination": {
    "page": 1,
    "limit": 3,
    "total": 205,
    "pages": 69
  }
}
```

---

### STEP 8: Test Pipeline Runs Endpoint
```powershell
$response = Invoke-WebRequest -Uri "http://localhost:8000/api/pipeline/runs" -Method GET -UseBasicParsing
$response.Content | ConvertFrom-Json | ConvertTo-Json -Depth 2
```

**Expected Output:**
```json
{
  "runs": [
    {
      "id": 5,
      "started_at": "2026-04-10T20:45:31.891448Z",
      "finished_at": "2026-04-10T20:46:22.709912Z",
      "status": "success",
      "jobs_fetched": 100,
      "jobs_inserted": 85,
      "jobs_skipped": 15,
      "error_message": null,
      "duration_seconds": 50.818464
    },
    {
      "id": 4,
      "started_at": "2026-04-10T20:31:31.891448Z",
      ...
    }
  ],
  "total_count": 5,
  "cache_status": "MISS"
}
```

---

### STEP 9: Test Filter with Parameters
```powershell
$response = Invoke-WebRequest -Uri "http://localhost:8000/api/jobs/search?limit=5&sort_by=salary_min" -Method GET -UseBasicParsing
$response.Content | ConvertFrom-Json | ConvertTo-Json -Depth 1
```

**Expected Output:**
```
Jobs sorted by salary (ascending)
```

---

### STEP 10: Check API Documentation
Open in your browser:
```
http://localhost:8000/api/docs
```

✅ **You'll see interactive Swagger UI with all endpoints**

---

## 🔍 Verify Everything Works

**Checklist:**
- [ ] Database initialized successfully
- [ ] API server running on port 8000
- [ ] Health check returns `healthy`
- [ ] Initial data count: 120 jobs
- [ ] Pipeline runs successfully
- [ ] New jobs inserted into database
- [ ] /api/jobs/search returns job data
- [ ] /api/pipeline/runs returns execution history
- [ ] Interactive docs available at /api/docs

---

## 🚀 Ready for Production

Once all tests pass:
1. All code is pushed to GitHub ✅
2. Database is on Neon (cloud) ✅
3. API works locally and is ready for Render deployment ✅

Next: Deploy to Render by pushing to main branch

---
