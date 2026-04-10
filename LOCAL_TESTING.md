## ✅ Local Testing Guide - Step by Step

### Prerequisites
- Backend API running
- Database configured (SQLite for local dev, Neon for production)

---

## STEP 1: Initialize Database (One-time)

**Windows PowerShell:**
```powershell
cd c:\Users\asiif\Downloads\Projects\job-market-analytics\backend
python init_db.py
```

**Expected Output:**
```
Database connection successful! Server time: ...
Creating database tables...
Database tables created successfully!
Found X tables: job, skill, job_skill, pipeline_run, ...
✓ Database initialization complete!
```

---

## STEP 2: Start Your Backend Locally

**Windows PowerShell (Terminal 1):**
```powershell
cd c:\Users\asiif\Downloads\Projects\job-market-analytics\backend
python -m src.api.main
```

**Expected Output:**
```
INFO:     Started server process [1234]
INFO:     Uvicorn running on http://0.0.0.0:8000
```

---

## STEP 3: Verify LOCAL Setup (Before Triggering Pipeline)

**Windows PowerShell (Terminal 2):**
```powershell
cd c:\Users\asiif\Downloads\Projects\job-market-analytics\backend
python verify_local_setup.py
```

**Expected First Output (empty database):**
```
✅ Database connected!
✅ Found 7 tables
   📋 job
   📋 skill
   📋 job_skill
   📋 pipeline_run
   ...
   
3️⃣  CHECKING DATA COUNTS
   📊 Jobs: 0
   📊 Skills: 0
   📊 Job-Skill relationships: 0
   📊 Pipeline runs logged: 0
```

---

## STEP 4: Trigger ETL Pipeline (Push New Data)

**Windows PowerShell (Terminal 2):**
```powershell
$response = Invoke-WebRequest -Uri "http://localhost:8000/api/pipeline/run?pages=2" -Method POST -UseBasicParsing
$response.Content | ConvertFrom-Json | ConvertTo-Json -Depth 3
```

**Expected Output (while running):**
```json
{
  "status": "success",
  "message": "Pipeline completed successfully",
  "run_id": 1,
  "statistics": {
    "jobs_inserted": 87,
    "duplicates_skipped": 13,
    "skills_extracted": 245,
    "errors": 0
  },
  "timestamp": "2026-04-11T01:15:32.123456"
}
```

⏳ **This takes 30-60 seconds - wait for completion**

---

## STEP 5: Verify Data Was Inserted

**After ETL completes, run verification again:**
```powershell
python verify_local_setup.py
```

**Expected Second Output (with data):**
```
✅ Database connected!
✅ Found 7 tables

3️⃣  CHECKING DATA COUNTS
   📊 Jobs: 87
   📊 Skills: 245
   📊 Job-Skill relationships: 342
   📊 Pipeline runs logged: 1

4️⃣  RECENT PIPELINE RUNS
   Run ID: 1
   Status: SUCCESS
   Started: 2026-04-11 01:15:00
   Finished: 2026-04-11 01:16:15
   Duration: 75.3s
   Jobs Fetched: 100
   Jobs Inserted: 87
   Jobs Skipped: 13

5️⃣  SAMPLE JOBS IN DATABASE
   Job 1:
   Title: Senior Python Developer
   Company: Tech Corp
   Location: San Francisco, CA
   Salary: $120,000 - $160,000
   Fetched: 2026-04-11 01:15:10
   Description: We're looking for an experienced Python developer...

✅ VERIFICATION COMPLETE
```

---

## STEP 6: Test API Endpoints

**Check a specific job search:**
```powershell
Invoke-WebRequest -Uri "http://localhost:8000/api/jobs/search?limit=3&sort_by=date" `
  -Method GET -UseBasicParsing | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json
```

**Check pipeline runs history:**
```powershell
Invoke-WebRequest -Uri "http://localhost:8000/api/pipeline/runs" `
  -Method GET -UseBasicParsing | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json
```

**Check skills extracted:**
```powershell
Invoke-WebRequest -Uri "http://localhost:8000/api/skills?limit=5" `
  -Method GET -UseBasicParsing | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json
```

---

## TROUBLESHOOTING

### ❌ "Database connection failed"
**Solution:** Check `.env` file has correct DATABASE_URL
```powershell
# Check environment variables
Get-Content backend\.env | Select-String DATABASE_URL
```

### ❌ "No tables found"
**Solution:** Run initialization script
```powershell
python backend/init_db.py
```

### ❌ "Port 8000 already in use"
**Solution:** Kill existing process and retry
```powershell
# Find process on port 8000
Get-NetTCPConnection -LocalPort 8000 | Select-Object -ExpandProperty OwningProcess

# Kill it (replace PID with actual number)
Stop-Process -Id <PID> -Force
```

### ❌ "0 jobs inserted"
**Solution:** Check Adzuna credentials in `.env`
```powershell
Get-Content backend\.env | Select-String ADZUNA
```

---

## QUICK SUMMARY

**One-Command Local Test:**
```powershell
# Terminal 1 - Start API
cd backend; python -m src.api.main

# Terminal 2 - After API starts, run all checks
cd backend; python verify_local_setup.py; `
Invoke-WebRequest -Uri "http://localhost:8000/api/pipeline/run?pages=2" -Method POST -UseBasicParsing; `
Start-Sleep -Seconds 90; python verify_local_setup.py
```

This will:
1. ✅ Check database connection
2. ✅ Show initial data counts (should be 0)
3. ✅ Trigger ETL pipeline
4. ✅ Wait for completion
5. ✅ Show final data counts (should have jobs, skills)

---

