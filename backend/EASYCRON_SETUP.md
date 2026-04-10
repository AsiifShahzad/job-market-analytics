# ⏰ EasyCron Setup Guide

**EasyCron** is a simple external service that triggers your API on a schedule. Great alternative to GitHub Actions if you prefer.

---

## Setup Steps (5 minutes)

### 1. Sign Up
- Go to: https://www.easycron.com/
- Sign up with email (free tier available)
- Verify email

### 2. Create New Cron Job
Click **"Create New Cron"** button

Fill in the fields:

| Field | Value |
|-------|-------|
| **Cron Expression** | `0 */6 * * *` |
| **URL** | `https://job-market-analytics-p4sy.onrender.com/api/pipeline/run?pages=5` |
| **HTTP Method** | `POST` |
| **HTTP Headers** | `Content-Type: application/json` |
| **Timeout** | `300` seconds |

### 3. Cron Expression Explained

`0 */6 * * *` means:
- **0** = At minute 0
- ***/6** = Every 6 hours
- **\* \* \*** = Every day, every month, every weekday

**Other options:**
- Every 3 hours: `0 */3 * * *`
- Every 12 hours: `0 */12 * * *`
- Daily at 9 AM UTC: `0 9 * * *`
- Weekly on Monday: `0 9 * * 1`

### 4. Save and Enable
- Click **"Save"**
- Toggle **Enable** switch to ON
- You should see: ✅ "Enabled"

### 5. Test It
- Click **"Execute Now"** button
- Wait 10 seconds
- Check execution log (should show HTTP 200)

---

## Monitoring

### View Execution History
1. Go to your cron job
2. Click **"Execution History"**
3. See all runs with timestamps and status codes

### Check Logs
Each execution shows:
- ✅ HTTP status code
- Response time
- Response body (if any)
- Any errors

### Common Status Codes
- **200** = ✅ Success
- **201** = ✅ Created (new data)
- **4xx** = ❌ Client error (check API URL)
- **5xx** = ❌ Server error (check Render backend)

---

## Example Cron Jobs

### Daily at Midnight
```
Cron: 0 0 * * *
URL: https://job-market-analytics-p4sy.onrender.com/api/pipeline/run?pages=3
```

### Four Times Daily
```
Cron: 0 0,6,12,18 * * *
URL: https://job-market-analytics-p4sy.onrender.com/api/pipeline/run?pages=5
```

### Every Hour
```
Cron: 0 * * * *
URL: https://job-market-analytics-p4sy.onrender.com/api/pipeline/run?pages=1
```

---

## Troubleshooting

### ❌ "Cron job failed"
- Check Render backend is running
- Verify your Render URL is correct
- Check Adzuna API is accessible

### ❌ "Timeout error"
- Increase timeout to 600 seconds
- Try fetching fewer pages (pages=3 instead of pages=5)

### ❌ "HTTP 404 Not Found"
- Double-check your Render URL
- Verify `/api/pipeline/run` endpoint exists
- Check your backend is deployed

### ✅ Not sure if it worked?
- Log in to EasyCron
- Click your cron job
- Click "Execute Now"
- Check execution logs immediately

---

## Advantages vs GitHub Actions

| Feature | EasyCron | GitHub Actions |
|---------|----------|-----------------|
| Setup Time | 5 min | 2 min |
| Free Tier | ✅ Yes | ✅ Yes |
| External Service | ✅ Yes | No |
| Logs | ✅ Built-in | GitHub Actions tab |
| Reliability | ⚠️ Depends on EasyCron | Native to GitHub |

---

## Cost

**EasyCron Free Tier:**
- ✅ Unlimited cron jobs
- ✅ Up to 6 runs per hour
- ✅ Email alerts
- ✅ Basic execution logs

**Premium (Optional):**
- $5+/month for advanced features
- Timeout up to 3600 seconds
- More detailed logs

---

## Recommendations

**Use EasyCron if:**
- You prefer external service
- Want simple UI
- Don't want GitHub Actions complexity

**Use GitHub Actions if:**
- You want free & reliable
- Already using GitHub
- Want integrated logging

---

**Next Step:** Decide between EasyCron or GitHub Actions and enable! 🚀
