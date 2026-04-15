# JobPulse AI

A comprehensive job market intelligence platform that analyzes real job postings to uncover skills demand, salary trends, and hiring insights.

Live Demo: https://job-market-analytics-omega.vercel.app/

---

## Problem

Job seekers, career coaches, and HR professionals lack structured insights into what skills employers actually want, what they're willing to pay, and where the best opportunities are. Job boards show individual listings, but no one provides actionable, data-driven market intelligence across thousands of verified job postings.

---

## Impact

- Analyzes 10,000+ verified, deduplicated job postings in real time
- Extracts skills demand, salary benchmarks, and hiring patterns from raw job data
- Provides trend analysis — week-over-week growth rates for emerging skills
- Breaks down compensation by experience level, remote status, and location
- Streams pipeline execution so users watch data collection and validation happen live

---

## Solution

A full-stack platform with an ETL pipeline backend and an interactive React frontend.

The backend runs a multi-stage data pipeline: fetch jobs from Adzuna API, validate for authenticity (filters fake listings and low-quality postings), extract skills using NLP, and compute analytics with a 70-hour intelligent cache. The frontend displays dashboards for job search, skill trends, salary insights, and market overview — with real-time pipeline monitoring so users can trigger new data runs and track completion.

Every analysis returns skill demand rankings, trending skills with growth rates, salary insights by role and experience level, remote vs. on-site compensation analysis, top hiring cities, and market distribution by seniority.

---

## Tech Stack

**Backend:** FastAPI · SQLAlchemy · PostgreSQL (Neon) · Adzuna API · NLP (spaCy) · Pydantic · Structlog

**Frontend:** React 18 · Vite · Tailwind CSS · Recharts · React Router · TanStack Query · Zustand

**Icons & UI:** React Icons (Font Awesome 6) · Responsive Design

**Deployment:** Render (backend) · Vercel (frontend)

**Database:** Neon PostgreSQL with async support

---

## Author

**Asif Shahzad** — AI/ML Engineer  
[Portfolio](https://asiifshahzad.vercel.app) · [LinkedIn](https://www.linkedin.com/in/asiifshahzad) · [Email](mailto:shahzadasif041@gmail.com)

