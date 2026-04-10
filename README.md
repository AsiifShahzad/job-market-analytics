# Job Market Analytics Platform

> Automated pipeline that extracts real-time job market trends, in-demand skills, and salary signals from 50K+ live records — reducing manual analysis time by 70%.

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)

[![Status](https://img.shields.io/badge/Status-Production-brightgreen?style=flat)]()

[![Records](https://img.shields.io/badge/Data-50K%2B%20Records-blue?style=flat)]()

---

## Problem Statement

Job seekers and analysts waste hours manually scanning job boards to identify trending roles, required skills, and salary benchmarks. This platform automates the entire pipeline — from raw data collection to actionable visual insights — in a fully repeatable, scalable way.

---

## Key Results

| Metric | Result |

|--------|--------|

| Records processed | 50,000+ live job listings |

| Manual analysis time saved | 70% reduction |

| Skills extracted (NLP) | 200+ unique technical skills ranked |

| Data sources | Multiple job boards (CSV + SQL) |

---

## System Architecture

```

Raw Job Data (CSV/SQL)

        │

        ▼

┌─────────────────┐

│  Data Ingestion │  ← Scraping / CSV import / SQL queries

└────────┬────────┘

         │

         ▼

┌─────────────────┐

│  Data Cleaning  │  ← Deduplication, null handling, normalization

└────────┬────────┘

         │

         ▼

┌──────────────────────┐

│  NLP Feature Extract │  ← Skill extraction, role classification, salary parsing

└────────┬─────────────┘

         │

         ▼

┌─────────────────┐

│  Analytics Engine│  ← Trend analysis, ranking, aggregations

└────────┬────────┘

         │

         ▼

┌─────────────────┐

│  Dashboard/Report│  ← Power BI / Matplotlib visualizations

└─────────────────┘

```

---

## Tech Stack

| Layer | Tools |

|-------|-------|

| Data Collection | Python, BeautifulSoup / Scrapy, SQL |

| Processing | Pandas, NumPy |

| NLP | spaCy / regex-based skill extractor |

| Storage | PostgreSQL / SQLite |

| Visualization | Power BI, Matplotlib, Seaborn |

| Environment | Python 3.10+, virtualenv |

---

## Features

- **Automated ingestion** — loads job data from CSV exports or direct SQL queries

- **Skill frequency ranking** — extracts and ranks 200+ technical skills by demand

- **Role trend analysis** — identifies fastest-growing job titles over time

- **Salary parsing** — normalizes salary ranges across formats

- **Interactive dashboard** — Power BI report with slicers by location, role, and skill

- **Repeatable pipeline** — single command reruns full analysis on fresh data

---

## Setup & Usage

```bash

# 1. Clone the repo

git clone https://github.com/asiifshahzad/job-market-analytics.git

cd job-market-analytics

# 2. Create virtual environment

python -m venv venv

source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies

pip install -r requirements.txt

# 4. Add your data

# Place CSV files in data/raw/ or configure SQL connection in src/ingest.py

# 5. Run the pipeline

python src/ingest.py

python src/clean.py

python src/analyze.py

```

---

## Sample Insights

- **Top 5 in-demand skills:** Python, SQL, Machine Learning, Power BI, Docker

- **Fastest growing role:** AI/ML Engineer (+38% YoY in listings)

- **Avg. salary range (ML roles):** PKR 150K–350K/month

---

## 👤 Author

**Asif Shahzad** — AI/ML Engineer  

[Portfolio](https://asiifshahzad.vercel.app/) · [LinkedIn](https://www.linkedin.com/in/asiifshahzad) · [Email](mailto:shahzadasif041@gmail.com)

