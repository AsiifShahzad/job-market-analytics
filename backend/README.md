# Backend - FastAPI Server & ETL Pipeline

Job market data processing, API endpoints, and database management.

## Structure

```
backend/
├── src/
│   ├── api/           # REST API endpoints
│   ├── flows/         # ETL pipeline orchestration
│   ├── ingestion/     # Data fetching and ingestion
│   ├── cleaning/      # Data cleaning and normalization
│   ├── features/      # Feature engineering
│   ├── nlp/           # NLP skill extraction
│   ├── db/            # Database models and setup
│   ├── intelligence/  # Analytics and insights
│   └── utils/         # Utilities, config, logging
├── config/            # YAML configuration
├── deploy/            # Systemd services, nginx, deployment scripts
├── dashboards/        # Analytics dashboards (Streamlit, etc.)
├── requirements.txt   # Python dependencies
├── alembic.ini        # Database migration config
└── test_api.py        # API tests
```

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

3. **Run backend server:**
   ```bash
   python -m src.api.main
   ```

## Database

- **Migrations:** `alembic upgrade head`
- **Init DB:** Database tables are auto-created on first run
- **Models:** See `src/db/models.py`

## API Endpoints

See FastAPI docs at: `http://localhost:8000/docs`

## Testing

```bash
python test_api.py
```

## Deployment

See `deploy/` folder for:
- Systemd service files
- Nginx configuration
- EC2 setup scripts
