# Python Backend - Sports Quant Engine

## Structure

```
python-backend/
├── api/              # API routes (future expansion)
├── services/         # Business logic & external APIs
│   ├── allsports_client.py
│   ├── theoddsapi_client.py
│   ├── data_pipeline.py
│   ├── ml_predictor.py
│   └── ...
├── models/           # Data models
├── utils/            # Utilities & validators
├── scripts/          # Standalone scripts
├── sql/              # SQL files
├── docs/             # Documentation
├── main.py           # FastAPI application
└── db_connection.py  # Database connection
```

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials
```

## Run

```bash
uvicorn main:app --reload --port 8000
```

## API Documentation

Visit http://localhost:8000/docs
