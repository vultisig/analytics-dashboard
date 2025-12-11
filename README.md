# Vultisig Analytics Dashboard

Analytics dashboard for tracking Vultisig swap metrics across multiple DEX aggregators including THORChain, MayaChain, LiFi, and Arkham.

## Architecture

```
analytics-dashboard/
├── dashboard/           # Next.js frontend (React 19, TypeScript, Tailwind)
├── vultisig-analytics/  # Python backend (Flask API, data ingestion)
└── docker-compose.yml   # Full stack orchestration
```

## Features

- Real-time swap volume, revenue, and user tracking
- Multi-provider analytics (THORChain, MayaChain, LiFi, Arkham)
- Historical data visualization with customizable date ranges
- Automatic data sync from blockchain APIs
- PostgreSQL + TimescaleDB for time-series data

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Git

### Run with Docker

```bash
# Clone the repository
git clone https://github.com/vultisig/analytics-dashboard.git
cd analytics-dashboard

# Create the database volume (first time only)
docker volume create vultisig-analytics_postgres_data

# Start all services
docker compose up -d
```

Services will be available at:
- **Dashboard**: http://localhost:3000
- **API**: http://localhost:8080
- **PostgreSQL**: localhost:5432

### Environment Variables

Create a `.env` file in the root directory:

```env
# Optional: Override default API keys
ARKHAM_API_KEY=your_key
LIFI_API_KEY=your_key
```

## Development

### Frontend (Next.js)

```bash
cd dashboard
npm install
npm run dev
```

### Backend (Python)

```bash
cd vultisig-analytics
pip install -r requirements.txt
python api_server.py
```

### Manual Data Ingestion

```bash
cd vultisig-analytics
python run_ingestion.py
```

## Services

| Service | Description | Port |
|---------|-------------|------|
| `dashboard` | Next.js frontend | 3000 |
| `backend` | Flask REST API | 8080 |
| `postgres` | TimescaleDB database | 5432 |
| `sync` | Continuous data ingestion | - |

## License

MIT
