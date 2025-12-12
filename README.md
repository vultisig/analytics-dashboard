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
- **Holders Tab**: $VULT token holder tier distribution and address lookup
- **Referrals Tab**: Affiliate/referral tracking and performance metrics

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
MORALIS_API_KEY=your_key  # Required for Holders tab (free tier at moralis.io)
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

## Holders Tab

The Holders tab displays $VULT token holder tier distribution and allows users to check their tier status.

### Tier System

| Tier | $VULT Required | Discount |
|------|----------------|----------|
| Ultimate | 1,000,000 | 50 bps (100% off) |
| Diamond | 100,000 | 35 bps |
| Platinum | 15,000 | 25 bps |
| Gold | 7,500 | 20 bps |
| Silver | 3,000 | 10 bps |
| Bronze | 1,500 | 5 bps |

**THORGuard NFT Boost**: Holding a THORGuard NFT upgrades your tier by one level (max to Platinum).

### Blacklist Configuration

To exclude addresses from tier calculations (e.g., treasury, LP pools, exchanges), edit:

```
vultisig-analytics/config/blacklist.json
```

```json
{
  "description": "Addresses excluded from VULT holder tier calculations",
  "blacklist": [
    {
      "address": "0x...",
      "description": "Description of the address"
    }
  ]
}
```

Changes are applied on the next daily sync (00:00 UTC).

### Data Sync

- Holder data syncs daily at **00:00 UTC**
- Uses Moralis API to fetch VULT token and THORGuard NFT holders
- Requires `MORALIS_API_KEY` environment variable

## License

MIT
