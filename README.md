# Real-Time Trade Surveillance Platform

A production-style data engineering pipeline that detects market abuse patterns (spoofing, wash trading, front-running) in real-time using streaming market data.

## Architecture

```
┌─────────────────┐  ┌──────────────────┐
│  Finnhub WS     │  │  Order Generator  │
│  (Real prices)  │  │  (Synthetic)      │
└────────┬────────┘  └────────┬─────────┘
         │                    │
         ▼                    ▼
┌─────────────────────────────────────────┐
│            Apache Kafka                  │
│  Topics: market-data, orders, trades     │
└────────────────┬────────────────────────┘
                 │
         ┌───────┴───────┐
         ▼               ▼
┌─────────────────┐  ┌──────────────┐
│  Apache Flink   │  │  Data Lake   │
│  (Real-time     │  │  (S3/MinIO)  │
│   detection)    │  │  Bronze →    │
│                 │  │  Silver →    │
│  • Spoofing     │  │  Gold        │
│  • Wash trading │  │              │
│  • Front-running│  │              │
└────────┬────────┘  └──────────────┘
         │
         ▼
┌─────────────────┐
│  PostgreSQL     │
│  (Alerts DB)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Dashboard      │
│  (Compliance)   │
└─────────────────┘
```

## Tech Stack

| Component            | Technology           | Purpose                              |
|----------------------|----------------------|--------------------------------------|
| Market Data          | Finnhub WebSocket    | Real-time stock prices               |
| Order Simulation     | Python               | Synthetic trader behavior            |
| Streaming Ingestion  | Apache Kafka         | Central event bus                    |
| Stream Processing    | Apache Flink (PyFlink)| Real-time pattern detection         |
| Data Lake Storage    | MinIO (S3-compatible)| Raw + processed data                 |
| Table Format         | Apache Iceberg       | Lakehouse medallion layers           |
| Alert Storage        | PostgreSQL           | Surveillance alerts                  |
| Dashboard            | Streamlit            | Compliance analyst UI                |
| Orchestration        | Docker Compose       | Local development environment        |

## Detection Patterns

### 1. Spoofing Detection
Identifies traders who place large orders and rapidly cancel them to manipulate prices.
- **Signal**: High cancel-to-order ratio in rolling 2-minute windows
- **Context**: Cross-referenced with market volatility to reduce false positives

### 2. Wash Trading Detection
Detects circular trading between related accounts that creates artificial volume.
- **Signal**: Matching buy/sell patterns between account pairs on same asset
- **Context**: Time proximity, price similarity, volume matching

### 3. Front-Running Detection
Flags broker accounts that trade ahead of large client orders.
- **Signal**: Broker account trades same asset shortly before a large client order
- **Context**: Position direction alignment, timing window, order size

## Project Structure

```
trade-surveillance/
├── docker-compose.yml          # Full platform orchestration
├── services/
│   ├── market-data-collector/  # Finnhub WebSocket → Kafka
│   ├── order-generator/        # Synthetic order/trade events → Kafka
│   ├── flink-processor/        # Real-time surveillance rules
│   └── alert-service/          # Alert storage + dashboard
├── infrastructure/             # Kafka, MinIO, PostgreSQL configs
├── config/                     # Environment variables, thresholds
└── docs/                       # Architecture decisions, runbooks
```

## Quick Start

```bash
# 1. Clone and configure
cp .env.example .env
# Add your Finnhub API key to .env

# 2. Start the platform
docker-compose up -d

# 3. View the compliance dashboard
open http://localhost:8501
```

## Build Phases

- [x] Phase 1: Project structure + real market data ingestion
- [ ] Phase 2: Kafka streaming pipeline
- [ ] Phase 3: Flink real-time detection (spoofing, wash trading, front-running)
- [ ] Phase 4: Data lakehouse (Bronze → Silver → Gold)
- [ ] Phase 5: Compliance dashboard
- [ ] Phase 6: AWS deployment
