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

## AWS Production Architecture

The local Docker Compose stack maps directly to managed AWS services. Each component is swapped for its cloud-native equivalent to eliminate operational overhead, gain automatic scaling, and meet the uptime and compliance requirements of a production trading environment.

```
                    ┌──────────────────────────────────────────────────────────────────────┐
                    │                        AWS Cloud  (VPC)                              │
                    │                                                                      │
  ┌─────────────┐   │  ┌──────────────────────────────────────────────────────────────┐   │
  │  Finnhub    │   │  │  Public Subnet  (NAT Gateway egress only)                    │   │
  │  WebSocket  ├───┼─▶│                                                              │   │
  │  (external) │   │  │  ┌──────────────────────┐  ┌──────────────────────────────┐ │   │
  └─────────────┘   │  │  │  ECS Fargate          │  │  ECS Fargate                 │ │   │
                    │  │  │  market-data-collector│  │  order-generator             │ │   │
                    │  │  └──────────┬────────────┘  └───────────────┬──────────────┘ │   │
                    │  └────────────────────────────────────────────────────────────── │   │
                    │               │                                 │                │   │
                    │  ┌────────────▼─────────────────────────────── ▼ ─────────────┐ │   │
                    │  │  Private Subnet A                                           │ │   │
                    │  │                                                             │ │   │
                    │  │  ┌──────────────────────────────────────────────────────┐  │ │   │
                    │  │  │              Amazon MSK  (Apache Kafka)              │  │ │   │
                    │  │  │  Topics: market-data-stocks  market-data-crypto      │  │ │   │
                    │  │  │           orders  trades  alerts                     │  │ │   │
                    │  │  │  3 brokers · Multi-AZ · TLS + SASL/SCRAM auth        │  │ │   │
                    │  │  └─────────────────────┬────────────────────────────────┘  │ │   │
                    │  │                         │                                   │ │   │
                    │  │          ┌──────────────┴────────────┐                      │ │   │
                    │  │          ▼                           ▼                      │ │   │
                    │  │  ┌───────────────────┐   ┌───────────────────────────────┐ │ │   │
                    │  │  │  Amazon Managed   │   │  Amazon S3  (Data Lake)       │ │ │   │
                    │  │  │  Flink            │   │                               │ │ │   │
                    │  │  │                   │   │  bronze/  raw JSONL           │ │ │   │
                    │  │  │  • Spoofing job   │   │  silver/  cleaned Parquet     │ │ │   │
                    │  │  │  • Wash-trade job │   │  gold/    Iceberg tables      │ │ │   │
                    │  │  │  • Alerts → MSK   │   │                               │ │ │   │
                    │  │  └─────────┬─────────┘   └──────────────┬────────────────┘ │ │   │
                    │  │            │                             │                  │ │   │
                    │  │            ▼                   ┌─────────▼──────────┐       │ │   │
                    │  │  ┌─────────────────────┐       │  AWS Glue          │       │ │   │
                    │  │  │  Amazon RDS          │       │  bronze → silver   │       │ │   │
                    │  │  │  PostgreSQL          │       │  silver → gold     │       │ │   │
                    │  │  │  Multi-AZ · KMS enc  │       │  Iceberg compaction│       │ │   │
                    │  │  │  Alerts + audit log  │       └────────────────────┘       │ │   │
                    │  │  └─────────┬────────────┘                                    │ │   │
                    │  └─────────────────────────────────────────────────────────────┘ │   │
                    │               │                                                   │   │
                    │  ┌────────────▼─────────────────────────────────────────────┐    │   │
                    │  │  Public Subnet B                                          │    │   │
                    │  │  ECS Fargate — Streamlit Dashboard                        │    │   │
                    │  │  ALB (HTTPS · port 443) · Cognito auth                   │    │   │
                    │  └───────────────────────────────────────────────────────────┘    │   │
                    └──────────────────────────────────────────────────────────────────┘   │
                                                                                            │
```

### Why each managed service

| Local (Docker Compose) | AWS Production | Reason for the swap |
|---|---|---|
| Apache Kafka (self-managed) | **Amazon MSK** | Eliminates broker patching, ZooKeeper management, and disk scaling. MSK handles Multi-AZ replication, TLS encryption, SASL/SCRAM auth, and CloudWatch metrics with no ops burden. A 3-broker `kafka.m5.xlarge` cluster sustains ~200 MB/s throughput. |
| Apache Flink (self-managed) | **Amazon Managed Service for Apache Flink** | No cluster provisioning — jobs deploy as Python packages. AWS manages S3 checkpointing, task-manager auto-scaling, and job-manager HA failover. Billed per Kinesis Processing Unit consumed, not by instance uptime. |
| MinIO (local) | **Amazon S3 + Apache Iceberg** | S3 provides 11-nines durability at $0.023/GB/month with zero servers. Iceberg adds full ACID transactions, time-travel queries, and schema evolution on top of the raw Parquet partitions — critical for regulatory audit trails and MiFID II record-keeping. AWS Glue Data Catalog indexes the tables for Athena and Redshift Spectrum queries. |
| Python batch ETL | **AWS Glue** | Serverless Spark for the bronze → silver → gold transforms. Glue triggers on S3 event notifications, eliminating the polling loop and hand-rolled state file. Job bookmarks provide exactly-once processing across restarts. |
| PostgreSQL (Docker) | **Amazon RDS PostgreSQL Multi-AZ** | Automatic failover in under 60 seconds, point-in-time recovery, and storage auto-scaling. KMS encryption at rest is a compliance requirement for financial alert data in most jurisdictions (SOC 2, PCI-DSS). |
| Docker Compose | **ECS Fargate** | Serverless containers — no EC2 fleet to patch. Each microservice runs as an independent Fargate task with its own IAM task role, CPU/memory allocation, and CloudWatch log group. The ALB handles HTTPS termination, health checks, and sticky sessions for the dashboard. |

See [`docs/aws-architecture.md`](docs/aws-architecture.md) for estimated monthly costs, IAM roles, and VPC configuration details.

---

## Build Phases

- [x] Phase 1: Project structure + real market data ingestion
- [x] Phase 2: Kafka streaming pipeline
- [x] Phase 3: Flink real-time detection (spoofing, wash trading)
- [x] Phase 4: Data lakehouse (Bronze → Silver → Gold)
- [x] Phase 5: Compliance dashboard
- [ ] Phase 6: AWS deployment
