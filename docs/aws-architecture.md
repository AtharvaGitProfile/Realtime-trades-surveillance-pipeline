# AWS Production Deployment — Architecture Notes

Detailed reference for deploying the Trade Surveillance Platform on AWS. Covers service sizing, estimated costs, IAM roles, and VPC layout.

---

## Table of Contents

1. [Service Mapping](#1-service-mapping)
2. [VPC and Network Layout](#2-vpc-and-network-layout)
3. [Service Configuration](#3-service-configuration)
4. [IAM Roles](#4-iam-roles)
5. [Estimated Monthly Costs](#5-estimated-monthly-costs)
6. [Data Flow and Security](#6-data-flow-and-security)
7. [Deployment Order](#7-deployment-order)

---

## 1. Service Mapping

| Local service | AWS service | Notes |
|---|---|---|
| Kafka (Docker) | Amazon MSK | kafka.m5.xlarge, 3 brokers, Multi-AZ |
| Flink (Docker) | Amazon Managed Service for Apache Flink | PyFlink 1.18, auto-scaling KPUs |
| MinIO | Amazon S3 | Single bucket, lifecycle rules per prefix |
| Python ETL | AWS Glue | Glue 4.0 (Spark 3.3), triggered by S3 events |
| PostgreSQL (Docker) | Amazon RDS PostgreSQL 16 | db.t3.medium, Multi-AZ, encrypted |
| Streamlit (Docker) | ECS Fargate | 1 vCPU / 2 GB, behind ALB |
| Market-data collector | ECS Fargate | 0.5 vCPU / 1 GB |
| Order generator | ECS Fargate | 0.5 vCPU / 1 GB |

---

## 2. VPC and Network Layout

### CIDR block recommendation

```
VPC:  10.0.0.0/16

  Public subnets  (one per AZ — NAT Gateway, ALB)
    10.0.0.0/24   us-east-1a
    10.0.1.0/24   us-east-1b
    10.0.2.0/24   us-east-1c

  Private subnets (one per AZ — MSK, Flink, RDS, Fargate tasks)
    10.0.10.0/24  us-east-1a
    10.0.11.0/24  us-east-1b
    10.0.12.0/24  us-east-1c
```

### Routing

- Private subnets route `0.0.0.0/0` through a NAT Gateway in the corresponding public subnet.
- Public subnets route `0.0.0.0/0` directly through the Internet Gateway.
- The dashboard ALB lives in the public subnets; its target group (Fargate tasks) lives in the private subnets.

### Security Groups

| Security Group | Inbound | Outbound | Attached to |
|---|---|---|---|
| `sg-msk` | TCP 9094 (TLS) from `sg-fargate-tasks`, `sg-flink` | None (stateful) | MSK brokers |
| `sg-flink` | None (Flink is managed, no inbound needed) | TCP 9094 to `sg-msk`, TCP 443 to S3 VPC endpoint | Managed Flink VPC connection |
| `sg-rds` | TCP 5432 from `sg-fargate-tasks` | None | RDS |
| `sg-fargate-tasks` | None (tasks initiate outbound) | TCP 9094 to `sg-msk`, TCP 443 to S3/internet | Fargate task ENIs |
| `sg-alb` | TCP 443 from `0.0.0.0/0` | TCP 8501 to `sg-fargate-dashboard` | Application Load Balancer |
| `sg-fargate-dashboard` | TCP 8501 from `sg-alb` | TCP 5432 to `sg-rds`, TCP 9094 to `sg-msk`, TCP 443 to S3 | Dashboard Fargate task |

### VPC Endpoints (recommended)

Using VPC endpoints avoids NAT Gateway data charges for high-volume S3 and Glue traffic:

- **S3 Gateway endpoint** — free, covers all S3 traffic from private subnets
- **Glue Interface endpoint** — ~$7/month, eliminates NAT for Glue API calls
- **CloudWatch Logs Interface endpoint** — ~$7/month, covers container log shipping

---

## 3. Service Configuration

### Amazon MSK

```
Broker instance:   kafka.m5.xlarge  (4 vCPU, 16 GB RAM)
Number of brokers: 3  (one per AZ)
Storage per broker: 1 TB (gp3, expandable)
Kafka version:     3.6.0
Auth:              SASL/SCRAM (credentials stored in AWS Secrets Manager)
Encryption:        TLS in-transit + KMS at-rest
Replication factor: 3  (all topics)
Retention:         orders/trades: 7 days  |  alerts: 30 days  |  market-data: 3 days
```

Topic partitions match the local setup (3 each) but can be increased without downtime via MSK's managed partition reassignment.

### Amazon Managed Service for Apache Flink

```
Flink version:     1.18
Runtime:           Python (PyFlink wheel uploaded to S3)
Parallelism:       4 (auto-scales up to 16 KPUs under load)
Checkpointing:     every 60 s → S3 (s3://surveillance-lake/flink-checkpoints/)
Log delivery:      CloudWatch Logs  (/aws/flink/spoofing-detector, /aws/flink/wash-trade-detector)
```

Each Flink job is a separate Managed Flink application. The jobs are identical to the local PyFlink code; only the Kafka bootstrap servers and connector JAR paths change (JAR uploaded to S3 and referenced via `pipeline.jars`).

### Amazon S3 — Data Lake

```
Bucket:  s3://trade-surveillance-prod-lake-{account-id}
Region:  same as VPC (us-east-1)

Prefixes and lifecycle rules:
  bronze/    →  Standard storage, expire after 90 days
  silver/    →  Standard storage, transition to S3-IA after 30 days, expire after 1 year
  gold/      →  Standard storage, transition to S3-IA after 60 days, no expiry (regulatory)
  flink-checkpoints/  →  Standard storage, expire after 7 days

Versioning:  Enabled on gold/ (Iceberg needs it for snapshot metadata)
Encryption:  SSE-KMS (aws/s3 key, or dedicated CMK for stricter key rotation)
```

The gold layer uses **Apache Iceberg** table format with the AWS Glue Data Catalog as the catalog. This enables:
- Athena queries directly against `gold.trader_daily_activity`, `gold.asset_hourly_summary`, etc.
- Redshift Spectrum external tables for BI tools
- Full `SELECT ... AS OF TIMESTAMP` time-travel for regulatory lookbacks

### AWS Glue

```
Glue version:    4.0  (Spark 3.3, Python 3.10)
Worker type:     G.1X  (4 vCPU, 16 GB RAM per worker)
Workers:         2 (auto-scales to 10 for large backfills)
Trigger:         S3 event notification → EventBridge rule → Glue job
Schedule:        Also runs on a 5-minute cron as a fallback
```

The Glue job is a direct port of `silver_gold_writer.py` with the MinIO client replaced by native Spark S3A reads/writes and Iceberg writes via the Glue Iceberg connector. Job bookmarks replace the `silver/_state/last_processed.json` pattern.

### Amazon RDS PostgreSQL

```
Engine:          PostgreSQL 16.2
Instance:        db.t3.medium  (2 vCPU, 4 GB RAM)
Storage:         100 GB gp3, auto-scaling enabled (max 1 TB)
Multi-AZ:        Yes
Encryption:      KMS (dedicated CMK)
Backup retention: 35 days  (point-in-time recovery)
Deletion protection: Enabled
Parameter group: max_connections=200, log_min_duration_statement=1000
```

In production the Flink alert sink writes directly to RDS via JDBC rather than through Kafka, eliminating one hop for the compliance alert path.

### ECS Fargate

```
Cluster:  trade-surveillance-prod

Services:
  market-data-collector   0.5 vCPU / 1024 MB   desired: 1   min: 1   max: 2
  order-generator         0.5 vCPU / 1024 MB   desired: 1   min: 1   max: 1
  dashboard               1.0 vCPU / 2048 MB   desired: 2   min: 1   max: 4

Container registry:  Amazon ECR  (one repo per service)
Log driver:          awslogs → CloudWatch Logs  (30-day retention)
Platform version:    LATEST (Fargate 1.4.0+)
```

The dashboard runs with 2 desired tasks behind an ALB for zero-downtime deploys. Cognito User Pools provides authentication — the ALB listener rule requires a valid Cognito JWT before forwarding to Fargate.

---

## 4. IAM Roles

### `trade-surveillance-msk-producer` (Fargate tasks: collector, generator)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "MSKConnect",
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:WriteData"
      ],
      "Resource": [
        "arn:aws:kafka:us-east-1:ACCOUNT:cluster/trade-surveillance/*",
        "arn:aws:kafka:us-east-1:ACCOUNT:topic/trade-surveillance/*/market-data-stocks",
        "arn:aws:kafka:us-east-1:ACCOUNT:topic/trade-surveillance/*/market-data-crypto",
        "arn:aws:kafka:us-east-1:ACCOUNT:topic/trade-surveillance/*/orders",
        "arn:aws:kafka:us-east-1:ACCOUNT:topic/trade-surveillance/*/trades"
      ]
    },
    {
      "Sid": "SecretsManagerMSKCreds",
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:us-east-1:ACCOUNT:secret:msk/sasl-creds-*"
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:us-east-1:ACCOUNT:log-group:/ecs/trade-surveillance/*:*"
    }
  ]
}
```

### `trade-surveillance-flink-execution` (Managed Flink applications)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Checkpoints",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::trade-surveillance-prod-lake-ACCOUNT/flink-checkpoints/*",
        "arn:aws:s3:::trade-surveillance-prod-lake-ACCOUNT"
      ]
    },
    {
      "Sid": "S3AlertsWrite",
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::trade-surveillance-prod-lake-ACCOUNT/bronze/alerts/*"
    },
    {
      "Sid": "MSKReadWrite",
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeGroup",
        "kafka-cluster:AlterGroup",
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:ReadData",
        "kafka-cluster:WriteData"
      ],
      "Resource": [
        "arn:aws:kafka:us-east-1:ACCOUNT:cluster/trade-surveillance/*",
        "arn:aws:kafka:us-east-1:ACCOUNT:topic/trade-surveillance/*/*",
        "arn:aws:kafka:us-east-1:ACCOUNT:group/trade-surveillance/*/*"
      ]
    },
    {
      "Sid": "CloudWatchMetrics",
      "Effect": "Allow",
      "Action": ["cloudwatch:PutMetricData", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "*"
    }
  ]
}
```

### `trade-surveillance-glue-etl` (Glue jobs)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3LakeReadWrite",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::trade-surveillance-prod-lake-ACCOUNT",
        "arn:aws:s3:::trade-surveillance-prod-lake-ACCOUNT/*"
      ]
    },
    {
      "Sid": "GlueCatalog",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetTable", "glue:GetPartition",
        "glue:CreateTable", "glue:UpdateTable",
        "glue:BatchCreatePartition", "glue:BatchDeletePartition"
      ],
      "Resource": [
        "arn:aws:glue:us-east-1:ACCOUNT:catalog",
        "arn:aws:glue:us-east-1:ACCOUNT:database/surveillance*",
        "arn:aws:glue:us-east-1:ACCOUNT:table/surveillance*/*"
      ]
    },
    {
      "Sid": "KMSDecrypt",
      "Effect": "Allow",
      "Action": ["kms:Decrypt", "kms:GenerateDataKey"],
      "Resource": "arn:aws:kms:us-east-1:ACCOUNT:key/LAKE-KMS-KEY-ID"
    }
  ]
}
```

### `trade-surveillance-dashboard` (ECS Fargate task role — dashboard service)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "MSKReadAlerts",
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeGroup",
        "kafka-cluster:AlterGroup",
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:ReadData"
      ],
      "Resource": [
        "arn:aws:kafka:us-east-1:ACCOUNT:cluster/trade-surveillance/*",
        "arn:aws:kafka:us-east-1:ACCOUNT:topic/trade-surveillance/*/alerts",
        "arn:aws:kafka:us-east-1:ACCOUNT:group/trade-surveillance/dashboard-*"
      ]
    },
    {
      "Sid": "S3GoldRead",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::trade-surveillance-prod-lake-ACCOUNT",
        "arn:aws:s3:::trade-surveillance-prod-lake-ACCOUNT/gold/*"
      ]
    },
    {
      "Sid": "SecretsManagerDB",
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:us-east-1:ACCOUNT:secret:rds/surveillance-*"
    }
  ]
}
```

---

## 5. Estimated Monthly Costs

Estimates based on **us-east-1**, moderate production load (~50k orders/hour, ~3k alerts/day). All prices as of 2024 — check the [AWS Pricing Calculator](https://calculator.aws/) for current rates.

### Compute

| Service | Config | Est. cost/month |
|---|---|---|
| Amazon MSK | 3x `kafka.m5.xlarge` brokers | ~$520 |
| MSK storage | 3x 1 TB gp3 | ~$90 |
| Managed Flink | 4 KPUs average (2 jobs × 2 KPU each) | ~$180 |
| ECS Fargate — collector | 0.5 vCPU / 1 GB, 730 hrs | ~$15 |
| ECS Fargate — generator | 0.5 vCPU / 1 GB, 730 hrs | ~$15 |
| ECS Fargate — dashboard | 2x (1 vCPU / 2 GB), 730 hrs | ~$60 |
| **Compute subtotal** | | **~$880** |

### Storage and data

| Service | Config | Est. cost/month |
|---|---|---|
| Amazon S3 | ~500 GB (bronze 90-day retention + silver/gold) | ~$12 |
| RDS PostgreSQL | db.t3.medium Multi-AZ, 100 GB gp3 | ~$110 |
| CloudWatch Logs | ~10 GB/month ingestion | ~$5 |
| AWS Glue | ~50 DPU-hours/month | ~$22 |
| NAT Gateway | ~100 GB data processed | ~$14 |
| ALB | ~1 LCU average | ~$20 |
| ECR | 4 images × ~1 GB | ~$2 |
| **Storage/data subtotal** | | **~$185** |

### **Total estimated: ~$1,065/month**

#### Cost optimisations available

- **MSK Serverless** (~$0.0015/partition-hour) instead of provisioned brokers — saves ~$400/month at this workload scale, but has higher per-message cost above ~50 MB/s.
- **Compute Savings Plans** (1-year, no-upfront) reduce Fargate and Flink costs by ~23%.
- **S3 Intelligent-Tiering** on silver/ and gold/ automatically moves cold objects to cheaper storage tiers.
- **RDS `db.t3.medium` → reserved instance** (1-year) reduces RDS cost by ~40% (~$44/month saving).
- **MSK Tiered Storage** offloads cold log segments to S3 at $0.023/GB, allowing smaller broker disks.

---

## 6. Data Flow and Security

### Encryption in transit

All inter-service communication uses TLS:
- Fargate → MSK: TLS on port 9094, SASL/SCRAM credentials from Secrets Manager
- Flink → MSK: same
- Flink → S3: HTTPS (VPC endpoint)
- Glue → S3: HTTPS (VPC endpoint)
- Dashboard → RDS: SSL required (`sslmode=require` in connection string)
- ALB → Browser: TLS 1.2+ with ACM-managed certificate

### Encryption at rest

- MSK: AWS-managed KMS key (or CMK) for broker EBS volumes
- S3: SSE-KMS on all objects via bucket default encryption policy
- RDS: KMS CMK on storage volume, automated backup encryption
- Secrets Manager: KMS-encrypted by default

### Secrets management

No credentials in environment variables or container images. All secrets (MSK SASL password, RDS password, Finnhub API key) live in AWS Secrets Manager. Fargate tasks and Glue jobs retrieve them at runtime via their IAM task roles using `secretsmanager:GetSecretValue`.

```
Secrets layout:
  trade-surveillance/msk/sasl-creds         → { "username": "...", "password": "..." }
  trade-surveillance/rds/surveillance-user  → { "username": "...", "password": "..." }
  trade-surveillance/finnhub/api-key        → { "key": "..." }
```

### Compliance considerations

- **CloudTrail**: enable for the account to log all API calls — required for SOC 2 Type II and MiFID II audit trails.
- **AWS Config**: enable rules `rds-multi-az-support`, `s3-bucket-ssl-requests-only`, `msk-in-cluster-node-require-tls` to enforce infrastructure compliance continuously.
- **GuardDuty**: enable for threat detection on S3 data access and IAM anomalies.
- **RDS audit log**: enable `pgaudit` extension to log all DML on the alerts table — exportable to CloudWatch Logs for SIEM ingestion.

---

## 7. Deployment Order

Infrastructure dependencies must be created in this order to avoid circular waits:

```
1.  VPC, subnets, IGW, NAT Gateways, route tables
2.  Security groups (all, referencing each other by ID)
3.  VPC Endpoints (S3 gateway, optional Glue/CloudWatch interface)
4.  KMS keys (lake-key, rds-key)
5.  S3 bucket + lifecycle rules + bucket policy
6.  Secrets Manager secrets (placeholder values)
7.  Amazon MSK cluster  ← needs VPC, sg-msk, KMS key
8.  MSK SASL/SCRAM user (via MSK API, update Secrets Manager)
9.  Amazon RDS  ← needs VPC, sg-rds, KMS key
10. ECR repositories + push Docker images
11. ECS Cluster + Task Definitions + IAM task roles
12. Managed Flink applications (upload PyFlink wheels to S3 first)
13. Glue Data Catalog databases + Glue job definitions
14. ECS Services (collector, generator)  ← needs MSK ready
15. ECS Service (dashboard) + ALB + Cognito User Pool
16. CloudWatch alarms + SNS notifications
```

### Recommended tooling

- **Terraform** or **AWS CDK (TypeScript)** for infrastructure-as-code. The deployment order above maps cleanly to Terraform module dependencies.
- Use **Terraform workspaces** (`dev`, `staging`, `prod`) to replicate the environment at different cost tiers — a dev workspace can use MSK Serverless and a single-AZ RDS `db.t3.micro` to cut costs by ~80%.
