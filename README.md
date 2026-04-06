# Supplychain360-unified-data-platform
End-to-end data engineering platform for SupplyChain360 to centralize fragmented logistics, inventory, and sales data. Built to optimize inventory planning, monitor supplier performance, and reduce operational stockouts.

> AWS · GCP BigQuery · dbt · Airflow · Airbyte · Terraform

---

## Overview

Supplychain360 ingests supply chain data from multiple sources, stages it in AWS S3, loads it into BigQuery, and transforms it using dbt to answer four core analytical questions:

- **Product stockout trends** — frequency, duration, and pattern of stockout events by product, category, and warehouse
- **Supplier delivery performance** — on-time rate, average delay, and lead time variance per supplier
- **Warehouse efficiency** — estimated inbound and outbound throughput derived from daily inventory snapshots
- **Regional sales demand** — gross and net revenue, units sold, and transaction volume by region, store, and period

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Local Machine                            │
│                                                                 │
│   ┌──────────────┐        ┌──────────────┐                     │
│   │   Airbyte    │        │   Airflow    │                     │
│   │ localhost:   │        │ localhost:   │                     │
│   │    8000      │        │    8080      │                     │
│   └──────┬───────┘        └──────┬───────┘                     │
│          │                       │                             │
│   ┌──────┴───────────────────────┤                             │
│   │  Sources                     │                             │
│   │  Google Sheets · Postgres · S3                             │
│   └──────────────────────────────┘                             │
└────────────────────────┬────────────────────────────────────────┘
                         │
          ┌──────────────▼──────────────┐
          │         AWS (Terraform)     │
          │                             │
          │   ┌─────────────────────┐   │
          │   │   S3 Raw Bucket     │   │
          │   │ Versioned · SSE ·   │   │
          │   │ Lifecycle-managed   │   │
          │   └──────────┬──────────┘   │
          │              │              │
          │   ┌──────────┴──────────┐   │
          │   │    IAM Pipeline     │   │
          │   │    User + SSM       │   │
          │   └─────────────────────┘   │
          └──────────────┬──────────────┘
                         │
          ┌──────────────▼──────────────┐
          │       GCP BigQuery          │
          │                             │
          │  01_ staging tables         │
          │         ↓                   │
          │  int_ intermediate models   │
          │         ↓                   │
          │  mart_ incremental marts    │
          └─────────────────────────────┘
```

### Data flow

```
Sources → Airbyte → S3 Raw Bucket → BigQuery (01_ tables) → dbt → Marts
                         ↑
                    Airflow DAGs orchestrate the full pipeline
```

---

## Repository Structure

```
supplychain360/
├── .github/
│   └── workflows/
│       └── pipeline.yml              # CI/CD pipeline
├── infra/
│   ├── bootstrap/                    # One-time state bucket bootstrap
│   ├── providers.tf                  # AWS + Google provider declarations
│   ├── backend.tf                    # Remote state (S3 + native lockfile)
│   ├── variables.tf                  # All variable declarations
│   ├── terraform.tfvars.example      # Committed placeholder values
│   ├── s3.tf                         # Raw S3 bucket
│   ├── iam.tf                        # Pipeline IAM user + SSM parameters
│   └── outputs.tf
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/
│       │   └── _sources.yml          # Source definitions (01_ tables)
│       ├── intermediate/             # Business logic and joins
│       └── marts/                    # Analytical outputs (incremental)
├── airflow/
│   ├── dags/
│   │   ├── dbt_layer_dag_factory.py  # Generates one DAG per dbt layer
│   │   └── config/
│   │       └── dbt_layers.yml        # Layer schedule configuration
│   └── tests/
│       └── test_dag_integrity.py
├── scripts/
│   └── convert_json_to_jsonl.py      # Converts source JSON arrays to JSONL
├── tests/
│   └── unit/                         # Unit tests for ingestion scripts
├── requirements.txt
└── .gitignore
```

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Terraform | >= 1.10.0 | Infrastructure provisioning |
| AWS CLI | Latest | Profile-based authentication |
| Docker Desktop | Latest | Local Airbyte and Airflow |
| Python | 3.11+ | dbt, Airflow, scripts |
| dbt-bigquery | 1.11.x | Transformation layer |
| gcloud CLI | Latest | GCP authentication |
| GCP account | — | BigQuery dataset and service account |
| AWS account | — | S3 bucket and IAM provisioning |

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/your-org/supplychain360.git
cd supplychain360
```

### 2. Configure AWS profile

```bash
aws configure --profile terraform
```

Verify:

```bash
aws sts get-caller-identity --profile terraform
```

### 3. Create the Terraform state bucket manually

Create the S3 bucket in the AWS console before running `terraform init`. Enable versioning on it. The bucket name goes into `backend.tf`.

```bash
# Verify your state bucket exists and the profile can reach it
aws s3 ls s3://your-state-bucket-name --profile terraform
```

### 4. Provision infrastructure

```bash
cd infra/
terraform init
terraform validate
terraform fmt
terraform plan
terraform apply
```

Retrieve the pipeline credentials from SSM after apply:

```bash
# Access key ID
aws ssm get-parameter \
  --name "/your-project/raw/aws_access_key_id" \
  --with-decryption \
  --profile terraform \
  --query Parameter.Value \
  --output text

# Secret access key
aws ssm get-parameter \
  --name "/your-project/raw/aws_secret_access_key" \
  --with-decryption \
  --profile terraform \
  --query Parameter.Value \
  --output text
```

### 5. Configure GCP

Create a GCP project and enable required APIs:

```bash
gcloud config set project your-gcp-project-id

gcloud services enable \
  bigquery.googleapis.com \
  iam.googleapis.com \
  cloudresourcemanager.googleapis.com \
  iamcredentials.googleapis.com
```

Create a Terraform operator service account in the GCP console with `BigQuery Admin` and `Project IAM Admin` roles. Download the JSON key to `~/.gcp/terraform-operator.json`.

### 6. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 7. Configure dbt

```bash
cp dbt/profiles.yml.example dbt/profiles.yml
# Edit profiles.yml with your GCP project ID and dataset

cd dbt && dbt deps
```

### 8. Start local services

```bash
# Airbyte — available at localhost:8000
cd /opt/airbyte && docker-compose up -d

# Airflow — available at localhost:8080
airflow db init
airflow users create --username admin --password changeme \
  --firstname Admin --lastname User --role Admin --email admin@example.com
airflow webserver -D
airflow scheduler -D
```

### 9. Configure Airbyte connectors

In the Airbyte UI add your sources (Google Sheets, Postgres, S3) and point the S3 destination at your Terraform-provisioned raw bucket using the pipeline credentials retrieved from SSM.

> **Note:** Airbyte's S3 connector requires JSONL format. If your source files are JSON arrays use the conversion script:
> ```bash
> python scripts/convert_json_to_jsonl.py
> ```

---

## dbt Models

Source tables in BigQuery use the `01_` prefix. Models read directly from these via `{{ source() }}` — there is no separate staging layer in dbt.

### Intermediate models

| Model | Description |
|-------|-------------|
| `int_inventory_stock_status` | Stockout flags, reorder threshold breach, and daily stock delta derived from snapshot lag |
| `int_supplier_delivery_performance` | On-time flag, delay days, and actual lead time joined to supplier via product |
| `int_warehouse_movements` | Estimated inbound and outbound volumes derived from inventory snapshot deltas |
| `int_sales_enriched` | Sales joined to products and stores with net sale amount, region, and category |

### Mart models — incremental materialisation

| Model | Analytical question |
|-------|---------------------|
| `mart_stockout_trends` | Product stockout trends by product, warehouse, category, and date |
| `mart_supplier_performance` | Supplier on-time %, average delay days, and lead time per carrier |
| `mart_warehouse_efficiency` | Throughput, average stock held, and shipments dispatched per warehouse |
| `mart_regional_sales_demand` | Revenue and units sold by region, store, product, and month |

### Dimension tables

| Model | Description |
|-------|-------------|
| `dim_products` | Product attributes — name, brand, category, unit price, supplier link |
| `dim_suppliers` | Supplier name, country, and category |
| `dim_stores` | Store name, city, state, and region |
| `dim_warehouses` | Warehouse city, state, and derived location label |

### Running dbt

```bash
# Run all models
dbt run

# Run a specific layer
dbt run --select intermediate
dbt run --select marts

# Force full refresh of incremental models
dbt run --select marts --full-refresh

# Run tests
dbt test

# Test sources only
dbt test --select source:supplychain360_db

# Generate source YAML (staging layer)
dbt --quiet run-operation generate_source --args '{
  "schema_name": "supplychain360_db",
  "generate_columns": true,
  "table_pattern": "raw%",
  "include_descriptions": true
}' | sed 's/\x1b\[[0-9;]*m//g' > models/staging/_sources.yml
```

---

## Airflow Orchestration

DAGs are generated from `airflow/config/dbt_layers.yml` using a factory pattern. One DAG is created per dbt layer on an independent schedule.

| DAG | Schedule | Task sequence |
|-----|----------|---------------|
| `dbt_staging_layer` | 6:00 AM daily | `deps → source_freshness → compile → run → test` |
| `dbt_intermediate_layer` | 7:00 AM daily | `deps → compile → run → test` |
| `dbt_marts_layer` | 8:00 AM daily | `deps → compile → run → test` |

---

## Infrastructure (Terraform)

All AWS resources are managed as flat Terraform files — no modules. The state bucket is created manually before `terraform init`.

| File | Provisions |
|------|------------|
| `backend.tf` | Remote state in S3 with native lockfile (`use_lockfile = true`) |
| `s3.tf` | Raw S3 bucket — versioning, SSE-S3, public access block, lifecycle rules |
| `iam.tf` | Pipeline IAM user, least-privilege S3 policy, access keys stored in SSM |
| `providers.tf` | AWS and Google provider declarations |

### S3 lifecycle policy

| Days | Storage class |
|------|--------------|
| 0 – 30 | STANDARD |
| 30 – 90 | STANDARD_IA |
| 90 – 180 | GLACIER |
| 180 – 365 | DEEP_ARCHIVE |
| 365+ | Expired |

---

## CI/CD Pipeline

GitHub Actions runs branch-specific jobs. Each feature branch triggers only the tests relevant to its domain.

| Branch | Jobs triggered |
|--------|----------------|
| `feature-infra/*` | `terraform fmt -check`, `terraform validate` |
| `feature-modeling/*` | `dbt deps`, `dbt compile`, `dbt test --select source:` |
| `feature-orchestration/*` | Airflow DAG integrity tests via pytest |
| `feature-ingestion/*` | Unit tests for conversion scripts |
| `dev` | Full dbt compile + run + test + pytest suite |
| `main` | dbt prod run + test + `terraform apply` |

### Required GitHub secrets

| Secret | Description |
|--------|-------------|
| `GCP_PROJECT_ID` | GCP project identifier |
| `GCP_SERVICE_ACCOUNT_KEY` | Base64 encoded service account JSON |
| `AWS_ACCESS_KEY_ID` | Pipeline IAM user access key |
| `AWS_SECRET_ACCESS_KEY` | Pipeline IAM user secret key |
| `DBT_BIGQUERY_DATASET` | Target BigQuery dataset name |
| `TF_STATE_BUCKET` | Name of the manually created state bucket |

---

## Environment Variables

> Never commit secrets. All sensitive values are retrieved from AWS SSM at runtime or injected via GitHub secrets.

```bash
# AWS — retrieved from SSM after terraform apply
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY

# GCP
GCP_PROJECT_ID
GOOGLE_APPLICATION_CREDENTIALS    # path to service account JSON (local only)

# dbt
DBT_PROFILES_DIR                  # ./dbt
DBT_BIGQUERY_DATASET
```

---

## Design Decisions and Trade-offs

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Airbyte / Airflow hosting | Local Docker | No EC2 cost or ops overhead during development |
| IAM credentials | User + access keys | Local tools cannot assume IAM roles — long-lived keys stored in SSM |
| State locking | S3 native lockfile | Eliminates DynamoDB dependency; requires Terraform >= 1.10 |
| Terraform structure | Flat files | Single environment — modules add overhead with no reuse benefit yet |
| Mart materialisation | Incremental + merge | Avoids full table scan on each run; date-partitioned for cost control |
| Warehouse movements | Snapshot delta | No movement transaction log — inbound/outbound derived from `lag()` |
| Source file format | JSONL | Airbyte S3 connector does not support JSON arrays — converted via script |

---

## Contributing

Branch naming follows the feature domain convention:

```
feature-infra/your-change
feature-modeling/your-change
feature-orchestration/your-change
feature-ingestion/your-change
```

All feature branches must target `dev` via pull request. Only `dev` merges to `main`. Direct pushes to `main` are disabled.

---

## Stack

| Layer | Tool |
|-------|------|
| Ingestion | Airbyte |
| Orchestration | Apache Airflow |
| Raw storage | AWS S3 |
| Identity | AWS IAM + SSM |
| Warehouse | Google BigQuery |
| Transformation | dbt (dbt-bigquery) |
| Infrastructure | Terraform |
| CI/CD | GitHub Actions |
