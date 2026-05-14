# ingest_gcp_postgresql_ecoapp

Ingest pipeline — **GCP · PostgreSQL · `your-db-hostname` · microservices (18 databases)**

Extracts data from a PostgreSQL instance hosting multiple microservices and writes to GCS Data Lake
as hive-partitioned Parquet. Orchestrated with **Prefect v3** and runs on **Kubernetes**

**Stack:** Python · SQLAlchemy (psycopg2) · pandas · gcsfs · GCP Secret Manager · Prefect v3 · Docker · Jenkins

---

## TL;DR

| | |
|---|---|
| **Source** | PostgreSQL `your-db-hostname` — 18 databases (1 microservice = 1 database = 1 flow) |
| **Sink** | GCS Data Lake · Parquet · hive-partitioned (`calendar_year/month_no/day_of_month`) |
| **Flows** | 18 Prefect v3 deployments · scheduled daily 01:40 ICT |
| **Pattern** | Per-flow TABLE_REGISTRY — each flow is a fully self-contained unit |
| **Auth** | GCP Secret Manager — no credentials hardcoded in the repo |

> **Context:** this repo is designed differently from [`ingest_gcp_mssql_popcorn`](../ingest_gcp_mssql_popcorn),
> which uses a centralized registry for a single DB instance with many domains.
> See [Design Decisions](#design-decisions) for the rationale of both approaches

---

## Architecture Overview

```
PostgreSQL (your-db-hostname)
    │
    │  18 databases — 1 per microservice
    │  subscription_api_prd, iam_api_prd, contract_api_prd, ...
    │
    │  SQLAlchemy + psycopg2
    ▼
[Extract Task]
    SQL query: date-range filter (backdate / startdate–enddate)
    Schema inspection via information_schema.columns (PostgreSQL)
    │
    ▼
[Transform Task]
    bit → 'true' / 'false'
    int/bigint → nullable Int64
    all columns → string (except partition cols)
    normalise nulls (NaT, None, NaN → np.nan)
    column names → lowercase
    │
    ▼
[Load Task]
    write Parquet → GCS (hive partitioning)
    │
    ▼
gs://{bucket}/gcp-storage-parquet/{gcs_application}/postgresql/{table}/
    └── calendar_year=YYYY/month_no=M/day_of_month=D/
        └── {uuid}-0.parquet
```

**Credentials flow:**
- Prefect Worker Node uses **ADC** to authenticate with GCP Secret Manager
- Secret Manager returns DB credentials + SA key → constructs `service_account.Credentials`
- `PREFECT_DEPLOY_MODE=1` → skips Secret Manager during the deploy step

---

## Design Decisions

### 1. Per-flow TABLE_REGISTRY — Why is this different from popcorn?

The source for this repo is a PostgreSQL instance hosting a **microservices architecture**
Each service owns its own database (database-per-service pattern) and evolves independently

```
your-db-hostname
├── subscription_api_prd     ← Subscription service
├── iam_iam_api_prd          ← IAM / Auth service
├── contract_api_prd         ← Contract management service
├── payment_payment_api_prd  ← Payment service
├── ivote_ivote_api_prd      ← iVote service
└── ... (18 databases)
```

This differs from popcorn, which is an ERP monolith where all domains share a single database
Using a centralized registry would cause problems:

- **Schema drift:** each service updates its schema on its own release cycle
  → a shared central registry would conflict frequently when multiple people edit it simultaneously
- **Blast radius:** a bug in a shared registry could affect all services simultaneously
- **Ownership boundary:** each service should own its own ingestion config

**Decision:** TABLE_REGISTRY lives inside each service's own flow file

```python
# flows/ingest_gcp_postgresql_subscription_api_prd.py
DB_NAME = 'subscription_api_prd'

TABLE_REGISTRY: dict[str, dict] = {
    'payment_receipts':         {'p_createdate': 'created_at', 'p_updatedate': 'updated_at'},
    'payment_receipts_details': {'p_createdate': 'created_at', 'p_updatedate': 'updated_at'},
    'payment_requests':         {'p_createdate': 'created_at', 'p_updatedate': 'updated_at'},
    ...
}
```

Each flow file is a self-contained unit — reading one file tells you everything about that service

**Known trade-offs:**
- The shared tasks/ and config/ still live in one repo → if a service truly requires isolation
  it should be split into a per-service repo, but the operational cost is too high for the ingest layer
- Adding a new service requires creating a new flow file plus entries in `deploy.py` and `prefect.yaml`
  (unlike popcorn where you only add a registry entry)

---

### 2. GCS path defaults to `db_name` — unlike popcorn

popcorn must explicitly specify `GCS_APPLICATION` because legacy paths don't align with DB names
ecoapp was designed from scratch, so the path can be derived directly from `db_name`:

```
# popcorn — GCS_APPLICATION set explicitly (legacy compatibility)
GCS_APPLICATION = 'gcp-ingest-mssql'   # ≠ DB name
GCS_APPLICATION = 'livingmart'          # ≠ DB name
GCS_APPLICATION = 'visitor'            # ≠ DB name

# ecoapp — GCS path mirrors db_name directly
DB_NAME = 'subscription_api_prd'
# → gcp-storage-parquet/subscription_api_prd/postgresql/{table}/
```

Except for flows migrated from older repos where the path already existed (`b2bsale`, `announcementv2` etc.)
which set `GCS_APPLICATION` explicitly with a `# maps to existing folder` comment

---

### 3. One flow in this repo does not ingest from a database — `contract_api_prd_enumdata`

Enum values for the contract service are defined in `.cs` files inside the application's Git repo
not in the database — so this flow clones the Git repo and parses the `.cs` files instead:

```python
# flows/ingest_gcp_postgresql_contract_api_prd_enumdata.py
ENUM_REGISTRY: dict[str, str] = {
    'EnumCustomerGroup.cs':    'EnumCustomerGroup',
    'EnumCustomerType.cs':     'EnumCustomerType',
    'EnumOrderStatus.cs':      'EnumOrderStatus',
    ...
}

@flow
def ingest_gcp_postgresql_contract_api_prd_enumdata(git_repo_url, git_branch='main'):
    # clone repo → parse C# enum → write Parquet → GCS
```

It uses the same flow pattern (Prefect, GCS output, hive partitioning) but with a different source
This demonstrates that the per-flow pattern accommodates edge cases without affecting other flows

---

### 4. Shared tasks/ but flow-level config — the chosen boundary

```
tasks/main_components_gcp.py  ← shared ETL core (ConnectorDB, Extract, Transform, Load)
tasks/tasks_gcp.py            ← shared Prefect @task wrappers
config/__init__.py            ← shared Secret Manager + env dispatcher
                                                 ↑
                              this is where sharing ends
                                                 ↓
flows/{service}.py            ← per-flow: DB_NAME, GCS_APPLICATION, TABLE_REGISTRY
```

Sharing tasks/ and config/ reduces duplication without sacrificing flow-level isolation
Each flow can still override behaviour through parameters in its TABLE_REGISTRY

---

### 5. Migration from hardcoded credentials → Secret Manager

This repo was migrated from a legacy repo that stored credentials directly in `config/*.json`
`config/__init__.py` has `CHANGE POINT` comments marking every migration change:

```python
# CHANGE POINT:
# The old repo read credentials from `config/*.json` and hardcoded config values.
# This migrated version reads these secrets from GCP Secret Manager instead:
# - db_host
# - db_password
# - service_account
```

This makes the repo safe to push to git with no credential leaks

---

## Project Structure

```
ingest_gcp_postgresql_ecoapp/
├── flows/
│   ├── __init__.py
│   ├── ingest_gcp_postgresql_announcement_announcement_api_prd.py
│   ├── ingest_gcp_postgresql_apollo_api_prd.py
│   ├── ingest_gcp_postgresql_b2b_auth_api_prd.py
│   ├── ingest_gcp_postgresql_contract_api_prd.py
│   ├── ingest_gcp_postgresql_contract_api_prd_enumdata.py   # source is Git repo, not a DB
│   ├── ingest_gcp_postgresql_facility_management_api_prd.py
│   ├── ingest_gcp_postgresql_iam_iam_api_prd.py
│   ├── ingest_gcp_postgresql_ivote_ivote_api_prd.py
│   ├── ingest_gcp_postgresql_lastmile_api_prd.py
│   ├── ingest_gcp_postgresql_learning_center_api_prd.py
│   ├── ingest_gcp_postgresql_notification_mkt_noti_api_prd.py
│   ├── ingest_gcp_postgresql_notification_notification_api_prd.py
│   ├── ingest_gcp_postgresql_payment_payment_api_prd.py
│   ├── ingest_gcp_postgresql_payment_payment_public_api_prd.py
│   ├── ingest_gcp_postgresql_pdpa_api_prd.py
│   ├── ingest_gcp_postgresql_role_api_prd.py
│   ├── ingest_gcp_postgresql_subscription_api_prd.py
│   └── ingest_gcp_postgresql_urb_mapping_mapping_api_prd.py
│
├── tasks/
│   ├── tasks_gcp.py            # Prefect @task wrappers: extract / transform / load
│   └── main_components_gcp.py  # ETL core: ConnectorDB, ExtractSourceData, TransformData, LoadSourceData
│
├── config/
│   ├── __init__.py             # env dispatcher + GCP Secret Manager helpers
│   ├── production.py           # config for branch main
│   └── development.py          # config for branch develop
│
├── config_flows/
│   └── __init__.py             # Prefect infra constants (work pool, cron, image)
│
├── scripts/
│   ├── run_local.sh
│   ├── command.sh
│   └── clean_repo.sh
│
├── run_local.py                # CLI: run a flow locally
├── deploy.py                   # Registers 18 deployments via Prefect v3 Python API
├── prefect.yaml                # Prefect deployment definitions (18 deployments)
├── Dockerfile                  # python:3.11-slim
└── requirements.txt
```

---

## Registered Flows

| Flow | Source DB | GCS Application | Notes |
|---|---|---|---|
| `ingest_gcp_postgresql_announcement_announcement_api_prd` | `announcement_announcement_api_prd` | `announcementv2` | legacy path |
| `ingest_gcp_postgresql_apollo_api_prd` | `apollo_api_prd` | `servicemaintenance` | |
| `ingest_gcp_postgresql_b2b_auth_api_prd` | `b2b_auth_api_prd` | `b2bauthapi` | legacy path |
| `ingest_gcp_postgresql_contract_api_prd` | `contract_api_prd` | `b2bsale` | legacy path |
| `ingest_gcp_postgresql_contract_api_prd_enumdata` | Git repo (`.cs` files) | `git` | non-DB source |
| `ingest_gcp_postgresql_facility_management_api_prd` | `facility_management_api_prd` | `facilityv2` | legacy path |
| `ingest_gcp_postgresql_iam_iam_api_prd` | `iam_iam_api_prd` | `pmsmanagement` | |
| `ingest_gcp_postgresql_ivote_ivote_api_prd` | `ivote_ivote_api_prd` | `ivote` | legacy path |
| `ingest_gcp_postgresql_lastmile_api_prd` | `lastmile_api_prd` | `lastmilev2` | legacy path |
| `ingest_gcp_postgresql_learning_center_api_prd` | `learning_center_api_prd` | `pmslearning-center` | |
| `ingest_gcp_postgresql_notification_mkt_noti_api_prd` | `notification_mkt_noti_api_prd` | `notification-mkt` | |
| `ingest_gcp_postgresql_notification_notification_api_prd` | `notification_notification_api_prd` | `notification` | |
| `ingest_gcp_postgresql_payment_payment_api_prd` | `payment_payment_api_prd` | `payment` | |
| `ingest_gcp_postgresql_payment_payment_public_api_prd` | `payment_payment_public_api_prd` | `payment_public` | |
| `ingest_gcp_postgresql_pdpa_api_prd` | `pdpa_api_prd` | `pdpa` | |
| `ingest_gcp_postgresql_role_api_prd` | `role_api_prd` | `residentpermission` | |
| `ingest_gcp_postgresql_subscription_api_prd` | `subscription_api_prd` | `subscription` | |
| `ingest_gcp_postgresql_urb_mapping_mapping_api_prd` | `urb_mapping_mapping_api_prd` | `urbmapping` | |

---

## GCS Output Path

```
gs://{bucket}/gcp-storage-parquet/{gcs_application}/postgresql/{table}/
    └── calendar_year=YYYY/month_no=M/day_of_month=D/
        └── {uuid}-0.parquet
```

**Example:**
```
gs://your-prd-datalake-bucket/gcp-storage-parquet/subscription_api_prd/postgresql/payment_receipts/
    calendar_year=2024/month_no=3/day_of_month=15/abc123-0.parquet
```

| Branch | GCS Bucket |
|---|---|
| `main` (production) | `your-prd-datalake-bucket` |
| `develop` | `your-nonprd-datalake-bucket` |

---

## TABLE_REGISTRY Format (per-flow)

```python
# flows/ingest_gcp_postgresql_{service}.py

DB_NAME         = 'subscription_api_prd'
GCS_APPLICATION = 'subscription_api_prd'   # or a legacy path if one already exists

TABLE_REGISTRY: dict[str, dict] = {

    # Standard case — incremental load using a date column
    'payment_receipts': {
        'p_createdate': 'created_at',
        'p_updatedate': 'updated_at',
    },

    # No update date — uses create date for both
    'companies_projects_logs': {
        'p_createdate': 'created_at',
        'p_updatedate': 'created_at',
    },

    # No date column → full load every run
    'enum_table': {
        'p_createdate': '',
        'p_updatedate': '',
    },
}
```

**Parameters accepted by `extract()`:**

| Parameter | Description |
|---|---|
| `p_createdate` | Column used to filter by creation date |
| `p_updatedate` | Column used to filter by last-modified date |
| `p_schema` | PostgreSQL schema (default: `public`) |
| `p_query_join` | SQL JOIN clause (for tables that require it) |
| `p_tablename_join` | Table to JOIN with |

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `CI_COMMIT_BRANCH` | `develop` | `main` → production config, otherwise → development |
| `IMAGE_TAG` | `ingest-gcp-postgresql-ecoapp:local` | Docker image tag |
| `PREFECT_DEPLOY_MODE` | *(unset)* | when set → skips Secret Manager during the deploy step |
| `PREFECT_WORK_POOL` | `kubernetes-pool` | Prefect work pool |
| `JOB_BACKDATE` | `-1` | number of days to look back (default) |

### GCP Secret Manager Keys

| Secret key | Used for |
|---|---|
| `your-secret-db-host-key` | DB host |
| `your-secret-db-password-key` | DB password |
| `your-secret-sa-key` | GCS credentials (service account JSON) |

---

## Local Development

```bash
pip install -r requirements.txt

export CI_COMMIT_BRANCH=develop

# Full run — processes all tables in the flow
python flows/ingest_gcp_postgresql_subscription_api_prd.py

# Repair mode — run a single table
python flows/ingest_gcp_postgresql_subscription_api_prd.py --table payment_receipts

# Override backdate
python flows/ingest_gcp_postgresql_subscription_api_prd.py --table payment_receipts --backdate -3

# Backfill a date range
python flows/ingest_gcp_postgresql_subscription_api_prd.py \
  --table payment_receipts \
  --startdate 2024-01-01 --enddate 2024-01-31
```

---

## Scheduling

```
cron : "40 18 * * *"   →  01:40 (ICT / Asia/Bangkok)
active: branch main only
```

All flows share the same schedule — all 18 deployments run concurrently

---

## Adding a New Service

1. Create a new flow file:
   ```bash
   cp flows/ingest_gcp_postgresql_subscription_api_prd.py \
      flows/ingest_gcp_postgresql_{service_name}.py
   ```
2. Update `DB_NAME`, `GCS_APPLICATION`, `FLOW_NAME`, the function name, and `TABLE_REGISTRY`
3. Add the import to `deploy.py` and append to `FLOW_OBJECTS`
4. Add a deployment block to `prefect.yaml`
5. Update this README (add a row to the Registered Flows table)
