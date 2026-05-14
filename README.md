# ingest_gcp_postgresql_ecoapp

Ingest repo สำหรับ **GCP · PostgreSQL · your-db-hostname**

ดึงข้อมูลจาก PostgreSQL instance `your-db-hostname` บน GCP แล้ว load ขึ้น GCS Data Lake ในรูปแบบ Parquet แบ่ง partition ตาม `calendar_year / month_no / day_of_month`

---

## สารบัญ

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Naming Convention](#naming-convention)
- [Flows ใน repo นี้](#flows-ใน-repo-นี้)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
  - [GCP Secret Manager](#gcp-secret-manager)
  - [Auth Mode บน Worker Node](#auth-mode-บน-worker-node)
- [Local Development](#local-development)
  - [Setup](#setup)
  - [Run Locally](#run-locally)
- [Prefect Deployment](#prefect-deployment)
  - [Deploy ผ่าน prefect.yaml (แนะนำ)](#deploy-ผ่าน-prefectyaml-แนะนำ)
  - [Deploy ผ่าน deploy.py](#deploy-ผ่าน-deploypy)
  - [Trigger manual run ผ่าน Prefect CLI](#trigger-manual-run-ผ่าน-prefect-cli)
- [CI/CD Pipeline (Jenkins)](#cicd-pipeline-jenkins)
- [GCS Output Path Structure](#gcs-output-path-structure)
- [ETL Flow Overview](#etl-flow-overview)
- [เพิ่ม db\_name ใหม่](#เพิ่ม-db_name-ใหม่)
- [เพิ่ม table ใหม่](#เพิ่ม-table-ใหม่)
- [Known Issues / TODO](#known-issues--todo)

---

## Architecture Overview

```
PostgreSQL (your-db-hostname)
    │
    │  SQLAlchemy + psycopg2
    ▼
[Extract Task]  ── date-range filter (backdate / startdate–enddate)
    │
    ▼
[Transform Task]  ── cast to string, handle bit/int columns, lowercase columns
    │
    ▼
[Load Task]  ── write Parquet → GCS Data Lake (hive partitioning)
    │
    ▼
gs://your-prd-datalake-bucket/gcp-storage-parquet/
    └── {application}/{db_type}/{table}/
        └── calendar_year=YYYY/month_no=M/day_of_month=D/
            └── {uuid}-0.parquet
```

**Credentials flow:**
- Prefect Worker Node ใช้ **ADC (Application Default Credentials)** เพื่อเชื่อมต่อ GCP Secret Manager
- Secret Manager จ่าย SA credentials → สร้าง `google.oauth2.service_account.Credentials`
- SA credentials ใช้ authenticate กับ GCS (via `gcsfs`) และ PostgreSQL (password)

---

## Project Structure

```
ingest_gcp_postgresql_prd_rds_apse1_ecoapp/
│
├── flows/                          # 1 ไฟล์ต่อ db_name
│   ├── __init__.py                 # export flow objects
│   └── ingest_gcp_postgresql_subscription_api_prd.py   # flow หลัก + TABLE_REGISTRY
│
├── tasks/                          # reusable Prefect tasks
│   ├── tasks_gcp.py                # @task wrappers: extract, transform, load
│   └── main_components_gcp.py      # core ETL logic: ConnectorDB, ExtractSourceData,
│                                   #   TransformData, LoadSourceData
│
├── config/                         # runtime config (db credentials, GCP settings)
│   ├── __init__.py                 # env-based dispatcher + GCP Secret Manager helpers
│   ├── production.py               # production values (branch: main)
│   └── development.py              # development values (branch: develop / default)
│
├── config_flows/                   # Prefect infra config (work pool, schedule, image)
│   └── __init__.py
│
├── scripts/
│   ├── run_local.sh                # wrapper: python run_local.py "$@"
│   ├── command.sh                  # wrapper: python deploy.py
│   └── clean_repo.sh               # ลบ __pycache__, .pyc, .DS_Store
│
├── run_local.py                    # dev/repair entry point (argparse CLI)
├── deploy.py                       # register flows as Prefect deployments
├── prefect.yaml                    # declarative deployment config (prefect deploy --all)
├── Dockerfile                      # python:3.11-slim, installs requirements.txt
├── requirements.txt                # pinned dependencies
├── Jenkinsfile                     # CI/CD: build → push → deploy to Prefect v3
└── pom.xml                         # Maven descriptor สำหรับ SonarQube code analysis
```

---

## Naming Convention

| Layer | Format | ตัวอย่าง |
|---|---|---|
| repo | `ingest_{platform}_{db_type}_{db_instance}` | `ingest_gcp_postgresql_ecoapp` |
| flow file | `ingest_{platform}_{db_type}_{db_name}.py` | `ingest_gcp_postgresql_subscription_api_prd.py` |
| flow name | เหมือน file ไม่มี `.py` | `ingest_gcp_postgresql_subscription_api_prd` |
| deployment | `{flow_name}/{flow_name}` | `ingest_gcp_postgresql_subscription_api_prd/ingest_gcp_postgresql_subscription_api_prd` |
| GCS path | `{bucket}/gcp-storage-parquet/{application}/{db_type}/{table}/...` | `.../subscription/postgresql/users/...` |

> **หมายเหตุ:** GCS path ปัจจุบันใช้ `GCS_APPLICATION` (`subscription`) ไม่ใช่ `DB_NAME` (`subscription_api_prd`) เพื่อ backward compatibility กับ folder structure เดิม — จะเปลี่ยนเป็น `DB_NAME` หลัง full migration ครบ

---

## Flows ใน repo นี้

| Flow | DB Name | Tables |
|---|---|---|
| `ingest_gcp_postgresql_subscription_api_prd` | `subscription_api_prd` | `payment_receipts`, `payment_receipts_details`, `payment_requests`, `payment_requests_details`, `subscriptions_log` |

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11 |
| prefect | ≥ 3.1, < 4 |
| Docker | (สำหรับ build/deploy) |
| GCP project access | `your-gcp-project-id` |
| Prefect Server | `http://<PREFECT_SERVER_IP>:4200` |

---

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `CI_COMMIT_BRANCH` | No | `develop` | กำหนด environment (`main` = production) |
| `IMAGE_TAG` | No | `ingest-gcp-postgresql-your-db-hostname:local` | Docker image tag |
| `SECRET_PROJECT_ID` | No | `your-gcp-project-id` | GCP project ที่เก็บ secrets |
| `SECRET_DB_HOST` | No | `gcp-your-db-hostname-db-host` | Secret name สำหรับ DB host |
| `SECRET_DB_PASSWORD` | No | `gcp-your-db-hostname-db-password` | Secret name สำหรับ DB password |
| `SECRET_SERVICE_ACCOUNT` | No | `gcp-dwh-service-account` | Secret name สำหรับ SA JSON |
| `PREFECT_API_URL` | Yes (deploy) | — | Prefect Server API endpoint |
| `PREFECT_WORK_POOL` | No | `kubernetes-pool` | Prefect work pool name |
| `PREFECT_WORK_QUEUE` | No | `default` | Prefect work queue name |
| `PREFECT_DEPLOY_MODE` | No | — | ถ้า set = `1` จะ skip การดึง secrets (ใช้ตอน `prefect deploy --all`) |
| `JOB_BACKDATE` | No | `-1` | Backdate offset เริ่มต้น (จำนวนวันย้อนหลัง) |
| `JOB_STARTDATE` | No | `""` | Start date สำหรับ backfill (YYYY-MM-DD) |
| `JOB_ENDDATE` | No | `""` | End date สำหรับ backfill (YYYY-MM-DD) |

### GCP Secret Manager

Secrets ที่ต้องสร้างไว้ใน project `your-gcp-project-id`:

| Secret Name | Content | ใช้โดย |
|---|---|---|
| `gcp-your-db-hostname-db-host` | hostname ของ PostgreSQL | `config/__init__.py` → `DB_HOST` |
| `gcp-your-db-hostname-db-password` | password ของ DB user | `config/__init__.py` → `DB_PASSWORD` |
| `gcp-dwh-service-account` | Service Account JSON (full content) | `config/__init__.py` → `GCSFS` credentials |

> Secret Manager client ใช้ **ADC** ของ worker node → ไม่ต้อง mount key file ใน container และไม่ต้องตั้ง `GOOGLE_APPLICATION_CREDENTIALS` ใน deployment

### Auth Mode บน Worker Node

เลือกวิธีใดวิธีหนึ่ง:

| Mode | วิธีตั้งค่า |
|---|---|
| **Workload Identity (GKE)** | ไม่ต้องทำอะไรเพิ่ม — ADC resolve จาก metadata server อัตโนมัติ |
| **Docker pool บน VM** | ตั้ง `GOOGLE_APPLICATION_CREDENTIALS` บน host แล้ว passthrough ผ่าน env ของ work pool เข้า container |

---

## Local Development

### Setup

```bash
# 1. สร้าง virtual environment
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# 2. ติดตั้ง dependencies
pip install -r requirements.txt

# 3. ตั้งค่า environment
export CI_COMMIT_BRANCH=develop
export SECRET_PROJECT_ID=your-gcp-project-id
# หรือสร้างไฟล์ .env แล้ว source ออก
```

> **หมายเหตุ:** รัน local ต้องมี ADC ที่มีสิทธิ์เข้า Secret Manager และ GCS
> ```bash
> gcloud auth application-default login
> ```

### Run Locally

```bash
# Full run (ทุก table ทุก db_name)
python run_local.py

# Full run เฉพาะ db_name
python run_local.py --flow ingest_gcp_postgresql_subscription_api_prd

# Repair single table
python run_local.py --flow ingest_gcp_postgresql_subscription_api_prd --table payment_receipts

# Repair พร้อมระบุ backdate
python run_local.py --flow ingest_gcp_postgresql_subscription_api_prd --table payment_receipts --backdate -3

# Backfill ช่วงวันที่
python run_local.py --flow ingest_gcp_postgresql_subscription_api_prd \
    --startdate 2024-01-01 --enddate 2024-01-31
```

หรือใช้ shell script:

```bash
bash scripts/run_local.sh --flow ingest_gcp_postgresql_subscription_api_prd --table payment_receipts
```

---

## Prefect Deployment

### Deploy ผ่าน prefect.yaml (แนะนำ)

```bash
export PREFECT_API_URL=http://<PREFECT_SERVER_IP>:4200/api
export PREFECT_WORK_POOL=docker-pool
export PREFECT_WORK_QUEUE=default
export IMAGE_TAG=<YOUR_REGISTRY>/<YOUR_PROJECT>/prefect_v3/ingest/ingest_gcp_postgresql_ecoapp:main-<commit>
export SECRET_PROJECT_ID=your-gcp-project-id
export CI_COMMIT_BRANCH=main
export PREFECT_DEPLOY_MODE=1

prefect deploy --all
```

> `PREFECT_DEPLOY_MODE=1` จำเป็นต้องตั้งเพื่อ skip การดึง DB credentials ขณะ register deployment

### Deploy ผ่าน deploy.py

```bash
# ใช้ในกรณีต้องการควบคุม deploy logic มากกว่า prefect.yaml
export PREFECT_API_URL=http://<PREFECT_SERVER_IP>:4200/api
export PREFECT_DEPLOY_MODE=1
python deploy.py
```

### Trigger manual run ผ่าน Prefect CLI

```bash
# Run ทุก table
prefect deployment run \
  'ingest_gcp_postgresql_subscription_api_prd/ingest_gcp_postgresql_subscription_api_prd'

# Repair single table
prefect deployment run \
  'ingest_gcp_postgresql_subscription_api_prd/ingest_gcp_postgresql_subscription_api_prd' \
  -p TABLE_NAME=payment_receipts

# Repair พร้อมระบุ backdate (ย้อนหลัง 3 วัน)
prefect deployment run \
  'ingest_gcp_postgresql_subscription_api_prd/ingest_gcp_postgresql_subscription_api_prd' \
  -p TABLE_NAME=payment_receipts -p JOB_BACKDATE_DEPEN=-3

# Backfill ช่วงวันที่
prefect deployment run \
  'ingest_gcp_postgresql_subscription_api_prd/ingest_gcp_postgresql_subscription_api_prd' \
  -p JOB_STARTDATE_DEPEN=2024-01-01 -p JOB_ENDDATE_DEPEN=2024-01-31
```

**Flow Parameters:**

| Parameter | Default | Description |
|---|---|---|
| `TABLE_NAME` | `""` | ว่าง = รันทุก table, ระบุชื่อ = repair mode |
| `JOB_BACKDATE_DEPEN` | `"-1"` | Backdate offset (negative int); `"-1"` = ใช้ค่า default จาก config |
| `JOB_STARTDATE_DEPEN` | `""` | Start date YYYY-MM-DD สำหรับ backfill |
| `JOB_ENDDATE_DEPEN` | `""` | End date YYYY-MM-DD สำหรับ backfill |

**Schedule:** `40 18 * * *` UTC = **01:40 น. (SGT)** เฉพาะ branch `main`

---

## CI/CD Pipeline (Jenkins)

```
git push → Jenkins
    │
    ├── Who am I / Check workspace
    ├── Code Analysis (SonarQube)   ← branches: develop, main, tags
    ├── Validate branch             ← skip ถ้าไม่ใช่ develop/main/production/tag
    ├── Build Docker image          ← branches: develop, main
    ├── Push to Artifact Registry   ← asia-southeast1-docker.pkg.dev/your-gcp-project-id/...
    ├── Deploy Prefect v3           ← รัน `prefect deploy --all` ใน container
    └── Remove local image
```

**Image naming:** `{IMAGE_NAME}:{BRANCH}-{SHORT_COMMIT}`
ตัวอย่าง: `...ingest_gcp_postgresql_prd_rds_apse1_ecoapp:main-a1b2c3d4`

**Prefect deploy step ใน Jenkins** inject env vars เข้า container ดังนี้:
- `GOOGLE_APPLICATION_CREDENTIALS` — GCR service account key (mount จาก Jenkins credentials)
- `PREFECT_API_URL`, `PREFECT_WORK_POOL`, `PREFECT_WORK_QUEUE`
- `CI_COMMIT_BRANCH`, `IMAGE_TAG`, `SECRET_PROJECT_ID`
- `PREFECT_DEPLOY_MODE=1` — ป้องกัน flow init ไม่ให้ดึง secrets ขณะ deploy

---

## GCS Output Path Structure

```
gs://your-prd-datalake-bucket/
└── gcp-storage-parquet/
    └── authentication/               ← GCS_APPLICATION (ชั่วคราว, จะเปลี่ยนเป็น auth_api_prd)
        └── postgresql/               ← DB_TYPE
            └── {table}/              ← ชื่อ table เช่น users, otps, sms, archive
                └── calendar_year=2024/
                    └── month_no=1/
                        └── day_of_month=15/
                            └── {uuid}-0.parquet
```

> ไฟล์ Parquet ใช้ **hive partitioning** และมี `existing_data_behavior='overwrite_or_ignore'` ซึ่งหมายความว่าถ้า partition เดิมมีอยู่แล้วจะไม่ overwrite (idempotent เฉพาะ partition ที่ไม่ซ้ำ)

---

## ETL Flow Overview

### Extract

1. ต่อ PostgreSQL ผ่าน SQLAlchemy + psycopg2
2. ดึง column metadata จาก `INFORMATION_SCHEMA.COLUMNS` เพื่อตรวจ datatype
3. สร้าง SQL query พร้อม date filter:
   - `p_startdate` / `p_enddate` → `BETWEEN` query
   - `p_backdate` → relative offset จากวันนี้ (timezone +7)
   - ถ้าไม่มี date column → ใช้ `NOW()` เป็น synthetic timestamp
4. รองรับ date column types: `timestamp`, `timestamp with/without time zone`, `integer` (unix epoch)

### Transform

1. แปลง `bit` columns → `'true'` / `'false'` string
2. แปลง `int` columns → `Int64` (nullable integer)
3. Lowercase column names ทั้งหมด
4. แปลง non-partition columns → `string`
5. Replace `NaT`, `None`, `NaN`, `nan`, `<NA>` → `np.nan`

### Load

1. แปลง DataFrame → PyArrow Table
2. Write ไป GCS ด้วย `pyarrow.dataset.write_dataset`
3. Format: Parquet, Hive partitioning (`calendar_year/month_no/day_of_month`)
4. Basename: `{uuid}-{i}.parquet` (ป้องกัน file collision)

---

## เพิ่ม db\_name ใหม่

1. **สร้าง flow file** — copy `flows/ingest_gcp_postgresql_subscription_api_prd.py` แล้วแก้:
   ```python
   DB_NAME = 'subscription_api_prd'
   GCS_APPLICATION = 'subscription'   # folder ใน GCS
   TABLE_REGISTRY = {
       'payment_receipts': {'p_createdate': 'created_at', 'p_updatedate': 'updated_at'},
       ...
   }
   ```

2. **เพิ่ม import ใน `flows/__init__.py`**
   ```python
   from flows.ingest_gcp_postgresql_chat_api_prd import ingest_gcp_postgresql_chat_api_prd
   ```

3. **เพิ่มใน `run_local.py`**
   ```python
   from flows.ingest_gcp_postgresql_chat_api_prd import ingest_gcp_postgresql_chat_api_prd
   FLOW_MAP['ingest_gcp_postgresql_chat_api_prd'] = ingest_gcp_postgresql_chat_api_prd
   ```

4. **เพิ่มใน `deploy.py`**
   ```python
   from flows.ingest_gcp_postgresql_chat_api_prd import ingest_gcp_postgresql_chat_api_prd
   FLOW_OBJECTS.append(ingest_gcp_postgresql_chat_api_prd)
   ```

5. **เพิ่ม deployment block ใน `prefect.yaml`** (copy block ที่มี แล้วแก้ `name` และ `entrypoint`)

---

## เพิ่ม table ใหม่

เปิด flow file ที่ต้องการ → เพิ่ม entry ใน `TABLE_REGISTRY` — **ไม่ต้องแตะโค้ดอื่น**

```python
TABLE_REGISTRY: dict[str, dict] = {
    ...
    'new_table': {
        'p_createdate': 'created_at',
        'p_updatedate': 'updated_at',
    },
}
```

**กรณีพิเศษ:**
- ถ้า table ไม่มี date column ที่เหมาะสม ให้ตั้ง `p_createdate=''` และ `p_updatedate=''` → Extract จะใช้ `NOW()` เป็น partition key
- ถ้า table ต้องการ JOIN ให้เพิ่ม `p_query_join` และ `p_tablename_join`

---

## Known Issues / TODO

| # | ระดับ | รายละเอียด | วิธีแก้ |
|---|---|---|---|
| 1 | 🔴 Bug | `run_local.py` และ `flows/__init__.py` import `ingest_gcp_postgresql_chat_api_prd` ที่ยังไม่มีไฟล์ → ImportError ทันที | สร้างไฟล์ flow ให้ครบ หรือ comment import ออกจนกว่าจะพร้อม |
| 2 | 🔴 Config | `config/development.py` ชี้ resource เดียวกับ `production.py` — ไม่มีการแยก dev environment | แยก GCP project / bucket / secret names สำหรับ development |
| 3 | 🟡 Hygiene | `__pycache__/` และ `.DS_Store` ถูก commit เข้า repo แม้มี `.gitignore` | `git rm -r --cached __pycache__/ && git rm --cached **/.DS_Store` แล้ว commit |
| 4 | 🟡 Code | `datetime_start` ใน `tasks_gcp.py` เป็น unused variable | ลบออก |
| 5 | 🟡 Code | `transform()` signature `p_dataframe=pd.DataFrame()` — mutable default argument และ type ไม่ตรงกับ runtime | เปลี่ยนเป็น `p_dataframe=None` พร้อม type hint `tuple` |
| 6 | 🟡 Code | `from config import *` ใน `main_components_gcp.py` | เปลี่ยนเป็น explicit imports |
| 7 | 🟡 Config | Work pool default ใน `config_flows` (`kubernetes-pool`) ไม่ตรงกับที่ Jenkins ใช้ (`docker-pool`) | sync ค่า default หรือ document ให้ชัดว่าต้อง set env var เสมอ |
| 8 | 🔵 TODO | GCS path ใช้ `GCS_APPLICATION` แทน `DB_NAME` ชั่วคราว | เปลี่ยน `p_application=GCS_APPLICATION` → `p_application=DB_NAME` หลัง full migration |