from __future__ import annotations

import os

APP_ENV    = os.getenv('CI_COMMIT_BRANCH', 'develop')
IMAGE_TAG  = os.getenv('IMAGE_TAG', 'ingest-gcp-postgresql-ecoapp:local')
PREFECT_IMAGE          = IMAGE_TAG
PREFECT_FLOW_DIRECTORY = '/app/flows/'

# ── Repo identity ─────────────────────────────────────────────────────────────
# repo     : ingest_{platform}_{db_type}_{db_instance}
# flow     : ingest_{platform}_{db_type}_{db_name}
# GCS path : {bucket}/gcp-storage-parquet/{db_name}/{db_type}/{table}/...
PLATFORM    = 'gcp'
DB_TYPE     = 'postgresql'
DB_INSTANCE = 'prd_rds_apse1_ecoapp'   # maps to DB_HOST_NAME in config/production.py

# ── Job date params ───────────────────────────────────────────────────────────
JOB_BACKDATE  = os.getenv('JOB_BACKDATE',  '-1')
JOB_STARTDATE = os.getenv('JOB_STARTDATE', '')
JOB_ENDDATE   = os.getenv('JOB_ENDDATE',   '')

# ── Prefect infra ─────────────────────────────────────────────────────────────
PREFECT_WORK_POOL  = os.getenv('PREFECT_WORK_POOL',  'kubernetes-pool')
PREFECT_WORK_QUEUE = os.getenv('PREFECT_WORK_QUEUE', 'default')
PREFECT_SCHEDULER_INGEST = '40 18 * * *' if APP_ENV == 'main' else None
