"""
flows/
    ingest_{platform}_{db_type}_{db_name}.py  — one file per DB name (database)

Repo    : ingest_gcp_postgresql_ecoapp
Instance: your-db-hostname  (GCP · PostgreSQL)

Registered flows
----------------
- ingest_gcp_postgresql_ecoapp_prd  (db: ecoapp_prd)
- ingest_gcp_postgresql_chat_api_prd  (db: chat_api_prd)

To add a new db_name:
    1. Create flows/ingest_gcp_postgresql_{db_name}.py
    2. Add import below
    3. Add deployment in prefect.yaml + deploy.py
"""

from flows.ingest_gcp_postgresql_announcement_announcement_api_prd import ingest_gcp_postgresql_announcement_announcement_api_prd
from flows.ingest_gcp_postgresql_apollo_api_prd import ingest_gcp_postgresql_apollo_api_prd
from flows.ingest_gcp_postgresql_b2b_auth_api_prd import ingest_gcp_postgresql_b2b_auth_api_prd
from flows.ingest_gcp_postgresql_contract_api_prd import ingest_gcp_postgresql_contract_api_prd
from flows.ingest_gcp_postgresql_ivote_ivote_api_prd import ingest_gcp_postgresql_ivote_ivote_api_prd


__all__ = [
    'ingest_gcp_postgresql_announcement_announcement_api_prd',
    'ingest_gcp_postgresql_apollo_api_prd',
    'ingest_gcp_postgresql_b2b_auth_api_prd',
    'ingest_gcp_postgresql_contract_api_prd',
    'ingest_gcp_postgresql_ivote_ivote_api_prd',
]
