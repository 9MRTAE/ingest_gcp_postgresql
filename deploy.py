"""
deploy.py — Register all flows in this repo as Prefect deployments.
 
To add a new db_name flow:
    1. Create flows/ingest_gcp_postgresql_{db_name}.py
    2. Import and append to FLOW_OBJECTS below
"""
 
from __future__ import annotations
 
from config_flows import PREFECT_IMAGE, PREFECT_SCHEDULER_INGEST, PREFECT_WORK_POOL, PREFECT_WORK_QUEUE
 
# ── Register flows here ───────────────────────────────────────────────────────
from flows.ingest_gcp_postgresql_announcement_announcement_api_prd import ingest_gcp_postgresql_announcement_announcement_api_prd
from flows.ingest_gcp_postgresql_apollo_api_prd import ingest_gcp_postgresql_apollo_api_prd
from flows.ingest_gcp_postgresql_b2b_auth_api_prd import ingest_gcp_postgresql_b2b_auth_api_prd
from flows.ingest_gcp_postgresql_contract_api_prd import ingest_gcp_postgresql_contract_api_prd
from flows.ingest_gcp_postgresql_contract_api_prd_enumdata import ingest_gcp_postgresql_contract_api_prd_enumdata
from flows.ingest_gcp_postgresql_facility_management_api_prd import ingest_gcp_postgresql_facility_management_api_prd
from flows.ingest_gcp_postgresql_iam_iam_api_prd import ingest_gcp_postgresql_iam_iam_api_prd
from flows.ingest_gcp_postgresql_ivote_ivote_api_prd import ingest_gcp_postgresql_ivote_ivote_api_prd
from flows.ingest_gcp_postgresql_lastmile_api_prd import ingest_gcp_postgresql_lastmile_api_prd
from flows.ingest_gcp_postgresql_learning_center_api_prd import ingest_gcp_postgresql_learning_center_api_prd
from flows.ingest_gcp_postgresql_notification_mkt_noti_api_prd import ingest_gcp_postgresql_notification_mkt_noti_api_prd
from flows.ingest_gcp_postgresql_notification_notification_api_prd import ingest_gcp_postgresql_notification_notification_api_prd
from flows.ingest_gcp_postgresql_payment_payment_api_prd import ingest_gcp_postgresql_payment_payment_api_prd
from flows.ingest_gcp_postgresql_payment_payment_public_api_prd import ingest_gcp_postgresql_payment_payment_public_api_prd
from flows.ingest_gcp_postgresql_pdpa_api_prd import ingest_gcp_postgresql_pdpa_api_prd
from flows.ingest_gcp_postgresql_role_api_prd import ingest_gcp_postgresql_role_api_prd
from flows.ingest_gcp_postgresql_subscription_api_prd import ingest_gcp_postgresql_subscription_api_prd
from flows.ingest_gcp_postgresql_urb_mapping_mapping_api_prd import ingest_gcp_postgresql_urb_mapping_mapping_api_prd
 
FLOW_OBJECTS = [
    ingest_gcp_postgresql_announcement_announcement_api_prd,
    ingest_gcp_postgresql_apollo_api_prd,
    ingest_gcp_postgresql_b2b_auth_api_prd,
    ingest_gcp_postgresql_contract_api_prd,
    ingest_gcp_postgresql_contract_api_prd_enumdata,
    ingest_gcp_postgresql_facility_management_api_prd,
    ingest_gcp_postgresql_iam_iam_api_prd,
    ingest_gcp_postgresql_ivote_ivote_api_prd,
    ingest_gcp_postgresql_lastmile_api_prd,
    ingest_gcp_postgresql_learning_center_api_prd,
    ingest_gcp_postgresql_notification_mkt_noti_api_prd,
    ingest_gcp_postgresql_notification_notification_api_prd,
    ingest_gcp_postgresql_payment_payment_api_prd,
    ingest_gcp_postgresql_payment_payment_public_api_prd,
    ingest_gcp_postgresql_pdpa_api_prd,
    ingest_gcp_postgresql_role_api_prd,
    ingest_gcp_postgresql_subscription_api_prd,
    ingest_gcp_postgresql_urb_mapping_mapping_api_prd,
]
# ─────────────────────────────────────────────────────────────────────────────
 
if __name__ == '__main__':
    for flow_obj in FLOW_OBJECTS:
        kwargs = dict(
            name=flow_obj.name,
            work_pool_name=PREFECT_WORK_POOL,
            image=PREFECT_IMAGE,
            push=False,
            job_variables={'env': {'PREFECT_WORK_QUEUE': PREFECT_WORK_QUEUE}},
        )
        if PREFECT_SCHEDULER_INGEST:
            kwargs['cron'] = PREFECT_SCHEDULER_INGEST
        flow_obj.deploy(**kwargs)
        print(f'Deployed: {flow_obj.name}')