"""
Flow  : ingest_gcp_postgresql_auth_api_prd
Repo  : ingest_gcp_postgresql_auth
DB    : GCP · PostgreSQL · prd-rds-apse1-auth · auth_api_prd

GCS output path:
    {bucket}/gcp-storage-parquet/auth_api_prd/postgresql/{table}/
    calendar_year=YYYY/month_no=M/day_of_month=D/

Run all tables (normal / scheduled):
    python flows/ingest_gcp_postgresql_auth_api_prd.py

Repair single table:
    python flows/ingest_gcp_postgresql_auth_api_prd.py --table users
    python flows/ingest_gcp_postgresql_auth_api_prd.py --table users --backdate -3
    python flows/ingest_gcp_postgresql_auth_api_prd.py --table users --startdate 2024-01-01 --enddate 2024-01-31
"""

from __future__ import annotations

import argparse

from prefect import flow
from prefect.logging import get_run_logger

from config_flows import (
    DB_TYPE,
    JOB_BACKDATE,
    JOB_ENDDATE,
    JOB_STARTDATE,
    PLATFORM,
)
from config import GCP_BUCKET_APPLICATION
from tasks.tasks_gcp import extract, load, transform

# ── Identity ──────────────────────────────────────────────────────────────────
DB_NAME   = 'contract_api_prd'
GCS_APPLICATION = 'b2bsale' # maps to gcp-storage-parquet/{GCS_APPLICATION}/postgresql/{table}/... existing folder in GCS bucket
FLOW_NAME = f'ingest_{PLATFORM}_{DB_TYPE}_{DB_NAME}'   # ingest_gcp_postgresql_contract_api_prd

# ── Table registry ────────────────────────────────────────────────────────────
# เพิ่ม/ลด table ที่นี่ — ไม่ต้องแตะ flow function
# key   : table name ใน source DB
# value : extract keyword args (p_createdate, p_updatedate และ optional extras)
TABLE_REGISTRY: dict[str, dict] = {
    'Addresss': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'BillingNoteItems': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'BillingNotes': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'BillingNoteToFlowAccLogs': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'CreateDate',
    },
    'Customers': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'CustomerTypes': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'OrderProducts': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'OrderProductsAll': {
        'p_createdate': '',
        'p_updatedate': '',
    },
    'Orders': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'ProductGroups': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'Products': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'ProductTypes': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'SalePositions': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'Sales': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'SaleStatuss': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'saleTeams': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'SaleZones': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    'Units': {
        'p_createdate': 'CreateDate',
        'p_updatedate': 'UpdateDate',
    },
    
    
}

# ── Flow ──────────────────────────────────────────────────────────────────────

@flow(name=FLOW_NAME, log_prints=True)
def ingest_gcp_postgresql_contract_api_prd(
    TABLE_NAME: str = '',
    JOB_BACKDATE_DEPEN: str = JOB_BACKDATE,
    JOB_STARTDATE_DEPEN: str = JOB_STARTDATE,
    JOB_ENDDATE_DEPEN: str = JOB_ENDDATE,
) -> dict[str, str]:
    """Ingest tables from contract_api_prd → GCS Data Lake.

    Parameters
    ----------
    TABLE_NAME : str
        ว่าง = รันทุก table (normal run)
        ระบุชื่อ = repair mode สำหรับ table นั้น
    JOB_BACKDATE_DEPEN : str
        Backdate offset (negative int). '-1' = ใช้ default จาก config
    JOB_STARTDATE_DEPEN / JOB_ENDDATE_DEPEN : str
        ระบุช่วงวันที่ (YYYY-MM-DD) สำหรับ backfill
    """
    logger = get_run_logger()

    effective_backdate = JOB_BACKDATE
    if str(JOB_BACKDATE_DEPEN) != '-1':
        effective_backdate = JOB_BACKDATE_DEPEN

    # ── Resolve tables ────────────────────────────────────────────────
    if TABLE_NAME:
        if TABLE_NAME not in TABLE_REGISTRY:
            raise ValueError(
                f"Unknown table '{TABLE_NAME}'. "
                f"Registered: {', '.join(TABLE_REGISTRY)}"
            )
        tables_to_run = [TABLE_NAME]
        logger.info('🔧 Repair mode — table: %s', TABLE_NAME)
    else:
        tables_to_run = list(TABLE_REGISTRY.keys())
        logger.info('▶ Full run — tables: %s', ', '.join(tables_to_run))

    # ── ETL loop ──────────────────────────────────────────────────────
    results: dict[str, str] = {}
    for table in tables_to_run:
        logger.info('── [%s] start ──', table)
        cfg = TABLE_REGISTRY[table]

        df_extracted  = extract(
            DB_NAME, table,
            p_backdate=effective_backdate,
            p_startdate=JOB_STARTDATE_DEPEN,
            p_enddate=JOB_ENDDATE_DEPEN,
            **cfg,
        )
        df_transformed = transform(df_extracted)
        results[table] = load(DB_TYPE, table, df_transformed, p_application=GCS_APPLICATION) #Change to DB_NAME after Full migration
        # GCS: {bucket}/gcp-storage-parquet/{DB_NAME}/{DB_TYPE}/{table}/...

        logger.info('── [%s] → %s ──', table, results[table])

    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=f'Run flow: {FLOW_NAME}')
    parser.add_argument('--table',     default='',            help='Repair single table (empty = all)')
    parser.add_argument('--backdate',  default=JOB_BACKDATE,  help='Backdate offset e.g. -3')
    parser.add_argument('--startdate', default=JOB_STARTDATE, help='Start date YYYY-MM-DD')
    parser.add_argument('--enddate',   default=JOB_ENDDATE,   help='End date   YYYY-MM-DD')
    args = parser.parse_args()

    ingest_gcp_postgresql_contract_api_prd(
        TABLE_NAME=args.table,
        JOB_BACKDATE_DEPEN=args.backdate,
        JOB_STARTDATE_DEPEN=args.startdate,
        JOB_ENDDATE_DEPEN=args.enddate,
    )
