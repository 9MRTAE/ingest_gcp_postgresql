"""
run_local.py — รันใน local สำหรับ dev / repair

Examples
--------
Full run (all tables, all db_names):
    python run_local.py

Full run เฉพาะ db_name:
    python run_local.py --flow ingest_gcp_postgresql_announcement_announcement_api_prd

Repair single table:
    python run_local.py --flow ingest_gcp_postgresql_announcement_announcement_api_prd --table post

Backfill:
    python run_local.py --flow ingest_gcp_postgresql_announcement_announcement_api_prd \
        --startdate 2024-01-01 --enddate 2024-01-31
"""

from __future__ import annotations
import argparse

from config_flows import JOB_BACKDATE, JOB_STARTDATE, JOB_ENDDATE
from flows.ingest_gcp_postgresql_announcement_announcement_api_prd import ingest_gcp_postgresql_announcement_announcement_api_prd
# from flows.ingest_gcp_postgresql_chat_api_prd  import ingest_gcp_postgresql_chat_api_prd

FLOW_MAP = {
    'ingest_gcp_postgresql_announcement_announcement_api_prd': ingest_gcp_postgresql_announcement_announcement_api_prd,
    # 'ingest_gcp_postgresql_chat_api_prd': ingest_gcp_postgresql_chat_api_prd,
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--flow',      default='',            choices=[''] + list(FLOW_MAP), help='Flow to run (empty = all)')
    parser.add_argument('--table',     default='',            help='Repair single table')
    parser.add_argument('--backdate',  default=JOB_BACKDATE)
    parser.add_argument('--startdate', default=JOB_STARTDATE)
    parser.add_argument('--enddate',   default=JOB_ENDDATE)
    args = parser.parse_args()

    kwargs = dict(
        TABLE_NAME=args.table,
        JOB_BACKDATE_DEPEN=args.backdate,
        JOB_STARTDATE_DEPEN=args.startdate,
        JOB_ENDDATE_DEPEN=args.enddate,
    )

    flows_to_run = [FLOW_MAP[args.flow]] if args.flow else list(FLOW_MAP.values())
    for flow_fn in flows_to_run:
        flow_fn(**kwargs)
