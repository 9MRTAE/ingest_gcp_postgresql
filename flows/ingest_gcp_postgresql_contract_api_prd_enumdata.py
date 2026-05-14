"""
Flow  : ingest_gcp_postgresql_contract_api_prd_enumdata
Repo  : ingest_gcp_postgresql_ecoapp
Source: Git repo · contract-management-api · Models/Enum/*.cs

GCS output path:
    {bucket}/gcp-storage-parquet/git/enumdata/{enum_group}/
    calendar_year=YYYY/month_no=M/day_of_month=D/

Run:
    python flows/ingest_gcp_postgresql_contract_api_prd_enumdata.py
"""

from __future__ import annotations

import os
import re
import tempfile

import git
import pandas as pd
from prefect import flow, get_run_logger

from config_flows import PLATFORM, DB_TYPE
from config import GCP_BUCKET_GIT, GIT_CONTRACT_API_PATH
from tasks.tasks_gcp import load

# ── Identity ──────────────────────────────────────────────────────────────────
DB_NAME   = 'contract_api_prd'
GCS_APPLICATION = 'git'
FLOW_NAME = f'ingest_{PLATFORM}_{DB_TYPE}_{DB_NAME}_enumdata'

# ── Enum Registry ─────────────────────────────────────────────────────────────
# เพิ่ม/ลด .cs file ที่นี่ — ไม่ต้องแตะ flow function
# key   : filename
# value : enum_group name ใน GCS
ENUM_REGISTRY: dict[str, str] = {
    'EnumCustomerGroup.cs':    'EnumCustomerGroup',
    'EnumCustomerType.cs':     'EnumCustomerType',
    'EnumInvoiceType.cs':      'EnumInvoiceType',
    'EnumOrderStatus.cs':      'EnumOrderStatus',
    'EnumOrderType.cs':        'EnumOrderType',
    'EnumPaymentFrequency.cs': 'EnumPaymentFrequency',
    'EnumProductType.cs':      'EnumProductType',
    'EnumSalePosition.cs':     'EnumSalePosition',
}

# ── Flow ──────────────────────────────────────────────────────────────────────

@flow(name=FLOW_NAME, log_prints=True)
def ingest_gcp_postgresql_contract_api_prd_enumdata(
    git_repo_url: str,
    git_branch:   str = 'main',
) -> dict[str, str]:
    """Ingest .cs enum files from contract-management-api → GCS Data Lake.

    Parameters
    ----------
    git_repo_url : str
        URL ของ Git repo contract-management-api
    git_branch : str
        Branch ที่ต้องการ clone (default: main)
    """
    logger = get_run_logger()
    logger.info('▶ Full run — enums: %s', ', '.join(ENUM_REGISTRY))

    results: dict[str, str] = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        # 1. Clone
        logger.info('Cloning %s @ %s', git_repo_url, git_branch)
        git.Repo.clone_from(git_repo_url, tmpdir, branch=git_branch, depth=1)

        # 2. Extract + Load per enum file
        for filename, enum_group in ENUM_REGISTRY.items():
            logger.info('── [%s] start ──', filename)

            filepath = os.path.join(tmpdir, GIT_CONTRACT_API_PATH, filename)
            with open(filepath, encoding='utf-8') as f:
                content = f.read()

            # Parse C# enum → DataFrame
            matches = re.findall(r'(\w+)\s*=\s*(\d+)', content)
            if not matches:
                raise ValueError(f'No enum values found in {filename}')

            df = pd.DataFrame(matches, columns=['enum_name', 'enum_value'])
            df['enum_value']  = df['enum_value'].astype(int)
            df['source_file'] = filename
            df['enum_group']  = enum_group

            results[filename] = load(
                GCS_APPLICATION, enum_group, df,
                p_application=GCS_APPLICATION,
            )
            logger.info('── [%s] → %s ──', filename, results[filename])

    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=f'Run flow: {FLOW_NAME}')
    parser.add_argument('--repo',   required=True, help='Git repo URL')
    parser.add_argument('--branch', default='main', help='Git branch')
    args = parser.parse_args()

    ingest_gcp_postgresql_contract_api_prd_enumdata(
        git_repo_url=args.repo,
        git_branch=args.branch,
    )