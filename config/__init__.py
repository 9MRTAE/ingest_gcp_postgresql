from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any

import gcsfs
from google.cloud import secretmanager
from google.oauth2 import service_account

APP_ENV = os.getenv('CI_COMMIT_BRANCH', 'develop')
IMAGE_TAG = os.getenv('IMAGE_TAG', 'ingest-gcp-postgresql-ecoapp:local')
PREFECT_PROJECT_NAME = APP_ENV

if APP_ENV == 'main':
    from .production import *  # noqa: F401,F403
elif APP_ENV == 'develop':
    from .development import *  # noqa: F401,F403
else:
    from .development import *  # noqa: F401,F403


@lru_cache(maxsize=1)
def _secret_client() -> secretmanager.SecretManagerServiceClient:
    return secretmanager.SecretManagerServiceClient()


@lru_cache(maxsize=None)
def get_secret(secret_id: str, *, project_id: str | None = None, version: str = 'latest') -> str:
    """Read a secret value from GCP Secret Manager.

    CHANGE POINT:
    The old repo read credentials from `config/*.json` and hardcoded config values.
    This migrated version reads these secrets from GCP Secret Manager instead:
    - db_host
    - db_password
    - service_account
    """
    effective_project_id = project_id or os.getenv('SECRET_PROJECT_ID', SECRET_PROJECT_ID)
    secret_name = f"projects/{effective_project_id}/secrets/{secret_id}/versions/{version}"
    response = _secret_client().access_secret_version(request={'name': secret_name})
    return response.payload.data.decode('utf-8')


@lru_cache(maxsize=1)
def get_service_account_credentials():
    raw = get_secret(os.getenv('SECRET_SERVICE_ACCOUNT', SECRET_SERVICE_ACCOUNT))
    info: dict[str, Any] = json.loads(raw)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


@lru_cache(maxsize=1)
def get_gcsfs() -> gcsfs.GCSFileSystem:
    return gcsfs.GCSFileSystem(project=GCP_PROJECT, token=get_service_account_credentials())


# When PREFECT_DEPLOY_MODE is set (e.g. during `prefect deploy --all`),
# skip fetching secrets from GCP Secret Manager because the deploy step
# only registers deployments and never connects to databases.
if os.getenv('PREFECT_DEPLOY_MODE'):
    DB_HOST = ''
    DB_PASSWORD = ''
    GCSFS = None  # type: ignore[assignment]
else:
    DB_HOST = get_secret(os.getenv('SECRET_DB_HOST', SECRET_DB_HOST))
    DB_PASSWORD = get_secret(os.getenv('SECRET_DB_PASSWORD', SECRET_DB_PASSWORD))
    GCSFS = get_gcsfs()
