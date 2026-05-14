### GCP ###
GIT_CONTRACT_API_PATH = './datasource/contract-management-api.API/Models/Enum/'
SCOPES             = ['https://www.googleapis.com/auth/devstorage.full_control']
GCP_PROJECT        = 'your-gcp-project-id'           # [SCRUBBED] replace with your GCP project ID
GCP_BUCKET         = 'your-nonprd-datalake-bucket'   # [SCRUBBED] replace with your non-prod GCS bucket name
GCP_BUCKET_PARQUET = 'gcp-storage-parquet'
GCP_BUCKET_MSSQL   = 'gcp-ingest-mssql'
GCP_BUCKET_MYSQL   = 'gcp-ingest-mysql'
GCP_REGION         = 'asia-southeast1'
GCP_BUCKET_GIT     = 'git'
GCP_BUCKET_APPLICATION = 'gcp-ingest-postgresql'

DB_TYPE     = 'postgresql'
DB_DRIVER   = 'psycopg2'
DB_USER     = 'postgres'
DB_PORT     = '5432'
DB_HOST_NAME = 'your-db-hostname'                    # [SCRUBBED] replace with your PostgreSQL hostname/alias

# CHANGE POINT:
# service account JSON file is no longer stored in repo.
# secret names can be overridden by env vars if needed.
SECRET_PROJECT_ID  = 'your-gcp-project-id'          # [SCRUBBED] replace with your GCP project ID
SECRET_DB_HOST     = 'your-secret-db-host-key'      # [SCRUBBED] replace with your Secret Manager key name for DB host
SECRET_DB_PASSWORD = 'your-secret-db-password-key'  # [SCRUBBED] replace with your Secret Manager key name for DB password
SECRET_SERVICE_ACCOUNT = 'your-secret-sa-key'       # [SCRUBBED] replace with your Secret Manager key name for service account JSON
