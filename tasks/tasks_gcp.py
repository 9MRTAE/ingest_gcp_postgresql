from __future__ import annotations

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

from config import GCP_BUCKET_APPLICATION
from tasks.main_components_gcp import ExtractSourceData, LoadSourceData, TransformData


@task(name='Extract', retries=0)
def extract(
    p_database,
    p_tablename,
    p_createdate,
    p_updatedate,
    p_backdate=1,
    p_startdate='',
    p_enddate='',
    p_schema='public',
    p_query_join='',
    p_tablename_join='',
    **kwargs,
):
    extracted = ExtractSourceData()
    logger = get_run_logger()

    df, df_datatype_int, df_datatype_bit = extracted.fn_Ingest_Postgresql(
        p_database,
        p_tablename,
        p_createdate,
        p_updatedate,
        p_backdate,
        p_startdate,
        p_enddate,
        p_schema,
        p_query_join,
        p_tablename_join,
    )

    logger.info('Extract data source database %s table %s', p_database, p_tablename)
    if not df.empty:
        logger.info('Rows : %s , Columns : %s', len(df), df.shape[1])
        logger.info('\n%s', pd.concat([df.iloc[:1], df.tail(1)]))

    return df, df_datatype_int, df_datatype_bit


@task(name='Transform', retries=0)
def transform(p_dataframe=None):
    if p_dataframe is None:
        p_dataframe = (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    logger = get_run_logger()
    logger.info('Transform data')

    transformed = TransformData()
    df = transformed.fn_Transform_To_String(p_dataframe=p_dataframe)

    if not df.empty:
        logger.info('Rows : %s , Columns : %s', len(df), df.shape[1])
        logger.info('\n%s', pd.concat([df.iloc[:1], df.tail(1)]))

    return df


@task(name='Load', retries=0)
def load(p_source_type, p_tablename, p_dataframe: pd.DataFrame, p_application: str = GCP_BUCKET_APPLICATION):
    logger = get_run_logger()
    loaded = LoadSourceData()

    logger.info('Load data to bucket %s table %s', p_source_type, p_tablename)
    if not p_dataframe.empty:
        logger.info('Rows : %s , Columns : %s', len(p_dataframe), p_dataframe.shape[1])
        logger.info('\n%s', pd.concat([p_dataframe.iloc[:1], p_dataframe.tail(1)]))

    message = loaded.fn_Load_To_Datalake_GCP(
        p_source_type=p_source_type,
        p_tablename=p_tablename,
        p_dataframe=p_dataframe,
        p_application=p_application,
    )
    logger.info('Load result: %s', message)
    return message
