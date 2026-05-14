from __future__ import annotations

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import sqlalchemy as SA
import uuid
from prefect.logging import get_run_logger

from config import (
    DB_TYPE,
    DB_DRIVER,
    DB_USER,
    DB_PASSWORD,
    DB_HOST,
    DB_PORT,
    DB_HOST_NAME,
    GCP_BUCKET,
    GCP_BUCKET_PARQUET,
    GCP_BUCKET_APPLICATION,
    GCSFS,
)


class ConnectorDB:
    def __init__(self):
        pass

    def fn_ConnectPostgresql(self, p_database):
        logger = get_run_logger()
        if not p_database:
            raise ValueError('p_database is required — cannot connect without a database name')
        db_name = p_database
        connection_string = f"{DB_TYPE}+{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{db_name}"
        # CHANGE POINT:
        # Do not log the full connection string because it exposes credentials.
        logger.info('Connect PostgreSQL source=%s db=%s', DB_HOST_NAME, db_name)
        engine = SA.create_engine(connection_string)
        conn = engine.connect()
        logger.info('Connect PostgreSQL: Pass')
        return conn


class ExtractSourceData:
    def __init__(self):
        self.connectdb = ConnectorDB()

    def fn_Ingest_Postgresql(
        self,
        p_database,
        p_tablename,
        p_createdate,
        p_updatedate,
        p_backdate=-1,
        p_startdate='',
        p_enddate='',
        p_schema='public',
        p_query_join='',
        p_tablename_join='',
    ):
        logger = get_run_logger()
        conn = self.connectdb.fn_ConnectPostgresql(p_database)
        p_backdate = int(p_backdate)
        p_timezone = 7

        try:
            sql_check_datatype = """SELECT column_name,data_type
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_catalog = '{database}'
        AND table_schema = '{schema}'
        AND table_name = '{tablename}' """.format(
                database=p_database,
                schema=p_schema,
                tablename=p_tablename,
            )
            df_check_datatype = pd.read_sql(sql_check_datatype, conn)
            df_datatype_int = df_check_datatype[df_check_datatype['data_type'].isin(['smallint', 'integer', 'bigint'])]
            df_datatype_bit = df_check_datatype[df_check_datatype['data_type'].isin(['bit'])]

            if p_query_join != '':
                sql_select = 'SELECT A.*,B."{createdate}",B."{updatedate}" '
                sql_from = p_query_join
            else:
                if p_createdate != '' and p_updatedate != '':
                    sql_select = 'SELECT * '
                    sql_from = 'FROM {schema}."{tablename}" AS B '
                else:
                    sql_select = """SELECT *,CAST(NOW() AS TIMESTAMP) AS ftCreateDate, CAST(NOW() AS TIMESTAMP) AS ftUpdateDate
                                ,EXTRACT (YEAR FROM NOW()) AS calendar_year
                                ,EXTRACT (MONTH FROM NOW()) AS month_no
                                ,EXTRACT (DAY FROM NOW()) AS day_of_month """
                    sql_from = 'FROM {schema}."{tablename}" AS B '

            sql_where = ''
            sql_daily = ''

            if p_createdate != '' and p_updatedate != '':
                if p_query_join != '':
                    effective_tablename = p_tablename_join
                    sql_check_date = """SELECT data_type
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE table_catalog = '{database}'
                    AND table_schema = '{schema}'
                    AND table_name = '{tablename}'
                    AND column_name = '{createdate}' """.format(
                        database=p_database,
                        tablename=effective_tablename,
                        createdate=p_createdate,
                        schema=p_schema,
                    )
                    df_check_date = pd.read_sql(sql_check_date, conn)
                    if df_check_date.empty:
                        logger.warning(
                            "Column '%s' not found in %s.%s; falling back to synthetic timestamps",
                            p_createdate,
                            p_schema,
                            effective_tablename,
                        )
                        p_createdate = ''
                        p_updatedate = ''
                        datetype = None
                    else:
                        datetype = df_check_date.data_type[0]
                else:
                    df_check_date = df_check_datatype[df_check_datatype['column_name'] == p_createdate].reset_index(drop=True)
                    if df_check_date.empty:
                        logger.warning(
                            "Column '%s' not found in %s.%s; falling back to synthetic timestamps",
                            p_createdate,
                            p_schema,
                            p_tablename,
                        )
                        p_createdate = ''
                        p_updatedate = ''
                        datetype = None
                    else:
                        datetype = df_check_date.data_type[0]

                if p_createdate == '' or p_updatedate == '':
                    # Missing partition columns; generate synthetic timestamps and partitions via NOW().
                    sql_select = """SELECT *,CAST(NOW() AS TIMESTAMP) AS ftCreateDate, CAST(NOW() AS TIMESTAMP) AS ftUpdateDate
                                ,EXTRACT (YEAR FROM NOW()) AS calendar_year
                                ,EXTRACT (MONTH FROM NOW()) AS month_no
                                ,EXTRACT (DAY FROM NOW()) AS day_of_month """
                    if p_query_join != '':
                        sql_from = p_query_join
                    else:
                        sql_from = 'FROM {schema}."{tablename}" AS B '
                elif datetype in ('datetime', 'timestamp with time zone', 'timestamp without time zone'):
                    sql_select += ',EXTRACT (YEAR FROM COALESCE(B."{updatedate}" ,B."{createdate}")) AS calendar_year '
                    sql_select += ',EXTRACT (MONTH FROM COALESCE(B."{updatedate}" ,B."{createdate}")) AS month_no '
                    sql_select += ',EXTRACT (DAY FROM COALESCE(B."{updatedate}" ,B."{createdate}")) AS day_of_month '
                    sql_where_createdate = 'B."{createdate}"'
                    sql_where_updatedate = 'B."{updatedate}"'
                elif datetype == 'integer':
                    sql_select += ",EXTRACT('Year' from to_timestamp (COALESCE ({updatedate},{createdate}))) as calendar_year "
                    sql_select += ",EXTRACT('Month' from to_timestamp (COALESCE ({updatedate},{createdate}))) as month_no "
                    sql_select += ",EXTRACT('Day' from to_timestamp (COALESCE ({updatedate},{createdate}))) as day_of_month "
                    sql_where_createdate = 'to_timestamp({createdate})'
                    sql_where_updatedate = 'to_timestamp({updatedate})'
                else:
                    raise TypeError(f'Unsupported datetime datatype for partitioning: {datetype}')

                if p_createdate != '' and p_updatedate != '' and p_startdate != '' and p_enddate != '':
                    sql_where = 'WHERE '
                    sql_daily = """(CAST (COALESCE(""" + sql_where_updatedate + """,""" + sql_where_createdate + """) AS DATE) BETWEEN '{startdate}' AND '{enddate}')"""
                else:
                    if p_createdate != '' and p_updatedate != '' and p_backdate < 0:
                        operator = '>' if p_backdate < -1 else '='
                        sql_where = 'WHERE '
                        sql_daily = """(CAST (COALESCE(""" + sql_where_updatedate + """,""" + sql_where_createdate + """) AS DATE) """ + operator + """ CAST(CAST(NOW() AS TIMESTAMP) + INTERVAL '{timezone} HOURS' + INTERVAL '{backdate} DAY' AS DATE) ) """

            sql_full = sql_select + sql_from + sql_where + sql_daily
            sql_full = sql_full.format(
                schema=p_schema,
                tablename=p_tablename,
                createdate=p_createdate,
                updatedate=p_updatedate,
                timezone=p_timezone,
                backdate=p_backdate,
                startdate=p_startdate,
                enddate=p_enddate,
            )
            logger.info('Source SQL : %s', sql_full)

            df = pd.read_sql(sql_full, conn)
        finally:
            conn.close()

        return df, df_datatype_int, df_datatype_bit


class TransformData:
    def __init__(self):
        pass

    def fn_Transform_To_String(self, p_dataframe: pd.DataFrame):
        logger = get_run_logger()
        logger.info('Start: fn_Transform_To_String')

        df_source = p_dataframe[0].copy()
        df_datatype_int = p_dataframe[1]
        df_datatype_bit = p_dataframe[2]

        bit_columns = df_datatype_bit.column_name.tolist() if not df_datatype_bit.empty else []
        int_columns = df_datatype_int.column_name.tolist() if not df_datatype_int.empty else []

        if bit_columns:
            # CHANGE POINT:
            # safer than old `applymap(ord)` and keeps output as 'true'/'false'.
            for col in bit_columns:
                df_source[col] = df_source[col].apply(
                    lambda v: np.nan if pd.isna(v)
                    else (ord(v) if isinstance(v, str)
                    else int.from_bytes(v, 'big') if isinstance(v, (bytes, bytearray))
                    else int(v))
                )
                df_source[col] = np.where(df_source[col] == 0, 'false', 'true')

        if int_columns:
            df_source[int_columns] = df_source[int_columns].astype('Int64')

        partition_columns = ['calendar_year', 'month_no', 'day_of_month']
        string_columns = [col for col in df_source.columns if col not in partition_columns]
        if string_columns:
            df_source[string_columns] = df_source[string_columns].astype(str)

        df = df_source.replace('NaT', np.nan)
        df = df.replace('None', np.nan)
        df = df.replace('NaN', np.nan)
        df = df.replace('nan', np.nan)
        df = df.replace('<NA>', np.nan)

        if string_columns:
            df[string_columns] = df[string_columns].astype(str)

        df.columns = df.columns.str.lower()
        return df


class LoadSourceData:
    def __init__(self):
        pass

    def fn_Load_To_Datalake_GCP(self, p_source_type, p_tablename, p_dataframe: pd.DataFrame, p_application: str = GCP_BUCKET_APPLICATION):
        logger = get_run_logger()
        logger.info('Start: fn_Load_To_Datalake_GCP')

        target_path = f"gs://{GCP_BUCKET}/{GCP_BUCKET_PARQUET}/{p_application}/{p_source_type}/{p_tablename}"
        logger.info('Load to : %s', target_path)

        ds.write_dataset(
            pa.Table.from_pandas(p_dataframe),
            target_path,
            format='parquet',
            filesystem=GCSFS,
            partitioning_flavor='hive',
            basename_template=f"{uuid.uuid4()}-{{i}}.parquet",
            partitioning=['calendar_year', 'month_no', 'day_of_month'],
            existing_data_behavior='overwrite_or_ignore',
            max_partitions=100000,
        )
        return 'success'
