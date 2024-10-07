"""Data Ingestion Glue for Turbocar Source
    This ingestion job can be executed in 4 different modes:
    Normal (Regular EOD Process):
        - To execute in this mode, the glue job should be triggered as is which takes default value NONE for all args
        - In this mode, we extract GFX source data for current system date
    Resume (Resume from where it failed for any given execution):
        - To execute in this mode, the glue job should be triggered with following set of arguments
                - PROCESS_ID = UUID of the past execution that is to be resumed
                - RUN_PROCESS = 'RESUME'
        - In this mode, for any given process ID, if there was any failure in the last execution, we load only those
               failed tables. Otherwise, we throw an error saying all tables have been processed successfully earlier.
    Restart (Restart the whole load if all successful in past execution else, load the delta):
        - To execute in this mode, the glue job should be triggered with following set of arguments
                - PROCESS_ID = UUID of the past execution that is to be restarted
                - RUN_PROCESS = 'RESTART'
        - In this mode, for any given process ID, if there was any failure in the last execution, we load only those
               failed tables. Otherwise, we restart the whole ingestion again for all tables
    History (Extraction for given date range): <Not implemented yet>
        - To execute in this mode, the glue job should be triggered with following set of arguments
                - RUN_PROCESS = 'HISTORY'
                - SOURCE_FROM_DATE = Start date of the ETL data extraction(YYYY-MM_DD HH:MI:SS)
                - SOURCE_TO_DATE = End date of the ETL data extraction(YYYY-MM_DD HH:MI:SS)
                - SOURCE_DATE = same as SOURCE_TO_DATE(YYYY-MM_DD HH:MI:SS)
            - HIST_TABLE_LIST  = If required to run for a specific tables then ['table1','table2'] Else default it takes
                                    as NONE (i.e. all ACTIVE tables from di-preprocess are loaded for this execution )
        - In this mode data will be extracted for the given date range

    ODP Design Confluence Documentation:
    https://wiki.svbank.com/pages/viewpage.action?spaceKey=OAD&title=ODP+Data+Engineering&preview=/127957327/159220663/LFI-TurboCar-Ingestion-Design%20Document-v1.pdf

    Modification History :
    ================================================
    Date			Modified by			    Description
    2022-08-30      Sathish Saminathan      Initial version
    2022-12-01     Sumit Kumar      Added History Load specific changes

"""
import sys
import logging
from typing import List

import pyspark

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession, types
from awsglue.context import GlueContext
from datetime import datetime, timedelta
import pytz
import pandas as pd

import requests
import boto3
from run_utility import formatter, DatabaseRunner, Runner
from typing import Tuple, Any


handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False


class MyRunner(DatabaseRunner):
    logger.info("MyRunner..")

    def get_boundary_date(
            self,
            source_date_col: str,
            source_to_date_col: str,
            driver_name: str,
            schema_name: str,
            ctrl_table_name: str,
            frequency: str,
            secret_name: str,
            source_time_zone: str,
    ) -> Tuple[Any, str]:
        """This function returns the max date from the table and previous month last date.

        Args
        source_date_col: name of the source date column
        source_to_date_col: name of the source from date column
        driver_name: name of the database driver
        schema_name: The name of the schema
        ctrl_table_name: The name of the control table
        source_from_date_col: name of the source from date column in control table
        frequency: EOD or INTRADAY
        secret_name: The name of the secret that contains the connection string to the database

        Returns:
            The max AS_OF_DT in the table for Daily jobs and previous month last date for Monthly jobs

        Modification History :
            ================================================
            Date			Modified by			    Description
            2022-08-30      Sathish Saminathan      Override the superclass method
        """
        if frequency == "EOD":
            sql_query = f"(select max({source_date_col}) POST_DATE, max({source_to_date_col}) as SOURCE_TO_DATE from TURBOCAR_EXTRACT.dbo.{ctrl_table_name} alias ) A"
            logger.info(f"sql-query:{sql_query},secret_name:{secret_name}")
            df_post_query = self.execute_jdbc_query(secret_name, driver_name, sql_query)
            query_result = df_post_query.collect()
            return query_result[0][0], query_result[0][1]
        elif frequency == "EOM":
            datetime_now = datetime.now(pytz.timezone(source_time_zone))
            dt = (datetime_now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
            return dt, dt


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "PIPELINE_NAME", "RUN_PROCESS", "PROCESS_ID", "SOURCE_DATE",
                                         "SOURCE_FROM_DATE", "SOURCE_TO_DATE","HIST_TABLE_LIST","dmc_sqs_queue"])

    block_size: str = str(128 * 1024 * 1024)
    fetchsize = 10000
    conf = (
        SparkConf()
            .set("spark.hadoop.parquet.block.size", block_size)
            .set("spark.hadoop.dfs.blocksize", block_size)
            .set("spark.hadoop.dfs.block.size", block_size)
            .set("spark.hadoop.fetchsize", fetchsize)
    )

    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    glueContext = GlueContext(sc)

    session = boto3.session.Session()
    dynamodb_resource = boto3.resource("dynamodb")
    dynamodb_client = session.client(service_name="dynamodb")
    secrets_client = session.client(service_name="secretsmanager")
    s3_client = session.client(service_name="s3")

    run = MyRunner(
        spark,
        glueContext,
        args,
        dynamodb_resource,
        dynamodb_client,
        secrets_client,
        s3_client,
    )
    logger.info("MyRunner Init")
    run.run()
