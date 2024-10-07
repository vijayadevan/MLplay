"""Data Ingestion Run Utility

This module contains the `Run Utility`_ class for the Data Ingestion Pipeline.
Further documentation can be found in the `ODP Confluence Documentation`_
Documentation Style is based on `Google Python Style Guide`_.

Todo:
    Replay


To use this:
  - Inherit the relevant class
  - Override the method table_helper
  - Run in AWS Glue Jobs

.. _ODP Confluence Documentation:
   https://wiki.svbank.com/display/OAD/ODP+Common
   _Run Utility:
   https://code.gitlab.prod.us-west-2.tlz.svbank.com/odp/application-odp-ingestion-resources/-/tree/dev/ingestion-scripts/stage-a/common/run_utility.py
   _Google Python Style Guide:
   https://google.github.io/styleguide/pyguide.html

Modification History :
    ================================================
    Date			Modified by			Description
    2022-08-25      Rajashekar Kolli    Initial Version
    2022-09-15      Surajit Mandal      Add EOM Functionality
    2022-10-13      Rishab              Added Trigger SQS Method to initiate Stage-A DMC Process
    2022-12-01      Rajesh Bandi        Enabled HISTORY Load
    
"""
import sys
import threading
from typing import List, Tuple, Any, Optional, Union
import base64
import json
import logging
import re
import pytz
import uuid
from datetime import datetime, timedelta
import types as python_types
from xmlrpc.client import Boolean
from boto3.dynamodb.types import TypeDeserializer
import boto3
# from mypy_boto3_dynamodb.service_resource import Table
from pyspark.sql import types as spark_types
import pyspark
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr, Equals, And, ConditionBase
from naming_config import get_name_ddb
import awsglue.context


class FormatterJSON(logging.Formatter):
    """This takes a log record and returns a JSON string"""

    def format(self, record):
        """It takes a log record, formats it as a JSON string, and returns the string

        Args:
            record: The log record that is being formatted

        Returns:
            A JSON string.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        j = {
            "levelname": record.levelname,
            "time": "%(asctime)s.%(msecs)dZ"
            % dict(asctime=record.asctime, msecs=record.msecs),
            "aws_request_id": getattr(
                record, "aws_request_id", "00000000-0000-0000-0000-000000000000"
            ),
            "message": record.message,
            "module": record.module,
            "extra_data": record.__dict__.get("data", {}),
        }
        return json.dumps(j)


formatter = FormatterJSON(
    "[%(levelname)s]\t%(asctime)s.%(msecs)dZ\t%(levelno)s\t%(message)s\n",
    "%Y-%m-%dT%H:%M:%S",
)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False


class Instrumentation(type):
    """This is a metaclass that is used for instrumenting a class

    Note:
        When applied to a class, will wrap all the functions in that class with a logging wrapper

    Modification History :
        ================================================
        Date			Modified by			Description
        2022-08-25      Rajashekar Kolli    Initial Version
    """

    def __new__(cls, name, bases, attrs):
        """This is the new method that is called when a class is created

        Note:
            For each attribute in the class, if the attribute is a function, replace it with a function that
            logs the function call

        Args:
            cls: The class being created
            name: The name of the class being created
            bases: The base classes of the class being defined
            attrs: A dictionary of the attributes of the class being created

        Returns:
            Instrumentation: The class object itself.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        for attr_name, attr_value in attrs.items():
            if isinstance(attr_value, python_types.FunctionType):
                attrs[attr_name] = cls.log(attr_value)

        return super(Instrumentation, cls).__new__(cls, name, bases, attrs)

    @classmethod
    def log(cls, func):
        """This is the function that is called when an instrumented function is called

        Note:
            This `log` function takes in a function as an argument and returns a wrapper function that logs
            the start and end of the function it wraps

        Args:
            cls: The class that the method belongs to
            func: The function that we're going to wrap

        Returns:
            The wrapper function.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """

        def wrapper(*args, **kwargs):
            """This wraps the function that is being instrumented

            Note:
                This wrapper function takes in any number of arguments and keyword arguments,
                logs the start of the function, runs the function, logs the completion of the function,
                and returns the result of the function.

            Returns:
                The wrapper function.

            Modification History :
                ================================================
                Date			Modified by			Description
                2022-08-25      Rajashekar Kolli    Initial Version
            """
            logger.info(f"Starting {func.__name__}...")
            result = func(*args, **kwargs)
            logger.info(f"Completed {func.__name__}!")
            return result

        return wrapper


class PreprocessController(metaclass=Instrumentation):
    """This class is used for coordinating with the infrastructure to preprocess the ingested data

    Note:
        The class is a wrapper for the DynamoDB tables that are used to track the status of the pre-processing
        steps of the data pipeline

    Modification History :
        ================================================
        Date			Modified by			Description
        2022-08-25      Rajashekar Kolli    Initial Version
    """

    def preprocess_read(self, dynamodb_resource, pipeline_name: str, env: str) -> dict:
        """This method is used to read the PreProcess Control

        Note:
            This function reads the last record from the DynamoDB table `odp-ENV-DI-PREPROCESS`
            where `ENV` is the environment (e.g. `dev`) and `PIPELINE_NM` is the name of the
            pipeline (e.g. `pipeline_name`)

        Args:
            dynamodb_resource: This is the boto3 resource object for DynamoDB
            pipeline_name (str): The name of the pipeline you want to run
            env (str): The environment you're working in

        Returns:
            dict: The last item in the list of items returned by the query.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """ 
        di_preprocess_table_name = "odp-ENV-DI-PREPROCESS".replace("ENV", env)
        di_preprocess_table = dynamodb_resource.Table(di_preprocess_table_name)
        di_preprocess = di_preprocess_table.query(
            KeyConditionExpression=Key("PIPELINE_NM").eq(pipeline_name),
        )
        logger.info(f"di_preprocess:{str(di_preprocess)}")
        logger.info(f"{len(di_preprocess['Items'])}")
        if len(di_preprocess['Items']) ==0: 
                notification = Notification()
                message = f"Invalid or missing pipeline name {pipeline_name}"
                error = message
                notification.publish_sns(pipeline_name=pipeline_name, env=env , error=error ,message=message)
                logger.error(error)
                logger.info(error)
                sys.exit(error)

        return di_preprocess.get("Items", [{}])[-1]


    def get_max_load_datetime(self, dynamodb_resource, pipelnie_name, env):
        """This method is used to read the PreProcess Control and return max load date for a given pipeline

        Note:
            This function reads the last record from the DynamoDB table `odp-ENV-DI-PREPROCESS`
            where `ENV` is the environment (e.g. `dev`) and `PIPELINE_NM` is the name of the
            pipeline (e.g. `pipeline_name`)

        Args:
            dynamodb_resource: This is the boto3 resource object for DynamoDB
            pipeline_name (str): The name of the pipeline you want to run
            env (str): The environment you're working in

        Returns:
            str: The last execution load_date.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        di_preprocess_table_name = "odp-ENV-DI-PREPROCESS-CONTROL".replace("ENV", env)
        table = dynamodb_resource.Table(di_preprocess_table_name)
        response = table.query(
            KeyConditionExpression=Key("PIPELINE_NM").eq(pipelnie_name),
            ScanIndexForward=False,
            Limit=1,
        )
        if response["Items"]:
            o_response = response["Items"][0]
            return o_response[
                "PULL_END"
            ]
        else:
            return None

    def preprocess_control_create(
        self,
        pipeline_name: str,
        di_preprocess_control_table,
        source_from_date,
        source_to_date,
        source_time_zone,
        process_execution_id,
    ) -> dict:
        """This method is used to create a new record PreProcess Control

        Note:
            This function creates a new item in the DI PreProcess Control table, which is used to track
            the status of the preprocessing process.

        Args:
            pipeline_name (str): The name of the pipeline that is being executed
            di_preprocess_control_table: The name of the table in DynamoDB that will store
            the control information for the preprocess step

        Returns:
            dict: The di_preprocess_control_item.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        # process_execution_id = str(uuid.uuid4())
        # logger.info(f"process_execution_id:{process_execution_id}")
        di_preprocess_control_item = {
            "PIPELINE_NM": pipeline_name,
            "LOAD_DT_TM": source_to_date,
            "UUID": process_execution_id,
            "PULL_START": source_from_date,
            "PULL_END": source_to_date,
        }
        di_preprocess_control_table.put_item(Item=di_preprocess_control_item)
        logger.info(
            "DI PreProcess Control Put Item complete",
            extra=dict(data=di_preprocess_control_item),
        )
        return di_preprocess_control_item

    def preprocess_control_read(
        self,
        dynamodb_resource,
        pipeline_name: str,
        env: str,
        process_execution_id: str = None,
        source_from_date: str = None,
        source_to_date: str = None,
    ) -> Tuple[dict, Any]:
        """This method is used to read the PreProcess Control

        Note:
            It queries the DynamoDB table for the last item in the table

        Args:
            dynamodb_resource: This is the boto3 resource object for DynamoDB
            pipeline_name (str): The name of the pipeline you want to run
            env (str): The environment you're running in
            process_execution_id (str): The ID of the process execution
            source_from_date: The end date of the source data
            source_to_date: The start date of the source data

        Returns:
            Tuple[dict, Any]: The last item in the list of items returned from the query.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        di_preprocess_control_table_name = "odp-ENV-DI-PREPROCESS-CONTROL".replace(
            "ENV", env
        )

        di_preprocess_control_table = dynamodb_resource.Table(
            di_preprocess_control_table_name
        )
        filter_expression: ConditionBase = ConditionBase(None)
        if process_execution_id:
            filter_expression = Attr("UUID").eq(process_execution_id)
            logger.info(f"process_execution_id filter in di-preprocess-control:{process_execution_id}")
        elif source_from_date and source_to_date:
            filter_expression = Attr("PULL_START").eq(source_from_date) & Attr(
                "PULL_END"
            ).eq(source_to_date)
            logger.info(f"source_from_date:{source_from_date},source_to_date:{source_to_date}")
        else:
            exception_info = f"""There is no filter expression: 
                UUID:{process_execution_id},
                PULL_START:{source_from_date}, 
                PULL_END:{source_to_date}
            """
            logger.info(exception_info)
            logger.error(exception_info)

            notification = Notification()
            message = exception_info
            notification.publish_sns(pipeline_name=pipeline_name, env=env , error=message ,message=message)
            sys.exit(message)

        logger.info(f"di_preprocess_control filter-expression:{str(filter_expression)}")
        di_preprocess_control = di_preprocess_control_table.query(
            KeyConditionExpression=Key("PIPELINE_NM").eq(pipeline_name),
            FilterExpression=filter_expression,
        ) 
        
        logger.info(f"di_preprocess_control:{str(di_preprocess_control)}")
        di_preprocess_control_items: List[dict] = di_preprocess_control.get("Items", [])
        logger.info(
            "Finished DI PreProcess Control Query Item",
            extra=dict(data=di_preprocess_control_items),
        )

        if len(di_preprocess_control_items) > 1:
            exception_info ="Too many preprocess control items found"
            logger.info(exception_info)
            logger.error(exception_info)

            notification = Notification()
            message = exception_info
            notification.publish_sns(pipeline_name=pipeline_name, env=env , error=message ,message=message)
            sys.exit(message)
            raise Exception(exception_info)
        else:
            di_preprocess_control_item = (
                di_preprocess_control_items[0] if di_preprocess_control_items else {}
            )

        return di_preprocess_control_item, di_preprocess_control_table 

    def preprocess_control_check(
        self, 
        env : str,
        dynamodb_client, 
        pipeline_name :str, 
        source_date : str, 
    ):
        """This method is used to read the PreProcess Control and checks for the pipeline and LOAD_DT_TM

        Note:
            It queries the DynamoDB table for the last item in the table

        Args:
            dynamodb_resource: This is the boto3 resource object for DynamoDB
            pipeline_name (str): The name of the pipeline you want to run
            env (str): The environment you're running in
            source_date: The start date of the source data

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version

        """
        try: 
            di_preprocess_control_table_name = "odp-ENV-DI-PREPROCESS-CONTROL".replace("ENV", env)
            key = {"PIPELINE_NM": {"S": pipeline_name}, "LOAD_DT_TM" : {"S": source_date}}

            preprocess_ctrl_response = dynamodb_client.get_item(TableName= di_preprocess_control_table_name, Key=key)
            preprocess_ctrl_response = preprocess_ctrl_response["Item"]
            if preprocess_ctrl_response:
                error_info = f"Process has already executed, process_id:{preprocess_ctrl_response['UUID']}"
                logger.info(error_info)
                logger.error(error_info)
                sys.exit(error_info)
        except Exception as e:
            exception_info = f"logging error when LOAD_DT_TM not found:{str(e)}, source_date:{source_date}"
            logger.info(exception_info)
            logger.error(exception_info)
            notification = Notification()
            message = exception_info
            notification.publish_sns(pipeline_name=pipeline_name, env=env , error=message ,message=message)

    def preprocess_control_update(
        self,
        attribute: str,
        value: str,
        pipeline_name: str,
        di_preprocess_control_item: dict,
        di_preprocess_control_table,
    ):
        """This method is used to update the PreProcess Control

        Note:
            This function updates the value of a specific attribute in a specific row in a DynamoDB table

        Args:
            attribute (str): The name of the attribute to update
            value (str): The value to update the attribute to
            pipeline_name (str): The name of the pipeline
            di_preprocess_control_item: This is the item that we're updating
            di_preprocess_control_table: the table object

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        di_preprocess_control_table.update_item(
            Key={
                "PIPELINE_NM": pipeline_name,
                "LOAD_DT_TM": di_preprocess_control_item["LOAD_DT_TM"],
            },
            UpdateExpression="SET #Attribute = :Value",
            ExpressionAttributeNames={"#Attribute": attribute},
            ExpressionAttributeValues={":Value": value},
        )

    def preprocess_control_delete(
        self, dynamodb_resource, env: str, pipeline_name: str
    ):
        """This method is used to delete an item from the PreProcess Control

        Note:
            This deletes the record from the table that matches the pipeline name and the load date time

        Args:
            dynamodb_resource: The boto3 resource object for DynamoDB
            env (str): The environment you're working in
            pipeline_name (str): The name of the pipeline you want to delete

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        di_preprocess_control_table_name = "odp-ENV-DI-PREPROCESS-CONTROL".replace(
            "ENV", env
        )

        di_preprocess_control_table = dynamodb_resource.Table(
            di_preprocess_control_table_name
        )
        di_preprocess_control = di_preprocess_control_table.query(
            KeyConditionExpression=Key("PIPELINE_NM").eq(pipeline_name),
        )

        di_preprocess_control_items: dict = di_preprocess_control.get("Items", [])

        if di_preprocess_control_items:
            di_preprocess_control_table.delete_item(
                Key={
                    "PIPELINE_NM": pipeline_name,
                    "LOAD_DT_TM": di_preprocess_control_items[0]["LOAD_DT_TM"],
                }
            )

    def preprocess_exec_status_create(
        self,
        pipeline_name: str,
        job_name: str,
        process_execution_id: str,
        run_process: str,
        di_preprocess_control_item: dict,
        di_preprocess_exec_status_table,
        pipeline_exec_date, 
        source_to_date,
        job_run_id,
    ):
        """This method is used to create a new record from the PreProcess Execution Status

        Note:
            This function creates a new item in the DynamoDB table `di_preprocess_exec_status`
            with the following attributes:

            - `PIPELINE_NM`: The name of the pipeline
            - `LOAD_DT_TM`: The date and time of the pipeline execution
            - `UUID`: The unique identifier of the pipeline execution
            - `PULL_START`: The date and time the pipeline execution started
            - `PULL_END`: The date and time the pipeline execution ended
            - `JOB_NAME`: The name of the job
            - `JOB_RUN_ID`: The unique identifier of the job execution
            - `PIPELINE_EXEC_DATE`: The date and time the pipeline execution started
            - `PIPELINE_STATUS`: The status of the pipeline execution
            - `RUN_PROCESS`: The process

        Args:
            pipeline_name (str): The name of the pipeline
            job_name (str): The name of the job that is being run
            process_execution_id (str): This is the unique identifier for the process execution
            run_process (str): This is the process that is being run. It can be either 'RUN', 'RESTART', 'RESUME'
            di_preprocess_control_item (dict): This is the item from the control table that we created in the previous step
            di_preprocess_exec_status_table: The table where the execution status will be stored

        Returns:
            The di_preprocess_exec_status_item.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        di_preprocess_exec_status_item = {
            "PIPELINE_NM": pipeline_name,
            "LOAD_DT_TM": di_preprocess_control_item["LOAD_DT_TM"],
            "UUID": process_execution_id,
            "PULL_START": di_preprocess_control_item["PULL_START"],
            "PULL_END": source_to_date,
            "JOB_NAME": job_name,
            "JOB_RUN_ID": job_run_id,
            "PIPELINE_EXEC_DATE": pipeline_exec_date,
            "PIPELINE_STATUS": "",
            "RUN_PROCESS": run_process,
            "TABLES": {},
        }
        logger.info(
            "DI PreProcess Exec Status Put Item",
            extra=dict(data=di_preprocess_exec_status_item),
        )
        di_preprocess_exec_status_table.put_item(Item=di_preprocess_exec_status_item)
        return di_preprocess_exec_status_item

    def preprocess_exec_status_read(
        self, dynamodb_resource, env: str, process_execution_id: str
    ) -> Tuple[dict, Any]:
        """This method is used to read a record from the PreProcess Execution Status

        Note:
            This function reads the last record from the DynamoDB table `odp-ENV-DI-PREPROCESS-EXEC-STATUS`
            where `ENV` is the environment name and `process_execution_id` is the UUID of the process execution

        Args:
            dynamodb_resource: This is the boto3 resource object for DynamoDB
            env (str): The environment you're running in
            process_execution_id (str): This is the UUID of the process execution

        Returns:
            Tuple[dict, dict]: The last record from the DynamoDB table `odp-ENV-DI-PREPROCESS-EXEC-STATUS`

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        di_preprocess_exec_status_table_name = (
            "odp-ENV-DI-PREPROCESS-EXEC-STATUS".replace("ENV", env)
        )

        di_preprocess_exec_status_table = dynamodb_resource.Table(
            di_preprocess_exec_status_table_name
        )
        di_preprocess_exec_status = di_preprocess_exec_status_table.query(
            KeyConditionExpression=Key("UUID").eq(process_execution_id),
        )
        di_preprocess_exec_status_items = di_preprocess_exec_status.get("Items", []) 
        logger.info(f"di_preprocess_exec_status_items: {str(di_preprocess_exec_status_items)}")
        if len(di_preprocess_exec_status_items) == 0 or di_preprocess_exec_status_items is None:
            di_preprocess_exec_status_item = {}
            logger.info(f"di_preprocess_exec_status_items are 0")
        else:
            logger.info(f"uuid:{process_execution_id}")
            while "LastEvaluatedKey" in di_preprocess_exec_status:
                di_preprocess_exec_status = di_preprocess_exec_status_table.query(
                    KeyConditionExpression=Key("UUID").eq(process_execution_id),
                    ExclusiveStartKey=di_preprocess_exec_status["LastEvaluatedKey"],
                )
                logger.info(f"di_preprocess_exec_status while LastEvaluatedKey:{str(di_preprocess_exec_status)}")
                di_preprocess_exec_status_items.extend(di_preprocess_exec_status.get("Items", []))
            di_preprocess_exec_status_item = di_preprocess_exec_status_items[-1]
        return di_preprocess_exec_status_item, di_preprocess_exec_status_table

    def get_preprocess_exec_status_error_check(
        self, dynamodb_client, env: str, process_execution_id, pipeline_execution_date
    ):
        """This function is used to get the error count from preprocess_exec_status table

        Args:
               dynamodb_resource: This is the boto3 resource object for DynamoDB
               env (str): The environment you're running in
               process_execution_id (str): This is the UUID of the process execution
               pipeline_execution_date: Pipeline execution date

         Returns:
               returns integer, number of failed tables.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        count = 0 
        tables = []
        di_preprocess_exec_status = self.get_preprocess_exec_status_tables(
            dynamodb_client, env, process_execution_id, pipeline_execution_date
        )
        logger.info(f"di_preprocess_exec_status:{str(di_preprocess_exec_status)}")
        if di_preprocess_exec_status: 
            for table_name, val in di_preprocess_exec_status.items():
                if val["STATUS"] == "FAILURE":
                    count = count + 1
                    tables.append(table_name)
            return count, tables
        else: 
            return 0, []

    def get_max_exec_date(self, dynamodb_resource, env, execution_id):

        """This method is used to read the Process Execution Status table and return execution date for any execution ID

        Note:
            This function reads the last record from the DynamoDB table `odp-ENV-DI-PREPROCESS-EXEC-STATUS`
            where `ENV` is the environment (e.g. `dev`) and execution_id

        Args:
            dynamodb_resource: This is the boto3 resource object for DynamoDB
            execution_id (str): The UUID for the execution
            env (str): The environment you're working in

        Returns:
            str: execution date.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        di_preprocess_exec_status_tbl_name = "odp-ENV-DI-PREPROCESS-EXEC-STATUS".replace("ENV", env)
        table = dynamodb_resource.Table(di_preprocess_exec_status_tbl_name)
        response = table.query(
            KeyConditionExpression=Key("UUID").eq(execution_id), 
            ScanIndexForward=False,
            Limit=1,
        )
        if response["Items"]:
            return response["Items"][0]["PIPELINE_EXEC_DATE"]
            # error_count, error_tables = self.get_preprocess_exec_status_tables(env,execution_id,o_response["PIPELINE_EXEC_DATE"])
            # return error_tables
        else:
            return None

    def get_preprocess_exec_status_tables(
        self, dynamodb_client, env, process_execution_id, pipeline_execution_date
    ):

        """This method is used to read the Process Execution Status table and return execution status for any execution ID

        Note:
            This function reads the last record from the DynamoDB table `odp-ENV-DI-PREPROCESS-EXEC-STATUS`
            where `ENV` is the environment (e.g. `dev`) and `UUID` is the name of the
            process_execution_id

        Args:
            dynamodb_resource: This is the boto3 resource object for DynamoDB
            process_execution_id (str): UUID of process execution
            env (str): The environment you're working in

        Returns:
            str: The last execution load_date.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        logger.info(f"pipeline_execution_date:{pipeline_execution_date}")
        logger.info(f"process_execution_id:{process_execution_id}")
        di_preprocess_exec_status_table_name = (
            "odp-ENV-DI-PREPROCESS-EXEC-STATUS".replace("ENV", env)
        ) 
        di_preprocess_exec_status_response = dynamodb_client.get_item(
            TableName=di_preprocess_exec_status_table_name,
            Key={
                "UUID": {"S": process_execution_id},
                "PIPELINE_EXEC_DATE": {"S": pipeline_execution_date},
            }
        )
        logger.info(f"di_preprocess_exec_status_response:{str(di_preprocess_exec_status_response)}")
        di_preprocess_exec_status_response = di_preprocess_exec_status_response["Item"]
        di_preprocess_exec_status = {
            k: TypeDeserializer().deserialize(value=v)
            for k, v in di_preprocess_exec_status_response.items()
        }
        if "TABLES" in di_preprocess_exec_status_response: 
            return di_preprocess_exec_status["TABLES"]
        else: 
            return []

    def preprocess_exec_status_update(
        self,
        attribute: str,
        value: str,
        process_execution_id: str,
        di_preprocess_control_item: dict,
        di_preprocess_exec_status_table,
        pipeline_exec_date,
    ):
        """This method is used to update a record in the PreProcess Execution Status

        Note:
            Update the value of the attribute in the preprocess_exec_status table for the given process_execution_id and
            load_dt_tm

        Args:
            attribute (str): The name of the attribute to update
            value (str): the value to be updated
            process_execution_id (str): This is the UUID of the process execution
            di_preprocess_control_item (dict): This is the item from the 'Preprocess Control' table
            di_preprocess_exec_status_table: the table where the status is stored

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        di_preprocess_exec_status_table.update_item(
            Key={
                "UUID": process_execution_id,
                "PIPELINE_EXEC_DATE":  pipeline_exec_date,
            },
            UpdateExpression="SET #Attribute = :Value",
            ExpressionAttributeNames={"#Attribute": attribute},
            ExpressionAttributeValues={":Value": value},
        )

    def preprocess_exec_status_table_create(
        self,
        di_preprocess_control_item,
        di_preprocess_exec_status_table,
        process_execution_id: str,
        table_name: str,
        pipeline_exec_date, 
    ):
        """This method is used to create a new record for the PreProcess Execution Status

        Note:
            Update the execution status table with the start time of the table being processed

        Args:
            di_preprocess_control_item (dict): This is the item from the control table
            di_preprocess_exec_status_table: The table object
            process_execution_id (str): This is the UUID of the process execution
            table_name (str): The name of the table to be created

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        start_time = datetime.now()
        start_time_str = start_time.strftime("%F %T")
        di_preprocess_exec_status_item = {
            "SRC_CNT": "0",
            "STATUS": "",
            "END_DT_TM": "",
            "TGT_CNT": "0",
            "START_DT_TM": start_time_str,
        }
        # Update Execution-Status with new Table
        logger.info("DI PreProcess Exec Status Update Item")
        expression_attribute_values = {":Table": di_preprocess_exec_status_item}
        di_preprocess_exec_status_table.update_item(
            Key={
                "UUID": process_execution_id,
                "PIPELINE_EXEC_DATE": pipeline_exec_date,
            },
            UpdateExpression="SET #Tables.#Table = :Table",
            ExpressionAttributeNames={"#Tables": "TABLES", "#Table": table_name},
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW",
        )

    def preprocess_exec_status_table_update(
        self,
        attribute: str,
        value: str,
        di_preprocess_control_item: dict,
        di_preprocess_exec_status_table,
        process_execution_id: str,
        table_name: str,
        pipline_exec_date
    ):
        """This method is used to update a record in the PreProcess Execution Status table

        Note:
            This function updates the preprocess_exec_status_table with the value of the attribute passed in

        Args:
            attribute (str): The name of the attribute to update
            value (str): The value to be updated
            di_preprocess_control_item (dict): This is the item in the PreProcess Control table
            di_preprocess_exec_status_table: The table that stores the status of the preprocessing execution
            process_execution_id (str): This is the UUID of the process execution
            table_name (str): The name of the table you want to update

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        attribute_names = {
            "#Tables": "TABLES",
            "#Table": table_name,
            "#Attribute": attribute,
        }
        attribute_values = {":VALUE": value}
        di_preprocess_exec_status_table.update_item(
            Key={
                "UUID": process_execution_id,
                "PIPELINE_EXEC_DATE": pipline_exec_date,
            },
            UpdateExpression="SET #Tables.#Table.#Attribute = :VALUE",
            ExpressionAttributeNames=attribute_names,
            ExpressionAttributeValues=attribute_values,
            ReturnValues="UPDATED_NEW",
        )

    def get_didc_tables(
        self,
        env: str,
        dynamodb_resource,
        source_name: str,
        schema_name: str,
    ):
        """This Function is used to get the list of DIDC tables
        Args:
            env: Environment name
            dynamodb_resource: This is the boto3 resource object for DynamoDB
            source_name : name of the source in DIDC table
            schema_name : name of the schema in DIDC table

        Return:
            List of tables

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        didc_dataset_table_name = "odp-ENV-DIDC-Dataset-Schema".replace("ENV", env)
        table = dynamodb_resource.Table(didc_dataset_table_name)
        response = table.query(
            IndexName="type-source-schema-index",
            KeyConditionExpression=Key("SourceName-SchemaName").eq(
                f"{source_name}-{schema_name}"
            ),
            ProjectionExpression="TableName",
        )
        data = response["Items"]
        while "LastEvaluatedKey" in response:
            response = table.query(
                IndexName="type-source-schema-index",
                KeyConditionExpression=Key("SourceName-SchemaName").eq(
                    f"{source_name}-{schema_name}"
                ),
                ProjectionExpression="TableName",
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            data.extend(response["Items"])
        Tables = [v for i in data for j, v in i.items()]
        return list(set(Tables))


class Notification(metaclass=Instrumentation):
    """This class send notifications to SNS

    Note:
        It sends notification to SNS topic

    Modification History :
        ================================================
        Date			Modified by			Description
        2022-08-25      Rajashekar Kolli    Initial Version
    """

    def publish_sns(self, **kwargs):
        """This method used to send notifications to SNS

        Note:
            It sends notification to SNS topic

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        try:
            sns_params = dict()
            for key, value in kwargs.items():
                sns_params[key] = value
            
            SystemDateTimeUTC= datetime.now(pytz.timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S %Z")
            sns_params["SystemDateTimeUTC"]= SystemDateTimeUTC
            sns_params["Stage"] = "Stage-A"
            sns_params["severity"] = "ERROR"
            message = json.dumps(sns_params)
            logger.info(f"parameters: {message}")
            
            dynamodb_resource = boto3.resource('dynamodb')
            sns_target_db = dynamodb_resource.Table('odp-ENV-DI-notification-fanout'.replace('ENV', sns_params["env"]))            
            response = sns_target_db.get_item(
                Key={"MSG_SEVERITY": sns_params["severity"]},
                ConsistentRead=True)
                
            items = response["Item"]
            logger.info(items)
            source_sns = items["SOURCE_SNS"]
            logger.info(f"target_sns : {source_sns}")

            sns = boto3.client('sns')
                
            response = sns.publish(
            TopicArn=source_sns ,
            Message= message,
            Subject = (f"{sns_params['pipeline_name']} : {sns_params['severity'] }")
            )
            logger.info(response)
        except Exception as e:
            logger.error(e)
            logger.error(f"error occured {message}",e)

class Runner(metaclass=Instrumentation):
    """This class runs the PreProcessing Pipeline

    Note:
        It reads data from an ingestion source, processes that data, then writes the data to S3

    Modification History :
        ================================================
        Date			Modified by			Description
        2022-08-25      Rajashekar Kolli    Initial Version
        2022-10-13      Rishab              Added stage-A DMC Framework changes
    """

    def __init__(
        self,
        spark_session: pyspark.sql.session.SparkSession,
        glue_context: awsglue.context.GlueContext,
        args: dict,
        dynamodb_resource,
        dynamodb_client,
        secrets_client,
        s3_client,
        sqs_client = None,
    ):
        """This function is used to initialize the class and set the variables that will be used throughout the class

        Args:
            spark_session: The Spark session that is created by Glue
            glue_context: The AWS Glue context object
            args: This is the dictionary of parameters passed to the job
            dynamodb_resource: This is the boto3 resource for DynamoDB
            dynamodb_client: boto3 client for DynamoDB
            secrets_client: This is the boto3 client for AWS Secrets Manager
            s3_client: This is the boto3 client for S3
            sqs_client: This is the boto3 client for SQS

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
            2022-10-13      Rishab              Initialised SQS Client and created DMC SQS QUEUE instance
        """
        self.spark = spark_session
        self.glue_context = glue_context

        self.run_process = args["RUN_PROCESS"]
        self.process_id = args["PROCESS_ID"]
        self.job_run_id = args["JOB_RUN_ID"]
        self.hist_table_list = args["HIST_TABLE_LIST"]
        self.source_date = ""
        self.source_from_date = (
            None if args["SOURCE_FROM_DATE"] == "NONE" else args["SOURCE_FROM_DATE"]
        )
        self.source_to_date = (
            None if args["SOURCE_TO_DATE"] == "NONE" else args["SOURCE_TO_DATE"]
        )
        self.pipeline_name = args["PIPELINE_NAME"]
        self.env = args["JOB_NAME"].split("-")[1]
        self.job_name = args["JOB_NAME"]
        start_time = datetime.now()
        self.pipeline_exec_date = start_time.strftime("%F %T")
        self.pipeline_satus = "FAILURE"

        self.dynamodb_resource = dynamodb_resource
        self.dynamodb_client = dynamodb_client
        self.s3_client = s3_client
        
        
        #self.dmc_flag = 'N'
        #self.dmc_operation = 'SRC_ONLY/TGT_ONLY'
        
        if "dmc_sqs_queue" in args:
            self.dmc_sqs_queue = args["dmc_sqs_queue"]
            regional_endpoint_url = self.dmc_sqs_queue.split("/")[0:3]
            self.sqs_client = boto3.client(service_name="sqs",endpoint_url='/'.join(regional_endpoint_url))
        else:
            self.dmc_sqs_queue = ""
            self.sqs_client = boto3.client(service_name="sqs")
        
        
        self.secrets_client = secrets_client
        self.preprocess_controller = PreprocessController()
        self.dataframe_write_mode = "overwrite"

        logger.info(f"run_process:{self.run_process}")
        logger.info(f"process_id:{self.process_id}")
        logger.info(f"source_date:{self.source_date }")
        logger.info(f"pipeline_name:{self.pipeline_name}")
        logger.info(f"env:{self.env}")
        logger.info(f"job_name:{self.job_name}")

    def run(self):  # sourcery skip: raise-specific-error
        """The main entry point for the PreProcessing Pipeline

        Note:
            This function starts a job, processes tables, and completes the job

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
            2022-09-15      Surajit Mandal      Add EOM Functionality
            2022-10-13      Rishab              Initialised DMC FLAG object and DMC_operation
        """
        dynamodb_di_preprocess_controller = PreprocessController()
        di_preprocess_item = self.get_preprocess_item(dynamodb_di_preprocess_controller)

        src_nm = di_preprocess_item["SRC_NM"]
        src_type = di_preprocess_item["SRC_TYPE"]
        secret_name = di_preprocess_item["SECRET"]
        schema_name = di_preprocess_item["SCHEMA"]
        driver = di_preprocess_item["DRIVER"]
        threads = di_preprocess_item["THREADS"]
        source_time_zone = di_preprocess_item["SOURCE_TIME_ZONE"]
        target_path = str(di_preprocess_item["TGT_PATH"])
        trigger_bucket = str(di_preprocess_item["TRIGGER_BUCKET"])
        trigger_filename = str(di_preprocess_item["TRIGGER_FILENAME"])
        default_source_from_date = di_preprocess_item["DEFAULT_SOURCE_FROM_DATE"]
        source_date_col = di_preprocess_item["POST_DATE_COLUMN"]
        source_to_date_col = di_preprocess_item["SOURCE_TO_DATE_COLUMN"]
        # schema_name = di_preprocess_item["SCHEMA_NAME"]
        ctrl_tbl_name = di_preprocess_item["CTRL_TBL_NAME"]
        frequency = di_preprocess_item["FREQ"]
        
        # check if dmc flag exists
        if "DMC_FLAG" in di_preprocess_item:
            self.dmc_flag = di_preprocess_item["DMC_FLAG"]
            if self.dmc_flag == 'Y' and self.dmc_sqs_queue == "":                    
                    raise Exception("Missing DMC SQS Queue Name. Please verify the job parameters in Glue Job!!")
        else:
            self.dmc_flag = 'N'

        # check if dmc_operation flag exists    
        if "DMC_operation" in di_preprocess_item:
            self.dmc_operation = di_preprocess_item["DMC_operation"]
        else:
            self.dmc_operation = 'SRC_ONLY/TGT_ONLY'
        
        logger.info(f"dmc_flag: {self.dmc_flag}")
        logger.info(f"dmc_operation: {self.dmc_operation}")
        logger.info(f"src_nm:{src_nm}")
        logger.info(f"src_type:{src_type}")
        logger.info(f"secret_name:{secret_name}")
        logger.info(f"schema_name:{schema_name}")
        logger.info(f"driver:{driver}")
        logger.info(f"threads:{threads}")
        logger.info(f"source_time_zone:{source_time_zone}")
        logger.info(f"target_path:{target_path}")
        logger.info(f"trigger_bucket:{trigger_bucket}")
        logger.info(f"trigger_filename:{trigger_filename}")
        logger.info(f"default_source_from_date:{default_source_from_date}")
        logger.info(f"source_date_col:{source_date_col}")
        logger.info(f"ctrl_tbl_name:{ctrl_tbl_name}")
        logger.info(f"frequency:{frequency}")

        if frequency != "EOM" and frequency != "EOD" and frequency != "INTRADAY" and frequency != "EOQ":
            exception_info =f"Incorrect Frequency Type {frequency}!! The valid Frequency types are 'EOM, 'EOD' or 'INTRADAY'"
            logger.info(exception_info)
            logger.error(exception_info)
            notification = Notification()
            message = exception_info
            notification.publish_sns(pipeline_name= self.pipeline_name, env= self.env , error=message ,message=message)
            sys.exit(message)

        delta_table_list = self.start_logic(dynamodb_di_preprocess_controller,
                default_source_from_date,
                source_date_col,
                source_to_date_col,
                driver,
                schema_name,
                ctrl_tbl_name,
                frequency,
                secret_name,
                source_time_zone)
        (
            di_preprocess_control_item,
            di_preprocess_control_table,
            di_preprocess_exec_status_table,
            di_preprocess_exec_status_tables,
            process_execution_id,
        ) = self.start_job(dynamodb_di_preprocess_controller, source_time_zone)

        # Reference logic from psgl_eod.get_table_list_to_preprocess
        # tables should be the intersection of previous tables and
        # current tables only when delta_table_list is non-empty
        logger.info(f"tables_list:{str(di_preprocess_item)}")
        tables: List[dict] = di_preprocess_item["TABLES"]
        tables = self.delta_table_validation(delta_table_list, tables)
        didc_tables = dynamodb_di_preprocess_controller.get_didc_tables(
            self.env, self.dynamodb_resource, src_nm, schema_name
        )
        self.didc_validation(tables, didc_tables)
        logger.info(f"didc_tables:{str(didc_tables)}")

        table_list = self.get_tablename_query_list(
            tables=tables,
            didc_tables=didc_tables,
            env=self.env,
            schema_name=schema_name,
            source_name=src_nm,
            source_from_date=self.source_from_date,
            source_to_date=self.source_to_date,
            source_date=self.source_date,
            target_path=target_path,
        )
        logger.info(f"table_list:{str(table_list)}")

        self.process_tables(
            preprocess_controller=dynamodb_di_preprocess_controller,
            di_preprocess_control_item=di_preprocess_control_item,
            di_preprocess_control_table=di_preprocess_control_table,
            di_preprocess_exec_status_table=di_preprocess_exec_status_table,
            di_preprocess_exec_status_tables=di_preprocess_exec_status_tables,
            driver=driver,
            process_execution_id=process_execution_id,
            schema_name=schema_name,
            source_date=self.source_date,
            secret_name=secret_name,
            src_nm=src_nm,
            src_type=src_type,
            tables=table_list,
            target_path=target_path,
            threads_count=threads,
        )

        self.complete_job(
            preprocess_controller=dynamodb_di_preprocess_controller,
            di_preprocess_control_item=di_preprocess_control_item,
            di_preprocess_exec_status_table=di_preprocess_exec_status_table,
            process_execution_id=process_execution_id,
            schema_name=schema_name,
            src_nm=src_nm,
            trigger_bucket=trigger_bucket,
            trigger_filename=trigger_filename,
        )

    def delta_table_validation(self, delta_table_list, tables):
        """The function will return the list of tables which are included in the ingestion table list
        and are business approved and cataloged

        Note:
            This function takes delta table list for any given execution and then returns a list after validating that
            they are in ingestion table list and are business approved and cataloged

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        if delta_table_list:
            delta_tables = {}
            delta_table_arr = []
            for index in tables:
                for table_name, val in index.items():
                    if table_name in delta_table_list:
                        delta_tables[table_name] = val
                        delta_table_arr.append(delta_tables)
            tables = delta_table_arr
            logger.info(f"tables:{str(tables)}")
        return tables

    def start_logic(self, preprocess_controller: PreprocessController,
                default_source_from_date,
                source_date_col,
                source_to_date_col,
                driver,
                schema_name,
                ctrl_tbl_name,
                frequency,
                secret_name,
                source_time_zone) -> List[dict]:
        """This function is used to start the PreProcessing Pipeline

        Note:
            run_process are RESTART, RELOAD, and HISTORY

        Args:
            preprocess_controller: This is the DataIngestion object that is passed to the start_logic method

        Notes:  logic for the different run types:
                        # 1. Normal: null-exec-id , run-process is null
                        # 2. RESTART: not null exec-id , run-process = 'RESTART
                        # 3. RESUME: not null exec-id , run-process = 'RESUME
                        # 4. HISTORY: null exec-id , run-process = 'HISTORY , source_from_date, to_date

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
            2022-12-01      Rajesh Bandi        Enabled HISTORY Load

        """

        if self.run_process == "HISTORY" and self.process_id == "NONE":
            """This is the case where the job is being run for historic user selected tables records"""
            logger.info("start_logic_history starting .. ")
            return self.start_logic_history(preprocess_controller,
                default_source_from_date,
                source_date_col,
                source_to_date_col,
                driver,
                schema_name,
                ctrl_tbl_name,
                frequency,
                secret_name,
                source_time_zone)
        elif self.run_process == "RESTART" and self.process_id != "NONE" and self.process_id is not None:
            """This is the case where the job is restarting new"""
            logger.info("start_logic_restart starting .. ")
            return self.start_logic_restart(preprocess_controller=preprocess_controller)
        elif self.run_process == "RESUME" and self.process_id != "NONE" and self.process_id is not None:
            """This is the case where the job starts from where it last left off"""
            logger.info("start_logic_resume starting .. ")
            return self.start_logic_resume(preprocess_controller=preprocess_controller)

        """This is default normal execution"""
        logger.info("start_logic_normal starting .. ")
        return self.start_logic_normal(preprocess_controller,
            default_source_from_date,
            source_date_col,
            source_to_date_col,
            driver,
            schema_name,
            ctrl_tbl_name,
            frequency,
            secret_name,
            source_time_zone)
    
    def start_logic_history(
        self, preprocess_controller: PreprocessController,
        default_source_from_date,
        source_date_col,
        source_to_date_col,
        driver,
        schema_name,
        ctrl_tbl_name,
        frequency,
        secret_name,
        source_time_zone,
    ) -> List[dict]:
        """This function is used to start the history loads as per source_from_date and to_date by user passed arguments

        Note:
            This starts history ETL load with user passed arguments

        Args:
            preprocess_controller: PreprocessController

        Returns:
            delta_table_list: This is the list of table dicts that were configured in di_preprocess [OR] user defined tables in hist_table_list argument

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-11-25      Rajesh Bandi    Initial Version

        """
        source_date = self.source_from_date
        source_to_date = self.source_to_date
        source_from_date = self.source_from_date    
        logger.info(f"Hist source_date:{source_date}")
        logger.info(f"Hist source_to_date:{source_to_date}")
        logger.info(f"Hist source_from_date:{source_from_date}")
        (
            di_preprocess_control_item,
            di_preprocess_control_table,
        ) = preprocess_controller.preprocess_control_read(
            dynamodb_resource=self.dynamodb_resource,
            pipeline_name=self.pipeline_name,
            env=self.env,
            process_execution_id=self.process_id,
        )
        preprocess_controller.preprocess_control_check(self.env,self.dynamodb_client, self.pipeline_name, self.source_date)
        self.set_process_exec_id()
        self.set_boundary_date(str(source_date), str(source_to_date))
        logger.info(f"source_date value:{self.source_date}")
        logger.info(f"source_to_date value:{self.source_to_date}")
        self.set_source_from_date(str(source_from_date))
        logger.info(f"source_from_date value:{self.source_from_date}")
        if self.hist_table_list != "NONE":
            hist_table_list = self.hist_table_list
            logger.info(f"Hist hist_table_list:{hist_table_list}")
            self.dataframe_write_mode = "append"
            current_date_time: str = datetime.now().strftime("%F %T")
            #uuid = di_preprocess_exec_status_item.get('UUID')
            #logger.info(f"UUID in RESUME:{uuid}")
            logger.info(f"List of hist tables:{str(hist_table_list)}")
            return hist_table_list

        else:
            return []
    def start_logic_normal(
        self, preprocess_controller: PreprocessController,
        default_source_from_date,
        source_date_col,
        source_to_date_col,
        driver,
        schema_name,
        ctrl_tbl_name,
        frequency,
        secret_name,
        source_time_zone,
    ) -> List[dict]:
        """This function is used to start the PreProcessing Pipeline in regular mode

        Note:
            This starts normal ETL load with all default parameters

        Args:
            preprocess_controller: PreprocessController

        Returns:
            delta_table_list: This is the list of table dicts that were not successful

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version

        """
        if self.source_date is None or self.source_to_date is None:
            source_date, source_to_date = self.get_boundary_date(
                source_date_col=source_date_col,
                source_to_date_col=source_to_date_col,
                driver_name=driver,
                schema_name=schema_name,
                ctrl_table_name=ctrl_tbl_name,
                frequency=frequency,
                secret_name=secret_name,
                source_time_zone=source_time_zone,
            )
            logger.info(f"source_date:{source_date}")
            logger.info(f"source_to_date:{source_to_date}")
            self.set_boundary_date(str(source_date), str(source_to_date))
        logger.info(f"source_date:{self.source_date}")
        logger.info(f"source_to_date:{self.source_to_date}")
        preprocess_controller.preprocess_control_check(self.env,self.dynamodb_client, self.pipeline_name, self.source_date)
        self.set_process_exec_id()
        (
            di_preprocess_control_item,
            di_preprocess_control_table,
        ) = preprocess_controller.preprocess_control_read(
            dynamodb_resource=self.dynamodb_resource,
            pipeline_name=self.pipeline_name,
            env=self.env,
            process_execution_id=self.process_id,
        )

        if self.source_from_date is None:
            source_from_date = preprocess_controller.get_max_load_datetime(
                self.dynamodb_resource, self.pipeline_name, self.env
         )

        if source_from_date is None:
            self.set_source_from_date(default_source_from_date)
        else:
            self.set_source_from_date(str(source_from_date))

        logger.info(f"source_from_date:{self.source_from_date}")
        logger.info(f"****:{self.source_date}")
        return []

    def start_logic_restart(
        self, preprocess_controller: PreprocessController
    ) -> List[dict]:  # sourcery skip: raise-specific-error
        """This function is used to restart the PreProcessing Pipeline

        Note:
            This starts over the last run job
            start time is set to the previous start time
            stop time is set to the previous end time

        Args:
            preprocess_controller: PreprocessController

        Returns:
            delta_table_list: This is the list of table dicts that were not successful

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version

        """

        if self.process_id is None or self.process_id =="NONE" or self.process_id == "":
            raise Exception ("Process execution id cannot be blank or null for RESTART functionality")

        self.dataframe_write_mode = "append"
        (
            di_preprocess_control_item,
            di_preprocess_control_table,
        ) = preprocess_controller.preprocess_control_read(
            dynamodb_resource=self.dynamodb_resource,
            pipeline_name=self.pipeline_name,
            env=self.env,
            process_execution_id=self.process_id,
        )

        if di_preprocess_control_item.get('UUID') is None:
            raise Exception (f"Invalid process-id:{self.process_id} for RESTART functionality")

        last_exec_process_id = di_preprocess_control_item["UUID"]
        self.source_date = di_preprocess_control_item["LOAD_DT_TM"]
        self.source_from_date = di_preprocess_control_item["PULL_START"]
        self.source_to_date = di_preprocess_control_item["PULL_END"]
        # if self.process_id == "NONE" or self.process_id != last_exec_process_id:
        #     raise Exception(
        #         "For Reload, we must pass valid process_id. Please rerun the same using Process ID {}",
        #         last_exec_process_id,
        #     )

        (
            di_preprocess_exec_status_item,
            di_preprocess_exec_status_table,
        ) = preprocess_controller.preprocess_exec_status_read(
            dynamodb_resource=self.dynamodb_resource,
            env=self.env,
            process_execution_id=self.process_id,
        )

        if di_preprocess_exec_status_item is None:
            raise Exception (f"No prior execution history found for RESTART functionality")

        # # get the dict of tables
        # di_preprocess_exec_status_tables: dict = (
        #     preprocess_controller.get_preprocess_exec_status_tables(
        #         self.dynamodb_client,
        #         self.env,
        #         self.process_id,
        #         self.pipeline_exec_date,
        #     )
        # )
        # return the list of unsuccessful table dicts
        return []
        #     di_preprocess_exec_status_table[table_name]
        #     for table_name, value in di_preprocess_exec_status_tables.items()
        #     if value["STATUS"] != "SUCCESS"
        # ]

    def start_logic_resume(
        self, preprocess_controller: PreprocessController
    ) -> List[dict]:  # sourcery skip: raise-specific-error
        """This function is used to resume the PreProcessing Pipeline

        Note:
            This resumes the last run job after the last table that ran
            start time is set to the previous start time
            end time is set to the previous end time

            Update Preprocess Control 'Table.PULL_START' with the current start time
            Update Preprocess Control 'Table.PULL_END' with the current start time

            Update Preprocess Control Exec Status 'Table.PULL_START' with the current start time
            Update Preprocess Control Exec Status 'Table.PULL_END' with the current start time

        Args:
            preprocess_controller: PreprocessController

        Returns:
            delta_table_list: This is the list of table dicts that were not successful

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version

        """
        self.dataframe_write_mode = "append"

        (
            di_preprocess_control_item,
            di_preprocess_control_table,
        ) = preprocess_controller.preprocess_control_read(
            dynamodb_resource=self.dynamodb_resource,
            pipeline_name=self.pipeline_name,
            env=self.env,
            process_execution_id=self.process_id,
            source_from_date=self.source_from_date,
            source_to_date=self.source_to_date,
        )
        if di_preprocess_control_item.get('UUID') is None:
            raise Exception (f"Invalid process-id:{self.process_id} for RESUME functionality")

        last_exec_process_id = di_preprocess_control_item["UUID"]
        self.source_date = di_preprocess_control_item["LOAD_DT_TM"]
        self.source_from_date = di_preprocess_control_item["PULL_START"]
        self.source_to_date = di_preprocess_control_item["PULL_END"]
        if self.process_id == "NONE" or self.process_id != last_exec_process_id:
            help_text = "we must pass a valid process_id"
            action_text = (
                f"Please rerun the same using Process ID {last_exec_process_id}"
            )
            raise Exception(f"For RESUME, {help_text}. {action_text}")

        current_date_time: str = datetime.now().strftime("%F %T")

        (
            di_preprocess_exec_status_item,
            di_preprocess_exec_status_table,
        ) = preprocess_controller.preprocess_exec_status_read(
            dynamodb_resource=self.dynamodb_resource,
            env=self.env,
            process_execution_id=di_preprocess_control_item["UUID"],
        )
        uuid = di_preprocess_exec_status_item.get('UUID')
        logger.info(f"UUID in RESUME:{uuid}")
        if di_preprocess_exec_status_item.get('UUID') is None:
            exception_info =f"Invalid process-id:{self.process_id} for RESUME functionality"
            logger.info(exception_info)
            logger.error(exception_info)
            notification = Notification()
            message = exception_info
            notification.publish_sns(pipeline_name= self.pipeline_name, env= self.env , error=message ,message=message)
            sys.exit(message)

        pipeline_exec_date = preprocess_controller.get_max_exec_date(self.dynamodb_resource, self.env, self.process_id)
        # self.set_pipeline_exec_date(pipeline_exec_date)
        error_count, error_tables = preprocess_controller.get_preprocess_exec_status_error_check(self.dynamodb_client, self.env, self.process_id, pipeline_exec_date)
        # # get the dict of tables
        # di_preprocess_exec_status_tables: dict = di_preprocess_exec_status_item[
        #     "TABLES"
        # ]

        # return the list of unsuccessful table dicts
        # unsuccessful_tables = [
        #     di_preprocess_exec_status_table[table_name]
        # if not unsuccessful_tables:
        #     raise Exception(
        #         f"""
        #             Previous Execution was successful for all tables
        #             for process_execution_id={di_preprocess_control_item["UUID"]}.
        #             To reload, rerun the same process_execution_id and run_process=RELOAD
        #             """
        #     )
        logger.info(f"List of error tables:{str(error_tables)}")
        return error_tables

    def process_tables(
        self,
        preprocess_controller,
        di_preprocess_control_item,
        di_preprocess_control_table,
        di_preprocess_exec_status_table,
        di_preprocess_exec_status_tables,
        driver,
        process_execution_id,
        schema_name,
        source_date,
        secret_name,
        src_nm,
        src_type,
        tables,
        target_path,
        threads_count,
    ):
        """This function is used to process the tables from the PreProcessing Metadata

        Note:
            This function loops through a list of tables, and for each table,
            it calls two other functions, start_table and complete_table

        Args:
            preprocess_controller: the data integration object
            di_preprocess_control_item: This is the control table that contains the list of tables to process
            di_preprocess_control_table: This is the table that contains the list of tables to process
            di_preprocess_exec_status_table: This is the table that will be used to track the status of the 'preprocessing'
            di_preprocess_exec_status_tables: This is a dataframe that contains the status of each table in the process
            driver: the driver to use to connect to the database
            process_execution_id: This is the unique ID for the process execution
            schema_name: The name of the schema in the database where the data will be stored
            secret_name: The name of the secret in AWS Secrets Manager that contains the credentials for the source database
            source_date: The date of the data being processed
            src_nm: The name of the source
            src_type: The type of source (e.g. Oracle, SQL Server, etc.)
            tables: a list of dictionaries, each dictionary containing a table name and a list of columns
            target_path: The path to the target directory where the files will be written

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
            2022-01-11      Rishab              Created dmc_src_tgt_pth instance

        """
        for i in tables:
            table_json = json.loads(i)
        splits = [
            tables[i : i + int(threads_count)]
            for i in range(0, len(tables), int(threads_count))
        ]
        for k in splits:
            threads = []
            for i in k:
                # table_list = self.get_table_list(self, tables, src_nm, schema_name, source_date, source_from_date, source_to_date, target_path)
                table_json = json.loads(i)
                tbl = table_json["TABLE_NAME"]
                qry = table_json["QUERY"]
                tgt_path = table_json["TGT_PATH"]
                self.dmc_src_tgt_pth = tgt_path
                process = threading.Thread(
                    target=self.start_table_main,
                    args=[
                        di_preprocess_control_item,
                        di_preprocess_control_table,
                        di_preprocess_exec_status_table,
                        di_preprocess_exec_status_tables,
                        driver,
                        preprocess_controller,
                        process_execution_id,
                        schema_name,
                        source_date,
                        secret_name,
                        src_nm,
                        src_type,
                        table_json,
                        tbl,
                        qry,
                        tgt_path,
                    ],
                )
                process.start()
                threads.append(process)
            for process in threads:
                process.join()

    def start_table_main(
        self,
        di_preprocess_control_item,
        di_preprocess_control_table,
        di_preprocess_exec_status_table,
        di_preprocess_exec_status_tables,
        driver,
        preprocess_controller,
        process_execution_id,
        schema_name,
        source_date,
        secret_name,
        src_nm,
        src_type,
        table_json,
        table_name,
        sql_query,
        target_path,
    ):

        logger.info(f"Table: {table_name}")
        logger.info(f"SQL: {sql_query}")
        df_source_data = self.start_table(
            preprocess_controller,
            di_preprocess_control_item,
            di_preprocess_exec_status_table,
            di_preprocess_exec_status_tables,
            driver,
            process_execution_id,
            schema_name,
            secret_name,
            src_type,
            table_json,
            table_name,
            sql_query,
        )
        
        self.complete_table(
            df_source_data,
            preprocess_controller,
            di_preprocess_control_item,
            di_preprocess_control_table,
            di_preprocess_exec_status_table,
            process_execution_id,
            schema_name,
            source_date,
            src_nm,
            table_name,
            target_path,
        )
        
    def dmc_sqs(self,di_preprocess_control_item,target_path,table_name,dmc_operation,entity_name=None):
        """This function is used to send the message to DMC SQS Queue after Processing the data for each Table

            Note:
                This function invokes trigger_sqs function
                
            Modification History :
                ================================================
                Date			Modified by			Description
                2022-11-08      Rishab              Initial Version
                2023-01-11      Rishab              Removed target_path 'NONE' condition
                2023-01-24      Rishab              Added entity_name in parameters
                
            """
        logger.info(f"DMC FLAG IS {self.dmc_flag}. DMC Operation:{dmc_operation}. Starting the Stage-A DMC Flow!")
        logger.info("DMC SQS queue started")
        logger.info(f"Target_path:{target_path}")

        if self.dmc_sqs_queue == "":
            raise Exception("Missing DMC SQS Queue Name")
            
        self.trigger_sqs(PIPELINE_NAME=self.pipeline_name,
                        table_name = table_name,
                        SOURCE_DATE = di_preprocess_control_item["LOAD_DT_TM"],
                        PROCESS_EXECUTION_ID = di_preprocess_control_item['UUID'],
                        ENV = self.env,
                        SOURCE_FROM_DATE = di_preprocess_control_item["PULL_START"],
                        SOURCE_TO_DATE = di_preprocess_control_item["PULL_END"],
                        TGT_PATH = target_path,
                        DMC_operation = dmc_operation,
                        entity_name = entity_name
                        )
        logger.info("DMC SQS queue completed")
        
    def trigger_sqs(self,PIPELINE_NAME,table_name,SOURCE_DATE,PROCESS_EXECUTION_ID,ENV,SOURCE_FROM_DATE,SOURCE_TO_DATE,TGT_PATH,DMC_operation,entity_name):
        """This function is used to send the message to DMC SQS Queue after Processing the data for each Table

            Note:
                This function send individual messages for each table processed for the given Source

            Args:
                PIPELINE_NAME: name of the pipeline running
                table_name: Name of the table to check DMC Count in Source and Target Layer
                SOURCE_DATE: The date of the data being processed
                PROCESS_EXECUTION_ID: This is the unique ID for the process execution
                ENV: The environment in which the ipeline is running
                SOURCE_TO_DATE: The pull end date of the data being processed
                SOURCE_FROM_DATE: The pull start date of the data being processed
                TGT_PATH: The path to the target directory where the files will be written
                
            Modification History :
                ================================================
                Date			Modified by			Description
                2022-11-08      Rishab              Initial Version
                2023-01-24      Rishab              Added entity_name for event_body
                
            """
        ##SQS MESSAGE CREATION
        #logger.info(ENV)
        json_file_name = PIPELINE_NAME+'.json'
        logger.info(json_file_name)
        #logger.info(table_name)
        logger.info("constructing the event")
        artifact_bucket = get_name_ddb('s3_ing_ingestion_glue_artifacts_bucket')
        event_body = {}
        event_body['pipeline_name']=PIPELINE_NAME
        event_body['source_date']=SOURCE_DATE
        event_body['source_layer_name']="SOURCE"
        event_body['target_layer_name']="LANDING"
        event_body['batch_id']=PROCESS_EXECUTION_ID
        event_body['dataset_name']=table_name
        event_body['dmc_config_path']="s3://{artifact_bucket}/datacontrol-config/{json_file_name}".format(artifact_bucket=artifact_bucket,json_file_name=json_file_name)
        event_body['target_file_path']=TGT_PATH+table_name
        event_body['env']=ENV
        event_body['pull_start_date']=SOURCE_FROM_DATE
        event_body['pull_end_date']=SOURCE_TO_DATE
        event_body['DMC_operation']= DMC_operation
        event_body['pipeline_exectime']=self.pipeline_exec_date
        if entity_name!=None:
            event_body['entity_name']=entity_name
        logger.info("done constructing the event")
        logger.info(event_body)
        # Get the service resource
        logger.info("Sending sqs message. SQS queue Name: {}".format(self.dmc_sqs_queue))        
        response = self.sqs_client.send_message(QueueUrl=self.dmc_sqs_queue,MessageBody=json.dumps(event_body),MessageGroupId="dmc-pipeline",MessageDeduplicationId=str(uuid.uuid1()))
        logger.info(response)
        logger.info("sqs msg sent")

    def start_job(
        self, preprocess_controller: PreprocessController, source_time_zone
    ) -> Tuple[dict, Any, Any, dict, dict, Any]:
        """This function is used to start the PreProcessing Pipeline Job

        Note:
            It reads the control table, creates a new control table if it doesn't exist, reads the
            execution status table, creates a new execution status table if it doesn't exist, and
            returns the control table, execution status table, and execution status tables

        Args:
            preprocess_controller: This is the class that contains all the methods for the job

        Returns:
            Tuple[dict, Any, Any, dict, dict, Any]: di_preprocess_control_item,
                                                    di_preprocess_control_table,
                                                    di_preprocess_exec_status_table,
                                                    di_preprocess_exec_status_tables,
                                                    di_preprocess_item,
                                                    process_execution_id

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version

        """
        (
            di_preprocess_control_item,
            di_preprocess_control_table,
        ) = preprocess_controller.preprocess_control_read(
            self.dynamodb_resource,
            self.pipeline_name,
            self.env,
            self.process_id,
            self.source_from_date,
            self.source_to_date,
        )
        if di_preprocess_control_item == {}:
            logger.info("DI PreProcess Control Item is Empty")
            di_preprocess_control_item = (
                preprocess_controller.preprocess_control_create(
                    pipeline_name=self.pipeline_name,
                    di_preprocess_control_table=di_preprocess_control_table,
                    source_from_date=self.source_from_date,
                    source_to_date=self.source_to_date,
                    source_time_zone=source_time_zone,
                    process_execution_id=self.process_id
                )
            )

        process_execution_id = di_preprocess_control_item["UUID"]
        (
            di_preprocess_exec_status_item,
            di_preprocess_exec_status_table,
        ) = preprocess_controller.preprocess_exec_status_read(
            self.dynamodb_resource, self.env, process_execution_id
        )

        if di_preprocess_exec_status_item == {} or self.run_process == "RESUME" or self.run_process == "RESTART":
            logger.info("DI PreProcess Execution Status Item is Empty")
            preprocess_controller.preprocess_exec_status_create(
                pipeline_name=self.pipeline_name,
                job_name=self.job_name,
                process_execution_id=process_execution_id,
                run_process=self.run_process,
                di_preprocess_control_item=di_preprocess_control_item,
                di_preprocess_exec_status_table=di_preprocess_exec_status_table,
                pipeline_exec_date= self.pipeline_exec_date,
                source_to_date=self.source_to_date,
                job_run_id= self.job_run_id,
            )

            di_preprocess_exec_status_tables = {}
        else:
            di_preprocess_exec_status_tables = di_preprocess_exec_status_item.get(
                "TABLES", {}
            )
        return (
            di_preprocess_control_item,
            di_preprocess_control_table,
            di_preprocess_exec_status_table,
            di_preprocess_exec_status_tables,
            process_execution_id,
        )

    def get_preprocess_item(self, preprocess_controller):
        di_preprocess_item: dict = preprocess_controller.preprocess_read(
            self.dynamodb_resource, self.pipeline_name, self.env
        )
        if di_preprocess_item is None or not di_preprocess_item:
            exception_info =f"No PreProcessing Metadata found for {self.pipeline_name} in {self.env}"
            logger.info(exception_info)
            logger.error(exception_info)
            notification = Notification()
            message = exception_info
            notification.publish_sns(pipeline_name= self.pipeline_name, env= self.env , error=message ,message=message)
            sys.exit(message)

        return di_preprocess_item

    def complete_job(
        self,
        preprocess_controller: PreprocessController,
        di_preprocess_control_item: dict,
        di_preprocess_exec_status_table,
        process_execution_id: str,
        schema_name: str,
        src_nm: str,
        trigger_bucket: str,
        trigger_filename: str,
    ):
        """This function is used to complete the PreProcessing Pipeline Job

        Note:
            It updates the status of the process execution in the database, and then uploads a file
            to S3 to trigger the next step in the pipeline

        Args:
            preprocess_controller: a class that contains the methods to interact with the database
            di_preprocess_control_item: This is the name of the table that contains the control
                                        information for the 'preprocess'
            di_preprocess_exec_status_table: The name of the table in the database that stores the
                                             status of the preprocessing execution status
            process_execution_id: This is the unique identifier for the process execution
            schema_name: The name of the schema to be used for the table
            src_nm: The name of the source system
            trigger_bucket: The bucket where the trigger file will be placed
            trigger_filename: Format is
                              s3://rgc-landing-bucket/rgc/preprocess/trigger/PROCESS_EXECUTION_ID/SCHEMA/SRC_NM/SOURCE_DATE

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version

        Todo:
            - migrate trigger file to each complete_table function
        """

        preprocess_controller.preprocess_exec_status_update(
            "PULL_END",
            self.source_to_date,
            process_execution_id,
            di_preprocess_control_item,
            di_preprocess_exec_status_table,
            self.pipeline_exec_date,
        )
        trigger_bucket = trigger_bucket.replace(
            "s3_rgc_landing_bucket", self.get_s3_rgc_landing_bucket()
        )

        trigger_filename = (
            trigger_filename.replace("PROCESS_EXECUTION_ID", process_execution_id)
            .replace("SCHEMA", schema_name)
            .replace("SRC_NM", src_nm)
            .replace("SOURCE_DATE", str(self.source_date).replace(" ", ":"))
        )
        logger.info(f"trigger file:{trigger_filename}")
        self.s3_client.put_object(Bucket=trigger_bucket, Key=trigger_filename)
        # preprocess_controller.preprocess_exec_status_update(
        #     "PIPELINE_STATUS",
        #     "SUCCESS",
        #     process_execution_id,
        #     di_preprocess_control_item,
        #     di_preprocess_exec_status_table,
        # )

        error_count, tables = preprocess_controller.get_preprocess_exec_status_error_check(
            self.dynamodb_client,
            self.env,
            process_execution_id,
            self.pipeline_exec_date,
        )
        status = "SUCCESS" if error_count == 0 else "FAILURE"

        preprocess_controller.preprocess_exec_status_update(
            "PIPELINE_STATUS",
            status,
            process_execution_id,
            di_preprocess_control_item,
            di_preprocess_exec_status_table,
            self.pipeline_exec_date
        )

    def start_table(
        self,
        preprocess_controller: PreprocessController,
        di_preprocess_control_item,
        di_preprocess_exec_status_table,
        di_preprocess_exec_status_tables,
        driver,
        process_execution_id,
        schema_name,
        secret_name,
        src_type,
        table_json,
        table_name,
        sql_query,
    ) -> pyspark.sql.DataFrame:
        """This function is used to start one table in the group of tables for the PreProcessing Pipeline

        Note:
            It checks the status of the table in the DI PreProcess Exec Status table, and if it's not in a
            success or failure state, it updates the status to running and starts the table

        Args:
            preprocess_controller: the preprocess_controller object
            di_preprocess_control_item: This is the row from the DI PreProcess Control
                                        table that corresponds to the table we're processing
            di_preprocess_exec_status_table: The name of the table that will hold the status
                                             of the 'preprocess execution'
            di_preprocess_exec_status_tables: This is a dictionary of the tables that have been processed
            driver: the driver name, e.g. 'oracle'
            process_execution_id: The process execution id is the unique id for the process execution
            schema_name: The name of the schema in the source database
            secret_name: The name of the secret that contains the connection information for the source database
            src_type: The type of source data. This can be either 'oracle' or 'api'
            table: the table name
            table_name: The name of the table to be processed

        Returns:
            A PySpark DataFrame and an optional string containing the job control command

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
            2022-11-08      Rishab              Introduced conditional statement to invoke stage-A DMC
            2023-01-11      Rishab              Passing tgt_pth in dmc_sqs method  
        """
        exec_status_table = di_preprocess_exec_status_tables.get(table_name, None)
        if exec_status_table:
            status = exec_status_table["STATUS"]
            logger.info(f"Status: {status}")

        preprocess_controller.preprocess_exec_status_table_create(
            di_preprocess_control_item=di_preprocess_control_item,
            di_preprocess_exec_status_table=di_preprocess_exec_status_table,
            process_execution_id=process_execution_id,
            table_name=table_name,
            pipeline_exec_date=self.pipeline_exec_date,
        )

        start_time: str = datetime.now().strftime("%F %T")
        preprocess_controller.preprocess_exec_status_table_update(
            "START_DT_TM",
            start_time,
            di_preprocess_control_item,
            di_preprocess_exec_status_table,
            process_execution_id,
            table_name,
            self.pipeline_exec_date,
        )

        preprocess_controller.preprocess_exec_status_table_update(
            "STATUS",
            "RUNNING",
            di_preprocess_control_item,
            di_preprocess_exec_status_table,
            process_execution_id,
            table_name,
            self.pipeline_exec_date
        )

        data_frame = self.spark.createDataFrame(
            data=self.spark.sparkContext.emptyRDD(),
            schema=spark_types.StructType([]),
        )

        logger.info("Executing execute_jdbc_query...")
        logger.info(f"SQL Query:{sql_query}")
        sql_query = f"({sql_query}) A"
        
        bucket_name, key = self.get_bucket_key(self.dmc_src_tgt_pth)
        tgt_pth = f"s3://{bucket_name}/{key}"
        
        try:
            data_frame = self.execute_jdbc_query(secret_name, driver, sql_query)
            
            if self.dmc_flag == 'Y' and self.dmc_operation == 'SRC_ONLY/TGT_ONLY':
                try:
                    self.dmc_sqs(di_preprocess_control_item,target_path=tgt_pth,table_name=table_name,dmc_operation='SRC_ONLY')
                except Exception as e:
                    logger.info("Start Table!! Error sending DMC SQS Queue message for SRC_ONLY")
                    logger.error(e)
                    raise e
           #else:
           #    logger.info("Start Table!! DMC SQS Message not required.")
                    
        except Exception as e:
            preprocess_controller.preprocess_exec_status_table_update(
                "STATUS",
                "FAILURE",
                di_preprocess_control_item,
                di_preprocess_exec_status_table,
                process_execution_id,
                table_name,
                self.pipeline_exec_date
            )
            logger.info(
                f"Table Helper Error: {e}",
                extra=dict(schema_name=schema_name, table_name=table_name),
            )
            exception_info =f"Table Helper Error: {e}"
            logger.info(exception_info)
            logger.error(exception_info)

            notification = Notification()
            message = exception_info
            notification.publish_sns(pipeline_name=self.pipeline_name, env=self.env , error=message ,message=message)
            sys.exit(message)
            # raise Exception(exception_info)
            logger.error(exception_info, exc_info=True)
        return data_frame

    def complete_table(
        self,
        df_source_data,
        preprocess_controller: PreprocessController,
        di_preprocess_control_item,
        di_preprocess_control_table,
        di_preprocess_exec_status_table,
        process_execution_id,
        schema_name,
        source_date,
        src_nm,
        table_name,
        target_path,
    ):
        """This function is used to complete one table in the group of tables for the PreProcessing Pipeline Job

        Note:
            It takes a dataframe, updates a few tables in a database, and then writes the dataframe to a parquet file.

        Args:
            df_source_data: The dataframe that contains the data to be written to the target
            preprocess_controller: This is the DataIngestion object that we created earlier
            di_preprocess_control_item: This is the item in the preprocess_control table that we're working on
            di_preprocess_control_table: This is the table that contains the control information for the 'preprocess'
            di_preprocess_exec_status_table: This is the table that stores the status of the 'preprocess execution'
            process_execution_id: This is the unique id for the process execution
            schema_name: The name of the schema in the source database
            source_date: The date of the data you're pulling
            src_nm: The source name
            table_name: The name of the table to be processed
            target_path: The path where the data will be written to

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
            2022-09-20      Surajit Mandal      Add write block size
            2022-11-08      Rishab              Introduced conditional statement to invoke stage-A DMC
        """

        # TO DO: get the source count from the Database instead of getting from the DF count
        preprocess_controller.preprocess_exec_status_table_update(
            "SRC_CNT",
            df_source_data.count(),
            di_preprocess_control_item,
            di_preprocess_exec_status_table,
            process_execution_id,
            table_name,
            self.pipeline_exec_date,
        )
        preprocess_controller.preprocess_exec_status_table_update(
            "TGT_CNT",
            df_source_data.count(),
            di_preprocess_control_item,
            di_preprocess_exec_status_table,
            process_execution_id,
            table_name,
            self.pipeline_exec_date
        )

        bucket_name, key = self.get_bucket_key(target_path)
        target_path = f"s3://{bucket_name}/{key}"

        tgt_path_write = f"{target_path}{table_name}"
        logger.info("bucket-{}, key ={}, tgt_path={}, path_write={}".format(bucket_name,key,target_path,tgt_path_write))
        spark_config_parquet_block_size = self.spark.sparkContext.getConf().get("spark.hadoop.parquet.block.size")
        logger.info(f"Block size received from configuration: {spark_config_parquet_block_size}")
        df_source_data.write.option("parquet.block.size",spark_config_parquet_block_size).option("mergeSchema", "true")\
            .parquet(tgt_path_write, mode=self.dataframe_write_mode)
        current_date_time: str = datetime.now().strftime("%F %T")
        
        if self.dmc_flag == 'Y' and self.dmc_operation == 'SRC_ONLY/TGT_ONLY':
            try:
                self.dmc_sqs(di_preprocess_control_item,target_path,table_name,dmc_operation='TGT_ONLY')  
            except Exception as e:
                logger.info(f"Complete Table!! DMC Operation : {self.dmc_operation}.Error sending DMC SQS Queue message")
                logger.error(e)
                raise e
        elif self.dmc_flag == 'Y' and self.dmc_operation == 'ALL':
            try:
                self.dmc_sqs(di_preprocess_control_item,target_path,table_name,dmc_operation='ALL')
            except Exception as e:
                logger.info(f"Complete Table!! DMC Operation : {self.dmc_operation}.Error sending DMC SQS Queue message")
                logger.error(e)
                raise e
        else:
            logger.info("DMC_FLAG is disabled")
        # preprocess_controller.preprocess_control_update(
        #     "PULL_END",
        #     self.source_date,
        #     self.pipeline_name,
        #     di_preprocess_control_item,
        #     di_preprocess_control_table,
        #     self.pipeline_exec_date,
        # )
        preprocess_controller.preprocess_exec_status_table_update(
            "END_DT_TM",
            current_date_time,
            di_preprocess_control_item,
            di_preprocess_exec_status_table,
            process_execution_id,
            table_name,
            self.pipeline_exec_date,
        )
        preprocess_controller.preprocess_exec_status_table_update(
            "STATUS",
            "SUCCESS",
            di_preprocess_control_item,
            di_preprocess_exec_status_table,
            process_execution_id,
            table_name,
            self.pipeline_exec_date,
        )

    def get_s3_rgc_landing_bucket(self) -> str:
        """This function returns the name of the S3 bucket that contains the data that will be written to

        Note:
            It gets the value of the `s3_rgc_landing_bucket` parameter from the `odp-regional-params` DynamoDB table

        Returns:
            The string value of the s3_rgc_landing_bucket

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        params_table = self.dynamodb_resource.Table("odp-regional-params")
        results = params_table.get_item(Key={"logicalName": "s3_rgc_landing_bucket"})
        return results["Item"]["value"]

    def table_helper(
        self,
        driver: str,
        schema_name: str,
        table_name: str,
        secret_name: str,
        table: dict,
    ) -> pyspark.sql.DataFrame:
        """This function is used to get the data from the source database and return it as a dataframe

        Note:
            Override this function in your subclass if you want to do something different with the data.

        Args:
            driver: The driver name, e.g. 'Oracle'
            schema_name: The name of the schema to create the table in
            table_name: The name of the table
            secret_name: The name of the secret that contains the connection string to the database
            table: a dictionary of the table's properties

        Returns:
            pyspark.sql.dataframe.DataFrame: The dataframe that contains the data to be written to the target

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        raise Exception("table_helper is not implemented")

    def get_source_date(
        self,
        source_date_column: str,
        driver_name: str,
        schema_name: str,
        ctrl_table_name: str,
        frequency: str,
        secret_name: str,
    ) -> str:
        """This function returns the max date from the table and the current date as the source to date.

        driver_name: name of the database driver
        schema_name: The name of the schema
        ctrl_table_name: The name of the control table
        source_date_col: name of the source from date column in control table
        frequency: EOM, EOD or INTRADAY
        secret_name: The name of the secret that contains the connection string to the database

        Returns:
            The max source_to_date in the table and the current date.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
            2022-09-15      Surajit Mandal      Add EOM Functionality
        """
        if frequency == "EOM":
            return (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
        elif frequency == "EOD":
            return datetime.now().strftime("%Y-%m-%d")
        elif frequency == "INTRADAY":
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
        """This function returns the max date from the table and the current date as the source to date.

        source_from_date: is the string date which would have passed to this function from preprocess-control table
        driver_name: name of the database driver
        schema_name: The name of the schema
        ctrl_table_name: The name of the control table
        source_from_date_col: name of the source from date column in control table
        frequency: EOM, EOD or INTRADAY
        secret_name: The name of the secret that contains the connection string to the database

        Returns:
            The max source_to_date in the table and the current date.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
            2022-09-15      Surajit Mandal      Add EOM Functionality
        """
        if frequency == "EOM":
            dt = (datetime.now(pytz.timezone(source_time_zone)).replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
        elif frequency == "EOD":
            dt = datetime.now(pytz.timezone(source_time_zone)).strftime("%Y-%m-%d")
        elif frequency == "INTRADAY":
            dt = datetime.now(pytz.timezone(source_time_zone)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        return dt, dt

    def get_post_date_query(self, schema_name: str, table_name: str) -> str:
        # TODO this is for pull date
        """This function returns the max date from the table and the current date as the source to date.

        schema_name: The name of the schema where the table is located
        table_name: The name of the table you want to extract data from

        Note:
            If we pass schema, table, column, we want to extract the source_to_date from that. If we are not passing anything,then have the source_to_date as current date
            When setting to current date, we need to check the pre-process table, look for FREQ if set to I, source_to_date should be timestamp
            source_from_date should be coming from preprocess_control table's prev record. When there is no previous record, we would set this to 1900-01-01
            source_date should ideally be set to system_date / timestamp based on FREQ.

        Returns:
            The max date in the table and the current date.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        query = f"""
                    SELECT max(AS_OF_DT) AS SOURCE_FROM_DATE,
                           CURRENT_DATE AS SOURCE_TO_DATE
                    FROM {schema_name}.{table_name}
                """
        return query

    def get_tablename_query_list(
        self,
        tables,
        didc_tables,
        env,
        schema_name,
        source_name,
        source_from_date,
        source_to_date,
        source_date,
        target_path,
    ):
        """This function returns a List which contains table_name and query from Process control table

        Args:
           tables: Table list dictionary from preprocess table
           didc_tables: list of didc tables
           env: Envieonment
           schema_name: name of the schema
           source_name: source name
           source_from_date: source from date
           source_to_date: source to date
           source_date: source date

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        # TO DO Formatting timestamp as per the database
        for x in range(len(tables)):
            json_list = []
            if type(tables[x]) == dict:
                for key, val in tables[x].items():
                    TABLE_NAME = key
                    if TABLE_NAME in didc_tables and val["ACTIVE"] == "Y":
                        query = (
                            val["QUERY"]
                            .replace("ENV", env)
                            .replace("SCHEMA", schema_name)
                            .replace(
                                "SOURCE_FROM_DATE",
                                f"{source_from_date}",
                            )
                            .replace(
                                "SOURCE_TO_DATE",
                                f"{source_to_date}",
                            )
                        )
                        target_path = (
                            target_path.replace("ENV", env)
                            .replace("SRC_NM", source_name)
                            .replace("SCHEMA", schema_name)
                            .replace("SOURCE_DATE", str(source_date).replace(" ", ":"))
                        )
                        data_set = {
                            "TABLE_NAME": TABLE_NAME,
                            "QUERY": query,
                            "TGT_PATH": target_path,
                        }
                        json_data = json.dumps(data_set)
                        json_list.append(json_data)
        return list(json_list)

    @staticmethod
    def get_increment_query(
        insert_query: str, source_from_date: str, source_to_date: str
    ) -> str:
        """This function returns the query to get the data from the source database that is newer than the last

        Note:
            It takes a query and a date range, and returns a new query that is the same as the original query,
            but with a date range filter added to it
            For Oracle Source, the PreProcess static query should be written with to_date function so that just the string replacement will work

        Args:
            insert_query: The query that you want to run to insert data into the target table
            source_from_date: The date from which you want to start pulling data
            source_to_date: The date to which you want to increment the data

        Returns:
            The query is being returned.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        insert_query = (
            re.sub(r"\s+", " ", insert_query)
            .strip()
            .upper()
            .replace("SOURCE_FROM_DATE", source_from_date)
            .replace("SOURCE_TO_DATE", source_to_date)
        )
        return re.sub(r"\s+", " ", insert_query).strip()

    @staticmethod
    def get_secret(secrets_client, secret_name: str):
        """
        This fetches the secrets from AWS Secrets Manager

        :param secrets_client:
        :param secret_name:
        :return:

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """

        try:
            get_secret_value_response = secrets_client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            """
            Error Code Examples:
                ExpiredTokenException: The provided token is expired.
                DecryptionFailureException: Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                InternalServiceErrorException: An error occurred on the server side.
                InvalidParameterException: You provided an invalid value for a parameter.
                InvalidRequestException: You provided a parameter value that is not valid for the current state of the resource.
                ResourceNotFoundException: We can't find the resource that you asked for.
            """
            logger.info(f"Error Code: {e.response['Error']['Code']}")
            logger.error(f"Error Code: {e.response['Error']['Code']}")
            raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if "SecretString" in get_secret_value_response:
                return json.loads(get_secret_value_response["SecretString"])
            else:
                return base64.b64decode(get_secret_value_response["SecretBinary"])

    def didc_validation(self, pipeline_tables: list, didc_tables: list):
        """This method returns the True or False by checking the pipeline-tables in DIDC tables.

        pipeline_name : name of the pipeline
        env: Environment Name e.g. dev, qa
        pipeline_tables: List of pipeline tables
        didc_tables: List of DIDC tables

        Returns:
           True when the list of pipeline-tables match with DIDC tables
           False when the list of pipeline-tables doesn't match with DIDC tables

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        validation_flag = True
        tables_not_found = []
        di_preprocess_item: dict = self.preprocess_controller.preprocess_read(
            self.dynamodb_resource, self.pipeline_name, self.env
        )
        logger.info(f"pipeline_tables:{str(pipeline_tables)}")
        for x in range(len(pipeline_tables)):
            if type(pipeline_tables[x]) == dict:
                for key, val in pipeline_tables[x].items():
                    TABLE_NAME = key
                    if TABLE_NAME in didc_tables:
                        pass
                    else:
                        validation_flag = False
                        tables_not_found.append(TABLE_NAME)
        if validation_flag == False:
            raise Exception(
                f"Stage-A Table(s) have not been found in DIDC List of tables: {tables_not_found}"
            )

    def execute_jdbc_query(self, secret_name, jdbc_driver, sql_query):
        """This function returns the DF by executing the SQL query.

        Args:
            secret_name : AWS Secret which contains the JDBC url, username and password
            jdbc_driver: the driver name, e.g. 'Oracle'
            sql_query: SQL query which is pushed down to database

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version

        """
        raise Exception("execute_jdbc_query is not implemented")

    def get_bucket_key(self, location):
        """This function returns bucket_name, key.

        Args:
            location: This function does the code remidiation to support multiple regions

        Returns:
            bucket_name: Name of the bucket
            key : S3 key

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version

        """
        path_parts = location.replace("s3://", "").split("/")
        bucket_name = path_parts.pop(0)
        bucket_name = get_name_ddb(bucket_name)
        key = "/".join(path_parts)
        return bucket_name, key

    def get_pipeline_failed_count(slef, response) -> int:
        """This function gets the list of failed table count
        Args:
            response: response of tables json from DI-PREPROCESS-EXEC-STATUS
        Returns:
            the count of failed tables

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        count = 0
        for x in range(len(response)):
            if type(response[x]) == dict:
                for key, val in response[x].items():
                    if type(val) == dict:
                        for key_1, val_1 in val.items():
                            if key_1 == "STATUS" and val_1 == "FAILURE":
                                count = count + 1
                            else:
                                count = count + 0
        return count

    def set_process_exec_id(self) -> None:
        """This function sets the source_date to the instance variable source_date
        Args:
            None
        Returns:
            None

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        self.process_id = str(uuid.uuid4())

    def set_source_from_date(self, source_from_date) -> None:
        """This function sets the source_date to the instance variable source_date
        Args:
            source_date: source_date has been passed
        Returns:
            None

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        self.source_from_date = source_from_date

    def set_pipeline_exec_date(self, pipeline_exec_date) -> None:
        """This function sets the pipeline_exec_date to the instance variable process_exec_date
        Args:
            pipeline_exec_date: pipeline_exec_date has been passed
        Returns:
            None

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        self.pipeline_exec_date = pipeline_exec_date

    def set_boundary_date(self, source_date: str, source_to_date: str) -> None:
        """This function sets the source_date, source_to_date to the instance variables source_date and source_to_date
        Args:
            source_date: source_date, source_to_date
        Returns:
            None

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        self.source_date = source_date
        self.source_to_date = source_to_date


class DatabaseRunner(Runner, metaclass=Instrumentation):
    """
    This class is a subclass of the Runner class and is used to ingest data from a Database

    Modification History :
        ================================================
        Date			Modified by			Description
        2022-08-25      Rajashekar Kolli    Initial Version
        2022-10-13      Rishab              Add SQS client and dmc sqs queue
    """

    def __init__(
        self,
        spark_session: pyspark.sql.session.SparkSession,
        glue_context: awsglue.context.GlueContext,
        args: dict,
        dynamodb_resource,
        dynamodb_client,
        secrets_client,
        s3_client,
        sqs_client = None,
    ):
        """This function is used to initialize the class and set the variables that will be used throughout the class

        Args:
            spark_session: The Spark session that is created by Glue
            glue_context: The AWS Glue context object
            args: This is the dictionary of parameters passed to the job
            dynamodb_resource: This is the boto3 resource for DynamoDB
            dynamodb_client: boto3 client for DynamoDB
            secrets_client: This is the boto3 client for AWS Secrets Manager
            s3_client: This is the boto3 client for S3
            sqs_client: This is the boto3 client for SQS

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
            2022-10-13      Rishab              Created SQS Queue URL object
        """
        super().__init__(
            spark_session,
            glue_context,
            args,
            dynamodb_resource,
            dynamodb_client,
            secrets_client,
            s3_client,
            sqs_client = None,
        )
        self.dynamodb_resource = dynamodb_resource
        self.pipeline_name = args["PIPELINE_NAME"]
        self.env = args["JOB_NAME"].split("-")[1]
        
        if "dmc_sqs_queue" in args:
            self.dmc_sqs_queue = args["dmc_sqs_queue"]
        else:
            self.dmc_sqs_queue = ""
            
        self.preprocess_controller = PreprocessController()
        self.di_preprocess_item: dict = self.preprocess_controller.preprocess_read(
            self.dynamodb_resource, self.pipeline_name, self.env
        )

    def table_helper(
        self,
        driver: str,
        schema_name: str,
        table_name: str,
        secret_name: str,
        table: dict,
    ) -> pyspark.sql.DataFrame:
        """This function is used to get the data from the source Database and return it as a dataframe

        Note:
            This function is overriding the table_helper function in the Runner class.

        Args:
            driver: the driver name, e.g. 'Oracle'
            schema_name: the name of the database schema to fetch from
            table_name: the name of the table to fetch from
            secret_name: the name of the secret that contains the connection string to the database
                         as well as credential information
            table: the boto3 table object

        Returns:
            pyspark.sql.dataframe.DataFrame: The dataframe that contains the data to be written to the target

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """
        logger.info(table)
        post_date_query = self.get_post_date_query(schema_name, table_name)
        insert_query = table[table_name]["QUERY"].upper()

        secret = self.get_secret(self.secrets_client, secret_name)
        conn_url = secret.get("conn_url")
        username = secret.get("user")
        password = secret.get("password")

        data_frame_post_date = (
            self.spark.read.format("jdbc")
            .option("url", conn_url)
            .option("query", post_date_query)
            .option("user", username)
            .option("password", password)
            .option("driver", driver)
            .load()
        )
        # TODO get from parameters
        source_date, source_from_date, source_to_date = data_frame_post_date.collect()[
            0
        ]
        increment_query = self.get_increment_query(
            insert_query, source_from_date, source_to_date
        )
        df_source_data = (
            self.glue_context.read.format("jdbc")
            .option("url", conn_url)
            .option("query", increment_query)
            .option("user", username)
            .option("password", password)
            .option("driver", driver)
            .load()
        )

        logger.info("df_source_data type")
        logger.info(type(df_source_data))
        return df_source_data

    def get_source_date(
        self,
        source_date_col: str,
        driver_name: str,
        schema_name: str,
        ctrl_table_name: str,
        frequency: str,
        secret_name: str,
    ) -> str:
        """This function returns the sourde date (post_date) from the source control table.

        Returns:
            The max date in the table and the current date.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
            2022-09-15      Surajit Mandal      Add EOM Functionality
        """

        if schema_name and ctrl_table_name and source_date_col:
            sql_query = f"(select max({source_date_col}) as POST_DATE from {schema_name}.{ctrl_table_name}) A"
            df_post_query = self.execute_jdbc_query(secret_name, driver_name, sql_query)
            return df_post_query.collect()[0][0]
        else:
            if frequency == "EOM":
                return (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
            if frequency == "EOD":
                return datetime.now().strftime("%Y-%m-%d")
            elif frequency == "INTRADAY":
                return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return ""

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
        """This function returns the max date from the table and the current date as the source to date.

        source_date_col: name of the source date column
        source_to_date_col: name of the source from date column
        driver_name: name of the database driver
        schema_name: The name of the schema
        ctrl_table_name: The name of the control table
        source_from_date_col: name of the source from date column in control table
        frequency: EOM, EOD or INTRADAY
        secret_name: The name of the secret that contains the connection string to the database


        Returns:
            The max source_to_date in the table and the current date.

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
            2022-09-15      Surajit Mandal      Add EOM Functionality
        """

        if schema_name and ctrl_table_name:
            sql_query = f"(select max({source_date_col}) POST_DATE, max({source_to_date_col}) as SOURCE_TO_DATE from {schema_name}.{ctrl_table_name} alias ) A"
            logger.info(f"sql-query:{sql_query},secret_name:{secret_name}")
            df_post_query = self.execute_jdbc_query(secret_name, driver_name, sql_query)
            query_result = df_post_query.collect()
            return query_result[0][0], query_result[0][1]
        else:
            if frequency == "EOM":
                dt = (datetime.now(pytz.timezone(source_time_zone)).replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
            elif frequency == "EOD":
                dt = datetime.now(pytz.timezone(source_time_zone)).strftime("%Y-%m-%d")
            elif frequency == "INTRADAY":
                dt = datetime.now(pytz.timezone(source_time_zone)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            return dt, dt

    def execute_jdbc_query(self, secret_name, jdbc_driver, sql_query):
        """This function returns the DF by executing the SQL query.

        Args:
            secret_name : AWS Secret which contains the JDBC url, username and password
            jdbc_driver: the driver name, e.g. 'Oracle'
            sql_query: SQL query which is pushed down to database

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
            2022-09-19      Surajit Mandal      Add JDBC query fetchsize related changes

        """

        secret = self.get_secret(self.secrets_client, secret_name)
        conn_url = secret.get("conn_url")
        username = secret.get("user")
        password = secret.get("password")
        fetch_size = self.spark.sparkContext.getConf().get("spark.hadoop.fetchsize")
        logger.info(f"Fetch size received from configuration: {fetch_size}")
        if fetch_size is not None:
            return (
                self.spark.read.format("jdbc")
                .option("url", conn_url)
                .option("dbtable", sql_query)
                .option("user", username)
                .option("password", password)
                .option("driver", jdbc_driver)
                .option("fetchsize", fetch_size)
                .load()
            )
        else:
            return (
                self.spark.read.format("jdbc")
                    .option("url", conn_url)
                    .option("dbtable", sql_query)
                    .option("user", username)
                    .option("password", password)
                    .option("driver", jdbc_driver)
                    .load()
            )


class APIRunner(Runner, metaclass=Instrumentation):
    """
    This class is a subclass of the Runner class, is instrumented, and is used to ingest data from an API

    Modification History :
        ================================================
        Date			Modified by			Description
        2022-08-25      Rajashekar Kolli    Initial Version
    """

    def table_helper(
        self,
        driver: str,
        schema_name: str,
        table_name: str,
        secret_name: str,
        table: dict,
    ) -> pyspark.sql.DataFrame:
        """This function is used to get the data from the source API and return it as a dataframe

        Note:
            This function is overriding the table_helper function in the Runner class.

        Args:
            driver: the driver name, e.g. 'Oracle'
            schema_name: the name of the database schema to fetch from
            table_name: the name of the table to fetch from
            secret_name: the name of the secret that contains the connection string to the database
                         as well as credential information
            table: the boto3 table object

        Returns:
            pyspark.sql.dataframe.DataFrame: The dataframe that contains the data to be written to the target

        Modification History :
            ================================================
            Date			Modified by			Description
            2022-08-25      Rajashekar Kolli    Initial Version
        """

class FlatFileRunner(Runner, metaclass=Instrumentation):
    """This class is a subclass of the Runner class, is instrumented, and is used to ingest data from a FlatFile"""


class KafkaRunner(Runner, metaclass=Instrumentation):
    """This class is a subclass of the Runner class, is instrumented, and is used to ingest data from a Kafka Stream"""
