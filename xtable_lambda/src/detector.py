# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import os
import re
from typing import Any, Dict

import boto3
from aws_lambda_powertools.utilities.typing import LambdaContext

from models import (
    ConversionTask,
    DatasetConfig,
    CatalogConfig,
    Dataset,
    CatalogOptions,
    S3Path,
)

# clients
glue_client = boto3.client("glue")
lambda_client = boto3.client("lambda")

# vars
CONVERTER_FUNCTION_NAME = os.environ["CONVERTER_FUNCTION_NAME"]


def get_convertable_tables():
    """
    Generator for all Glue tables in this accounts Glue Catalog
    Yields:
        dict: table dict
    """
    # Get paginator for databases
    databases = glue_client.get_paginator("get_databases").paginate()
    for database_list in databases:
        database_list = database_list["DatabaseList"]
        for database in database_list:
            # Get paginator for tables
            tables = glue_client.get_paginator("get_tables").paginate(
                DatabaseName=database["Name"]
            )
            for table_list in tables:
                table_list = table_list["TableList"]
                for table in table_list:
                    # if required table parameters exist
                    if {"xtable_target_formats", "xtable_table_type"} <= table["Parameters"].keys():
                        yield table


def handler(event: Dict[str, Any], context: LambdaContext):
    # iterate over databases and tables in Glue
    for table in get_convertable_tables():
        # vars
        s3_path = S3Path(table["StorageDescriptor"]["Location"])

        # create task payload
        task = ConversionTask(
            dataset_config=DatasetConfig(
                sourceFormat=table["Parameters"]["xtable_table_type"].upper(),
                targetFormats=re.findall(
                    r"\w+",
                    table["Parameters"]["xtable_target_formats"].upper(),
                ),
                datasets=[
                    Dataset(
                        tableBasePath=f"{s3_path}",
                        tableName=f"{s3_path.name}_converted",
                        namespace=f"{s3_path.parent.name}",
                    )
                ],
            )
        )

        # if ICEBERG add Glue Table Warehouse registration
        if "ICEBERG" in task.dataset_config.targetFormats:
            task.catalog_config = CatalogConfig(
                catalogOptions=CatalogOptions(warehouse=f"{s3_path}")
            )

        # send to converter lambda
        lambda_client.invoke(
            FunctionName=CONVERTER_FUNCTION_NAME,
            InvocationType="Event",
            Payload=task.model_dump_json(by_alias=True),
        )
