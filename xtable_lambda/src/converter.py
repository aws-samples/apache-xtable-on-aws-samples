# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import boto3
from pathlib import Path

from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.parser import event_parser

import xtable
from models import ConversionTask

# clients
lambda_client = boto3.client("lambda")


@event_parser(model=ConversionTask)
def handler(event: ConversionTask, context: LambdaContext):

    # sync with passed Dataset Config
    if "ICEBERG" not in event.dataset_config.targetFormats:

        xtable.sync(
            dataset_config=event.dataset_config,
            tmp_path=Path("/tmp"),
            jars=[
                Path(__file__).resolve().parent / "jars/*",
            ],
        )

    # if iceberg is target also register the table in the glue data catalog
    else:
        # sync with passed Dataset and Catalog Config
        xtable.sync(
            dataset_config=event.dataset_config,
            catalog_config=event.catalog_config,
            tmp_path=Path("/tmp"),
            jars=[
                Path(__file__).resolve().parent / "jars/*",
                Path(__file__).resolve().parent / "jars_iceberg/*",
            ],
        )
