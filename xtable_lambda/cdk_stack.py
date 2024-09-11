# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct


class XtableLambda(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Converter
        converter = _lambda.DockerImageFunction(
            scope=self,
            id="Converter",
            memory_size=10240,
            architecture=_lambda.Architecture.ARM_64,
            code=_lambda.DockerImageCode.from_image_asset(
                directory="src", cmd=["converter.handler"]
            ),
            timeout=Duration.minutes(15),
            environment={
                "SPARK_LOCAL_IP": "127.0.0.1",
            },
        )
        converter.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetTables",
                    "glue:GetTable",
                    "glue:GetDatabases",
                    "glue:GetDatabase",
                    "glue:UpdateDatabase",
                    "glue:CreateDatabase",
                    "glue:UpdateTable",
                    "glue:CreateTable",
                    "s3:Abort*",
                    "s3:DeleteObject*",
                    "s3:GetBucket*",
                    "s3:GetObject*",
                    "s3:List*",
                    "s3:PutObject",
                    "s3:PutObjectLegalHold",
                    "s3:PutObjectRetention",
                    "s3:PutObjectTagging",
                    "s3:PutObjectVersionTagging",
                ],
                resources=["*"],
            )
        )

        # Detector
        detector = _lambda.DockerImageFunction(
            scope=self,
            id="Detector",
            architecture=_lambda.Architecture.ARM_64,
            memory_size=1024,
            code=_lambda.DockerImageCode.from_image_asset(
                directory="src", cmd=["detector.handler"]
            ),
            timeout=Duration.minutes(3),
            environment={"CONVERTER_FUNCTION_NAME": converter.function_name},
        )
        converter.grant_invoke(detector)
        detector.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:GetTables", "glue:GetDatabases"],
                resources=["*"],
            )
        )

        # Scheduled events
        event = events.Rule(
            scope=self,
            id="DetectorSchedule",
            schedule=events.Schedule.rate(Duration.hours(1)),
        )
        event.add_target(targets.LambdaFunction(detector))
