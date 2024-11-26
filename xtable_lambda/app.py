# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#!/usr/bin/env python3
import os

import aws_cdk as cdk
from aws_cdk import Aspects
from cdk_nag import AwsSolutionsChecks


from cdk_stack import XtableLambda


app = cdk.App()
XtableLambda(
    app,
    "XtableLambda",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")
    ),
)
Aspects.of(app).add(AwsSolutionsChecks(verbose=True))

app.synth()
