# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import re
from typing import List, Literal, Optional
from pydantic import BaseModel, Field
from pathlib import PurePath


class Dataset(BaseModel):
    tableBasePath: str
    tableName: str
    namespace: Optional[str] = None
    partitionSpec: Optional[str] = None


class DatasetConfig(BaseModel):
    sourceFormat: Literal["DELTA", "ICEBERG", "HUDI"]
    targetFormats: List[Literal["DELTA", "ICEBERG", "HUDI"]]
    datasets: List[Dataset]


class CatalogOptions(BaseModel):
    warehouse: str
    catalog_impl: str = Field(
        alias="catalog-impl", default="org.apache.iceberg.aws.glue.GlueCatalog"
    )
    io_impl: str = Field(alias="io-impl", default="org.apache.iceberg.aws.s3.S3FileIO")


class CatalogConfig(BaseModel):
    catalogImpl: str = Field(default="org.apache.iceberg.aws.glue.GlueCatalog")
    catalogName: str = Field(default="glue")
    catalogOptions: CatalogOptions


class ConversionTask(BaseModel):
    dataset_config: DatasetConfig
    catalog_config: Optional[CatalogConfig] = None


class S3Path(PurePath):
    s3_uri_regex = re.compile(r"s3://(?P<path>.+)")
    path_regex = re.compile(r"/*(?P<path>.+)")
    drive = "s3://"

    def __init__(self, path: str):
        s3_match = re.match(self.s3_uri_regex, path)
        path_match = re.match(self.path_regex, path)
        if s3_match:
            super().__init__(s3_match.group("path"))
        elif path_match:
            super().__init__(path_match.group("path"))
