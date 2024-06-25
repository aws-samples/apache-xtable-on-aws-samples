# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from pathlib import Path
import yaml

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from xtable_operator import cmd


class XtableOperator(BaseOperator):
    """
    Xtable Operator

    This Operator is meant to be executed in an Amazon MWAA or Airflow environment. It capsulates the Apache Xtable java library calls.
    """

    @apply_defaults
    def __init__(
        self,
        dataset_config: dict,
        tmp_path: Path,
        iceberg_catalog_config: dict = None,
        java11: Path = None,
        *args,
        **kwargs
    ):
        """
        Initialize all the relevant paths. We need the xtable dataset and catalog configurations, 
        as well as a writeable temporary path to generate the config files on the fly. For executing the 
        xtable command, we need the java 11 library in the jars folder.

        Args:
            dataset_config (dict): Dataset configuration.
            tmp_path (Path): Path to the temporary folder.
            iceberg_catalog_config (dict, optional): Iceberg catalog configuration. Defaults to None.
            java11 (Path, optional): Path to the java 11 library. Defaults to Path(
                f"/usr/lib/jvm/java-11-amazon-corretto.{platform.machine()}/lib/server/libjvm.so"
            ).
        """
        super(XtableOperator, self).__init__(*args, **kwargs)

        # write config file
        config_path = tmp_path / "config.yaml"
        with config_path.open("w") as file:
            yaml.dump(dataset_config, file)

        # write catalog file
        if iceberg_catalog_config:
            catalog_path = tmp_path / "catalog.yaml"
            with catalog_path.open("w") as file:
                yaml.dump(iceberg_catalog_config, file)
        else:
            catalog_path = None

        # self
        self.config_path: Path = config_path
        self.catalog_path: Path = catalog_path
        self.jars: Path = Path(__file__).resolve().parent / "jars"
        self.java11: Path = java11

    def execute(self, context):
        """Execute the xtable command."""
        cmd.sync(
            config_path=self.config_path,
            catalog_path=self.catalog_path,
            jars=self.jars,
        )
