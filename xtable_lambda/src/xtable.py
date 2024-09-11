# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import platform
from pathlib import Path
from typing import List

import yaml
import jpype
import jpype.imports
import jpype.types

from models import DatasetConfig, CatalogConfig

def sync(
    dataset_config: DatasetConfig,
    tmp_path: Path,
    catalog_config: CatalogConfig = None,
    java11: Path = Path(
        f"/usr/lib/jvm/java-11-amazon-corretto.{platform.machine()}/lib/server/libjvm.so"
    ),
    jars: List[Path] = [Path(__file__).resolve().parent / "jars/*"],
):
    """
    Sync a dataset metadata from source to target format. Optionally writes it to a catalog.

    Args:
        dataset_config (dict): Dataset configuration.
        tmp_path (Path): Path to a temporary directory.
        catalog_config (dict, optional): Iceberg catalog configuration. Defaults to None.
        java11 (Path, optional): Path to the java 11 library. Defaults to Path(
            f"/usr/lib/jvm/java-11-amazon-corretto.{platform.machine()}/lib/server/libjvm.so"
        ).
        jars (Path(s), optional): Path(s) to the jar files. Defaults to Path(__file__).resolve().parent / "jars".

    Returns:
        None

    """

    # write config file
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as file:
        yaml.dump(dataset_config.model_dump(by_alias=True), file)

    # write catalog file
    if catalog_config:
        catalog_path = tmp_path / "catalog.yaml"
        with catalog_path.open("w") as file:
            yaml.dump(catalog_config.model_dump(by_alias=True), file)
    else:
        catalog_path = None

    # start a jvm in the background
    if jpype.isJVMStarted() is False:
        jpype.startJVM(java11.absolute().as_posix(), classpath=jars)

    # call java class with or without catalog config
    run_sync = jpype.JPackage("org").apache.xtable.utilities.RunSync.main
    if catalog_path:
        run_sync(
            [
                "--datasetConfig",
                config_path.absolute().as_posix(),
                "--icebergCatalogConfig",
                catalog_path.absolute().as_posix(),
            ]
        )

    else:
        run_sync(["--datasetConfig", config_path.absolute().as_posix()])
