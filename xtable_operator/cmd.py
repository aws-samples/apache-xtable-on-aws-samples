# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import jpype
import jpype.imports
import jpype.types
from pathlib import Path
import platform

def sync(
    config_path: Path,
    jars: Path,
    catalog_path: Path = None,
    java11: Path = Path(
        f"/usr/lib/jvm/java-11-amazon-corretto.{platform.machine()}/lib/server/libjvm.so"
    ),
):
    """Sync a dataset metadata from source to target format. Optionally writes it to a catalog.

    Args:
        config_path (Path): Path to the dataset config file.
        jars (Path): Path to the jars folder.
        catalog_path (Path, optional): Path to the catalog config file. Defaults to None.
        java11 (Path, optional): Path to the java 11 library. Defaults to Path(
            f"/usr/lib/jvm/java-11-amazon-corretto.{platform.machine()}/lib/server/libjvm.so"
        ).
    
    """

    # start a jvm in the background
    jpype.startJVM(java11.absolute().as_posix(), classpath=jars / "*")
    run_sync = jpype.JPackage("io").onetable.utilities.RunSync.main

    # call java class with or without catalog config
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

    # shutdown
    jpype.shutdownJVM()
