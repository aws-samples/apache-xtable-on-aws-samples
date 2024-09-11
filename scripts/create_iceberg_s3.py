import os
from pyspark.sql import SparkSession


# env
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# session
spark = (
    SparkSession.builder.appName("MyApp")
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.spark:spark-hadoop-cloud_2.12:3.4.3,software.amazon.awssdk:bundle:2.26.21,software.amazon.awssdk:url-connection-client:2.26.21",
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config(
        "spark.sql.catalog.spark_catalog.warehouse",
        os.environ["ICEBERG_WAREHOUSE_PATH"], # e.g. s3://data/
    )
    .config(
        "spark.sql.catalog.spark_catalog.type",
        "hadoop",
    )
    .config(
        "spark.sql.defaultCatalog",
        "spark_catalog",
    )
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.driver.memory", "16g")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.upload.buffer", "bytebuffer")
    .getOrCreate()
)


def generate_dataframe(n=100):
    import datetime
    import numpy as np
    import pandas as pd

    rng = np.random.default_rng()

    df = pd.DataFrame(
        {
            "date": rng.choice(
                pd.date_range(
                    datetime.datetime(2024, 1, 1), datetime.datetime(2025, 1, 1)
                ).strftime("%Y-%m-%d"),
                size=n,
            ),
            "col1_int": rng.integers(low=0, high=100, size=n),
            "col2_float": rng.uniform(low=0, high=1, size=n),
            "col3_str": rng.choice([f"str_{i}" for i in range(10)], size=n),
            "col4_time": pd.Timestamp.now(tz=datetime.timezone.utc).timestamp(),
            "col5_bool": rng.choice([True, False], size=n),
        }
    )

    df["id"] = df.index
    return spark.createDataFrame(df)


if __name__ == "__main__":
    # generate dataframe
    additional_options = {
        "path": os.environ["ICEBERG_TABLE_PATH"], # e.g. s3://data/iceberg/table
    }

    # Generate and sort by partitions
    df = generate_dataframe()
    df = df.repartition("date").sortWithinPartitions("date")
    df.createOrReplaceTempView("tmp_table")

    (
        df.write.format("iceberg")
        .options(**additional_options)
        .mode("append")
        .partitionBy("date")
        .saveAsTable("iceberg.table")
    )
