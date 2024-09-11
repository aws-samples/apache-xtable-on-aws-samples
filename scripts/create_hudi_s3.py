import os
from pyspark.sql import SparkSession


# env
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# session
spark = (
    SparkSession.builder.appName("MyApp")
    .config(
        "spark.jars.packages",
        "org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0,org.apache.spark:spark-hadoop-cloud_2.12:3.5.1",
    )
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
    )
    .config(
        "spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
    )
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
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
    hoodie_options = {
        "hoodie.table.name": "table",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "col4_time",
        "hoodie.datasource.write.operation": "insert",
        "hoodie.datasource.write.partitionpath.field": "date",
        "hoodie.datasource.write.table.name": "table",
        "hoodie.deltastreamer.source.dfs.listing.max.fileid": "-1",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "path": os.environ["HUDI_TABLE_PATH"], # e.g. s3://data/hudi/table
    }

    # generate dataframe
    dataframe = generate_dataframe()

    dataframe.write.format("hudi").options(**hoodie_options).mode("append").save()
