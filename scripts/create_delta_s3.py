import os
import pyspark
from delta import configure_spark_with_delta_pip
# env
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# session
builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)
spark = configure_spark_with_delta_pip(
    builder, extra_packages=["org.apache.spark:spark-hadoop-cloud_2.12:3.5.1"]
).getOrCreate()


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
    dataframe = generate_dataframe()

    # write table
    (
        dataframe.write.format("delta")
        .option("path", os.environ["DELTA_TABLE_PATH"]) # e.g. s3://data/delta/table
        .mode("overwrite")
        .partitionBy("date")
        .saveAsTable("table")
    )
    
