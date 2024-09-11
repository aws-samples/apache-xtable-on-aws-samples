import sys
import os
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Glue Setup
# --conf
# spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.spark_catalog.warehouse=<# e.g. s3://data/iceberg/table> --conf spark.sql.catalog.spark_catalog.type=hadoop --conf spark.sql.defaultCatalog=spark_catalog --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
# --datalake-formats
# iceberg
# --additional-python-modules
# Faker


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
glueContext = GlueContext(SparkContext())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


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


# generate dataframe
additional_options = {
    "path": args["ICEBERG_TABLE_PATH"], # e.g. s3://data/iceberg/table
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

# commit
job.commit()
