import sys
import os
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Glue Setup
# --conf
# spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false
# --datalake-formats
# hudi
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


additional_options = {
    "hoodie.table.name": "table",
    "hoodie.datasource.write.table.name": "table",
    "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "insert",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "col4_time",
    "hoodie.datasource.write.partitionpath.field": "date",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "path": args["HUDI_TABLE_PATH"], # e.g. s3://data/hudi/table
}

df = generate_dataframe()
df.write.format("hudi").options(**additional_options).mode("append").save()

job.commit()
