# Databricks notebook source
# MAGIC %pip install 'laktory==0.1.5'

# COMMAND ----------
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window

from laktory import dlt
from laktory import read_metadata
from laktory import models
from laktory import get_logger

dlt.spark = spark
logger = get_logger(__name__)


# Read pipeline definition
pl_name = spark.conf.get("pipeline_name", "pl-flight-data")
pl = read_metadata(pipeline=pl_name)


# --------------------------------------------------------------------------- #
# Non-CDC Tables                                                              #
# --------------------------------------------------------------------------- #


def define_table(table):
    @dlt.table_or_view(
        name=table.name,
        comment=table.comment,
        as_view=table.builder.as_dlt_view,
    )
    @dlt.expect_all(table.warning_expectations)
    @dlt.expect_all_or_drop(table.drop_expectations)
    @dlt.expect_all_or_fail(table.fail_expectations)
    def get_df():
        logger.info(f"Building {table.name} table")

        # Read Source
        df = table.builder.read_source(spark)
        df.printSchema()

        # Window
        w = Window.orderBy("timestamp")

        # Process
        df = df.withColumn("is_trimmed", F.abs("calibrated_airspeed") < 10)
        df = df.withColumn("was_trimmed", F.lag("is_trimmed").over(w))
        df = df.withColumn("trim_start", F.col("is_trimmed") & ~F.col("was_trimmed"))
        df = df.withColumn("trim_id", F.sum(F.col("trim_start").cast("int")).over(w))
        df = df.filter(F.col("is_trimmed"))

        # Aggregate
        aggs = []
        for c in df.columns:

            if c == "trim_id":
                continue

            if df.schema[c].dataType in [T.LongType(), T.DoubleType(), T.IntegerType(),]:
                aggs += [
                    F.mean(c).alias(c)
                ]
            else:
                aggs += [
                    F.first(c).alias(c)
                ]

        dfa = df.groupby("trim_id").agg(*aggs)

        # Return
        return dfa

    return get_df


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

models.Table(
    name="trims",

)
# Build tables
for table in pl.tables:
    if table.builder.template == "TRIM":
        wrapper = define_table(table)
        df = dlt.get_df(wrapper)
        display(df)