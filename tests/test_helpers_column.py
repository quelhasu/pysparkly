import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
import pysparkly
import datetime


def create_df(rows: list, spark: SparkSession):
    return spark.createDataFrame(rows)

def test_without_parentheses_content_func(spark: SparkSession):
    dataframe = create_df([
        Row(id=1, name="john (doe)")
    ], spark)

    result = dataframe.withColumn("new_name", pysparkly.without_parentheses_content(F.col("name")))
    expected_result = "john"
    collected_result = result.collect()
    assert collected_result[0].new_name == expected_result

def test_without_parentheses_func(spark: SparkSession):
    dataframe = create_df([
        Row(id=1, name="john (doe)")
    ], spark)

    result = dataframe.withColumn("new_name", pysparkly.without_parentheses(F.col("name")))
    expected_result = "john doe"
    collected_result = result.collect()
    assert collected_result[0].new_name == expected_result

def test_single_space_func(spark: SparkSession):
    dataframe = create_df([
        Row(id=1, name="john   doe")
    ], spark)

    result = dataframe.withColumn("new_name", pysparkly.single_space(F.col("name")))
    expected_result = "john doe"
    collected_result = result.collect()
    assert collected_result[0].new_name == expected_result

def test_without_tirets_func(spark: SparkSession):
    dataframe = create_df([
        Row(id=1, name="john-doe")
    ], spark)

    result = dataframe.withColumn("new_name", pysparkly.without_tirets(F.col("name")))
    expected_result = "john doe"
    collected_result = result.collect()
    assert collected_result[0].new_name == expected_result