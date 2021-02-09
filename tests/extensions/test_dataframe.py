import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
import datetime
from pysparkly.extensions import *


def create_df(rows: list, spark: SparkSession):
    return spark.createDataFrame(rows)

def test_dataframe_copy(spark: SparkSession):
    dataframe = create_df([
        Row(id=1, name="John", lastname="Doe", age=23, birthdate=datetime.date(1987,1,1))
    ], spark)
    result = dataframe.copy(['name'], ['surname'])
    expected_result = ['surname', 'name']
    assert all([col in result.columns for col in expected_result]) == True

    @F.udf(T.StringType())
    def _lower_string(str_to_lower):
        return str_to_lower.lower()

    result = dataframe.copy(['name', 'lastname'], ['lower_name', 'lower_lastname'], _lower_string)
    expected_result = ['lower_name', 'lower_lastname']
    assert all([col in result.columns for col in expected_result]) == True
    collected_result = result.collect()
    assert collected_result[0].lower_name == "john"
    assert collected_result[0].lower_lastname == "doe"

def test_dataframe_select_columns(spark: SparkSession):
    dataframe = create_df([
        Row(id=1, name="John", lastname="Doe", age=23, birthdate=datetime.date(1987,1,1), 
            star_date=datetime.date(2019,3,14), event_date=datetime.date(2000,1,1), birthdate_month=1)
    ], spark)
    result = dataframe.select_columns(included_pattern=["date"], excluded_pattern=["star"], dtypes=['date'])
    expected_result = ["birthdate", "event_date"]
    assert all([col in expected_result for col in result.columns]) == True



