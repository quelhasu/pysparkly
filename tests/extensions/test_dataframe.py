import pytest
from pyspark.sql import SparkSession, Row
import datetime
from pysparkly.extensions import *


def create_df(rows: list, spark: SparkSession):
    return spark.createDataFrame(rows)

def test_dataframe_select_columns(spark: SparkSession):
    dataframe = create_df([
        Row(id=1, name="John", firstname="Doe", age=23, birthdate=datetime.date(1987,1,1), 
            star_date=datetime.date(2019,3,14), event_date=datetime.date(2000,1,1), birthdate_month=1)
    ], spark)
    result = dataframe.select_columns(included_pattern=["date"], excluded_pattern=["star"], dtypes=['date'])
    expected_result = ["birthdate", "event_date"]
    assert all([col in expected_result for col in result.columns]) == True



