import pytest
from pyspark.sql import SparkSession, Row
import pysparkly
import datetime


def create_df(rows: list, spark: SparkSession):
    return spark.createDataFrame(rows)


def test_dataframe_parse_columns(spark: SparkSession):
    dataframe = create_df([
        Row(id=1, name="John", firstname="Doe", age=23, birthdate=datetime.date(1987,1,1), star_date=datetime.date(2019,3,14))
    ], spark)
    
    str_columns = pysparkly.parse_columns(dataframe, dtypes=['string'])
    expected_result_str = ['name', 'firstname']
    assert all([col in expected_result_str for col in str_columns]) == True

    date_columns = pysparkly.parse_columns(dataframe, included_pattern=['date'])
    expected_result_date = ['birthdate', 'star_date']
    assert all([col in expected_result_date for col in date_columns]) == True

    not_date_columns = pysparkly.parse_columns(dataframe, excluded_pattern=['date'])
    expected_result_not_date = ['id', 'name', 'firstname', 'age']
    assert all([col in expected_result_not_date for col in not_date_columns]) == True