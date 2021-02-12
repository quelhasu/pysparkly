import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
import datetime
from pysparkly.extensions import *


def create_df(rows: list, spark: SparkSession):
    return spark.createDataFrame(rows)


def test_dataframe_with_columns(spark: SparkSession):
    dataframe = create_df([
        Row(id=1, name="John", lastname="Doe", age=23,
            birthdate=datetime.date(1987, 1, 1)),
        Row(id=2, name="Nhoj", lastname="Eod", age=32,
            birthdate=datetime.date(1978, 1, 1))
    ], spark)

    @F.udf(T.StringType())
    def _lower_string(str_to_lower):
        return str_to_lower.lower()

    result = dataframe.with_columns(['name', 'lastname'], _lower_string,
                                    output_cols=['lower_name', 'lower_lastname'])
    collected_result = result.collect()
    assert collected_result[0].lower_name == "john"
    assert collected_result[0].lower_lastname == "doe"
    assert collected_result[1].lower_name == "nhoj"
    assert collected_result[1].lower_lastname == "eod"


def test_dataframe_order_columns(spark: SparkSession):
    dataframe = create_df([
        Row(ab=1, bb="b", ba="c", aa=2, e=datetime.date(1987, 1, 1), f=1.2)
    ], spark)
    result = dataframe.order_columns()
    expected_result = ["aa", "ab", "ba", "bb", "e", "f"]
    assert expected_result == result.columns

    result = dataframe.order_columns(order="desc")
    expected_result = ["f", "e", "bb", "ba", "ab", "aa"]
    assert expected_result == result.columns

    result = dataframe.order_columns(by_dtypes=True)
    expected_result = ["aa", "ab", "e", "f", "ba", "bb"]
    assert expected_result == result.columns


def test_dataframe_copy(spark: SparkSession):
    dataframe = create_df([
        Row(id=1, name="John", lastname="Doe", age=23,
            birthdate=datetime.date(1987, 1, 1))
    ], spark)
    result = dataframe.copy(['name'], ['surname'])
    expected_result = ['surname', 'name']
    assert all([col in result.columns for col in expected_result]) == True


def test_dataframe_select_columns(spark: SparkSession):
    dataframe = create_df([
        Row(id=1, name="John", lastname="Doe", age=23, birthdate=datetime.date(1987, 1, 1),
            star_date=datetime.date(2019, 3, 14), event_date=datetime.date(2000, 1, 1), birthdate_month=1)
    ], spark)
    result = dataframe.select_columns(
        included_pattern=["date"], excluded_pattern=["star"], dtypes=['date'])
    expected_result = ["birthdate", "event_date"]
    assert all([col in expected_result for col in result.columns]) == True
