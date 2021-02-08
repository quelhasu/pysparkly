import pytest

# from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

MASTER = 'local[*]'

def get_conf_spark() -> SparkConf:
    return SparkConf()\
        .set("spark.sql.shuffle.partitions", "1") \
        .set("spark.ui.enabled", "false")

@pytest.fixture(scope="session")
def spark(request):
    spark = SparkSession.builder\
        .appName("ETL")\
        .master(MASTER)\
        .config(conf=get_conf_spark())\
        .getOrCreate()
    return spark