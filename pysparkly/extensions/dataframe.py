from pydeco import add_method
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pysparkly import parse_columns


@add_method(DataFrame)
def order_columns(self, by_dtypes:bool=False):
    if by_dtypes:
        dtypes_dict = dict()
        for col, dtype in self.dtypes:
            dtypes_dict.setdefault(dtype, list())
            dtypes_dict[dtype].append(col)
        dtypes_dict = dict(sorted(dtypes_dict.items()))
        columns = [col for values in dtypes_dict.values() for col in sorted(values)]
        return self.select(columns)
        
    else:
        return self.select(sorted(self.columns))


@add_method(DataFrame)
def copy(self, input_cols, output_cols, udf=None, udf_args=None):
    """
    Copy columns in input_cols into output_cols with optional 
    user-defined function.

    :param DataFrame self:
    :param list(str) input_cols: copied columns.
    :param list(str) output_cols: returned columns.
    """
    columns = list(zip(input_cols, output_cols))
    for in_col, out_col in columns:
        if udf:
            if udf_args:
                self = self.withColumn(out_col, udf(in_col, udf_args))
            else:
                 self = self.withColumn(out_col, udf(in_col))
        else:
            self = self.withColumn(out_col, F.col(in_col))
    return self

@add_method(DataFrame)
def select_columns(self, included_pattern=None, excluded_pattern=None, columns=None, dtypes=None):
    """
    Select specific columns from dataframe.

    :param DataFrame self:
    :param list(str) included_pattern: keep columns in this list.
    :param list(str) excluded_pattern: keep columns not int this list.
    :param list(str) columns: columns to parsed, by default use dataframe's columns.
    :param list(str) dtypes: types to keep, if 'None' keep all columns.
    """
    columns_to_keep = parse_columns(self, included_pattern, excluded_pattern, columns, dtypes)
    return self.select(*list(columns_to_keep))