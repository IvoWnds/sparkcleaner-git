from pyspark.sql import DataFrame as SparkDataFrame

def cast_int_to_date(df: SparkDataFrame, col_name: str, date_format: str = ...) -> SparkDataFrame: ...