from pyspark.sql import DataFrame as SparkDataFrame

def remove_leading_zeros(df: SparkDataFrame, col_name: str, maintain_type: bool = ...): ...