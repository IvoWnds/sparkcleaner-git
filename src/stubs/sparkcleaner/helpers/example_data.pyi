from pyspark.sql import DataFrame as SparkDataFrame
from typing import Any

spark: Any

def load_example_data() -> SparkDataFrame: ...
