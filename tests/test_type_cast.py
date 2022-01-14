import unittest
import src.sparkcleaner.helpers.example_data as example_data
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import DataType, DateType, IntegerType, LongType
import src.sparkcleaner.functions.type_cast as sccast


class TypeCastTester(unittest.TestCase):
    """Class containing unit tests for the pyspark_df_cleaner.py module."""

    def setUp(self):
        """Load test data for clean testing."""
        self.test_df = example_data.load_example_data()

    def test_if_spark_df(self):
        """Spark df test to assure correct functioning of functions."""
        self.assertIsInstance(self.test_df, SparkDataFrame,
                              "df passed is not a Pyspark dataframe")

    def test_if_spark_df_has_rows(self):
        """Check row count as rows are needed for manipulations."""
        self.assertGreater(self.test_df.count(), 0, "df has no rows")

    def test_cast_int_to_date(self):
        """Check that cast_int_to_date works as expected."""
        og_date_col_type: DataType = self.test_df.schema["MODIFIED_DATE"] \
                                                 .dataType
        self.assertIsInstance(og_date_col_type, (LongType, IntegerType),
                              "col not correct type, test cannot be executed")
        trans_df: SparkDataFrame = sccast.cast_int_to_date(self.test_df,
                                                           "MODIFIED_DATE")
        self.assertIsInstance(trans_df.schema["MODIFIED_DATE"]
                                      .dataType, DateType)


if __name__ == "__main__":
    unittest.main()
