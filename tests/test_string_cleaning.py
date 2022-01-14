import unittest

import pyspark.sql.functions as F
import src.sparkcleaner.functions.string_cleaning as scsclean
import src.sparkcleaner.helpers.example_data as example_data 
from pyspark.sql import DataFrame as SparkDataFrame


class StringCleanTester(unittest.TestCase):
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

    def test_remove_leading_zeros(self):
        """Check that test_remove_leading_zeros function works as expected."""
        og_row_count: int = self.test_df.filter(F.col("MATERIAL")
                                                 .startswith("0")) \
                                        .count()
        self.assertGreater(og_row_count, 0,
                           "No zero leading rows, test cannot be executed")
        trans_df: SparkDataFrame = scsclean.remove_leading_zeros(
            self.test_df, "MATERIAL")

        # startswith("00") to prevent false pos after transformation
        self.assertEqual(trans_df.filter(F.col("MATERIAL")
                                          .startswith("00")).count(), 0)


if __name__ == "__main__":
    unittest.main()
