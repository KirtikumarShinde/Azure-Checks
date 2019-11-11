"""
This module contains overriding for SparkDataSet so that tables can be save as bucketed hive table
"""
from typing import Optional, Dict, Any

from kedro.contrib.io.pyspark import SparkDataSet
from pyspark.sql import DataFrame


class BucketedSparkDataSet(SparkDataSet):
    """
    BucketedSparkDataset overrides SparkDataSet
    This class has the same usage as SparkDataSet, with additional parameters of bucket column, bucket number and table
    name
    """
    def __init__(self,
                 filepath: str,
                 bucket_by: str,
                 number_of_buckets: int,
                 table_name: str,
                 file_format: str = "parquet",
                 load_args: Optional[Dict[str, Any]] = None,
                 save_args: Optional[Dict[str, Any]] = None) -> None:
        """
        BucketedSparkDataset overrides primarily the save method of the SparkDataSet so that a Spark Table is saved as
        a parquet file with selected number of buckets and bucket_by column.
        Args:
            filepath: path to a Spark data frame.
            bucket_by: Spark column to bucket by
            number_of_buckets: Integer indicating the number of buckets
            table_name: Spark Table name
            file_format: file format used during load and save
                operations. These are formats supported by the running
                SparkContext include parquet, csv. For a list of supported
                formats please refer to Apache Spark documentation at
                https://spark.apache.org/docs/latest/sql-programming-guide.html
            load_args: Load args passed to Spark DataFrameReader load method.
                It is dependent on the selected file format. You can find
                a list of read options for each supported format
                in Spark DataFrame read documentation:
                https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
            save_args: Save args passed to Spark DataFrame write options.
                Similar to load_args this is dependent on the selected file
                format. You can pass ``mode`` and ``partitionBy`` to specify
                your overwrite mode and partitioning respectively. You can find
                a list of options for each format in Spark DataFrame
                write documentation:
                https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
        """
        super(BucketedSparkDataSet, self).__init__(filepath=filepath,
                                                   file_format=file_format,
                                                   load_args=load_args,
                                                   save_args=save_args)
        self._num_buckets = number_of_buckets
        self._bucket_by = bucket_by
        self._table_name = table_name

    def _save(self, data: DataFrame) -> None:
        # NOTE: .sortBy can be added after bucketBy for sorted writes
        (
            data.write
                .option("path", self._filepath)
                .bucketBy(self._num_buckets, self._bucket_by)
                .saveAsTable(self._table_name, format=self._file_format, **self._save_args)
        )
