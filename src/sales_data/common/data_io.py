import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def load_csv_data(spark: SparkSession, file_path: str, schema: StructType) -> DataFrame:
    """Read a CSV file using an explicit schema."""

    logger.info("Reading raw CSV: %s", file_path)

    try:
        return (
            spark.read.format("csv")
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .load(file_path)
        )
    except Exception:
        logger.exception("Failed to read CSV: %s", file_path)
        raise


def write_csv_data(dataframe: DataFrame, output_path: str) -> None:
    """Write a DataFrame to CSV format."""
    logger.info("Writing CSV data: %s", output_path)

    try:
        (
            dataframe.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(output_path)
        )
        logger.info("CSV data successfully written: %s", output_path)
    except Exception:
        logger.exception("Failed to write CSV data: %s", output_path)
        raise


def read_parquet_data(spark: SparkSession, input_path: str) -> DataFrame:
    """Read a Parquet dataset."""
    logger.info("Reading Parquet data: %s", input_path)

    try:
        return spark.read.parquet(input_path)
    except Exception:
        logger.exception("Failed to read Parquet data: %s", input_path)
        raise


def write_parquet_data(dataframe: DataFrame, output_path: str) -> None:
    """Write a DataFrame to Parquet format."""
    logger.info("Writing Parquet data: %s", output_path)

    try:
        dataframe.write.mode("overwrite").parquet(output_path)
        logger.info("Parquet data successfully written: %s", output_path)
    except Exception:
        logger.exception("Failed to write Parquet data: %s", output_path)
        raise