from sales_data.utils.spark import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, DecimalType

from sales_data.utils.logger import setup_logger
from sales_data.common.data_io import (
    read_parquet_data,
    write_parquet_data,
)

logger = setup_logger()


def clean_string_columns(dataframe: DataFrame) -> DataFrame:
    """
    Trim whitespace and convert empty strings to null.

    :param dataframe: Input Spark DataFrame.
    :return: Cleaned Spark DataFrame.
    """
    for field in dataframe.schema.fields:
        if field.dataType.typeName() == "string":
            column = F.trim(F.col(field.name))

            dataframe = dataframe.withColumn(
                field.name, F.when(column == "", None).otherwise(column)
            )

    return dataframe


def transform_dataset_one(dataframe: DataFrame) -> DataFrame:
    """
    Transform employee call statistics.

    :param dataframe: Bronze dataset one.
    :return: Silver dataset one.
    """
    logger.info("Transforming dataset one")

    dataframe = clean_string_columns(dataframe)

    dataframe = (
        dataframe.withColumn("calls_made", F.col("calls_made").cast(IntegerType()))
        .withColumn("calls_successful", F.col("calls_successful").cast(IntegerType()))
        .dropDuplicates()
    )

    return dataframe


def transform_dataset_two(dataframe: DataFrame) -> DataFrame:
    """
    Transform employee and sales information.

    :param dataframe: Bronze dataset two.
    :return: Silver dataset two.
    """
    logger.info("Transforming dataset two")

    dataframe = clean_string_columns(dataframe)

    dataframe = dataframe.withColumn(
        "sales_amount", F.col("sales_amount").cast(DecimalType(18, 2))
    ).dropDuplicates()

    return dataframe


def transform_dataset_three(dataframe: DataFrame) -> DataFrame:
    """
    Transform sales transaction information.

    :param dataframe: Bronze dataset three.
    :return: Silver dataset three.
    """
    logger.info("Transforming dataset three")

    dataframe = clean_string_columns(dataframe)

    dataframe = (
        dataframe.withColumn("age", F.col("age").cast(IntegerType()))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .dropDuplicates()
    )

    return dataframe


def read_bronze_data(
    spark: SparkSession, bronze_path: str
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Read the three persisted Bronze datasets.

    :param spark: Active SparkSession.
    :param bronze_path: Root Bronze output directory.
    :return: Three Bronze DataFrames.
    """
    logger.info("Reading Bronze datasets from %s", bronze_path)

    dataset_one = read_parquet_data(spark, f"{bronze_path}/dataset_one")

    dataset_two = read_parquet_data(spark, f"{bronze_path}/dataset_two")

    dataset_three = read_parquet_data(spark, f"{bronze_path}/dataset_three")

    return dataset_one, dataset_two, dataset_three


def write_silver_data(dataframe: DataFrame, output_path: str) -> None:
    """
    Persist a Silver DataFrame as Parquet.

    :param dataframe: Transformed Spark DataFrame.
    :param output_path: Destination directory.
    """
    logger.info("Writing Silver data to %s", output_path)

    write_parquet_data(dataframe, output_path)


def run_silver_transformation(
    bronze_path, silver_path
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Execute the complete Bronze-to-Silver transformation.

    :param spark: Active SparkSession.
    :param bronze_path: Bronze root directory.
    :param silver_path: Silver root directory.
    :return: Three transformed Silver DataFrames.
    """
    logger.info("Starting Silver transformation")
    spark = get_spark_session()
    # Step 1: Read Bronze datasets
    dataset_one, dataset_two, dataset_three = read_bronze_data(spark, bronze_path)

    # Step 2: Transform datasets
    silver_one = transform_dataset_one(dataset_one)
    silver_two = transform_dataset_two(dataset_two)
    silver_three = transform_dataset_three(dataset_three)

    # Step 3: Write Silver datasets
    write_silver_data(silver_one, f"{silver_path}/dataset_one")
    write_silver_data(silver_two, f"{silver_path}/dataset_two")
    write_silver_data(silver_three, f"{silver_path}/dataset_three")

    logger.info("Silver transformation completed successfully")

    return silver_one, silver_two, silver_three
