from decimal import Decimal
from pathlib import Path

from chispa.dataframe_comparer import assert_df_equality

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    IntegerType,
)

from sales_data.transformations.silver_transform import (
    run_silver_transformation,
    transform_dataset_one,
    transform_dataset_two,
)
from sales_data.ingestion.bronze_ingestion import ingest_datasets

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_PATH = PROJECT_ROOT / "data" / "input" / "raw"
BRONZE_OUTPUT_PATH = PROJECT_ROOT / "data" / "output" / "bronze"
SILVER_OUTPUT_PATH = PROJECT_ROOT / "data" / "output" / "silver"


def test_transform_dataset_one(spark):
    input_df = spark.createDataFrame(
        [
            ("001", "IT", "100", "80"),
            ("002", "Marketing", "50", "40"),
        ],
        ["id", "area", "calls_made", "calls_successful"],
    )

    actual_df = transform_dataset_one(input_df)

    expected_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("area", StringType(), True),
            StructField("calls_made", IntegerType(), True),
            StructField("calls_successful", IntegerType(), True),
        ]
    )

    expected_df = spark.createDataFrame(
        [
            ("001", "IT", 100, 80),
            ("002", "Marketing", 50, 40),
        ],
        schema=expected_schema,
    )

    assert_df_equality(actual_df, expected_df)


def test_transform_dataset_two(spark):
    input_df = spark.createDataFrame(
        [("001", "Rahul", "Main Street, 10, 1234 AB", "2500.50")],
        ["id", "name", "address", "sales_amount"],
    )

    actual_df = transform_dataset_two(input_df)

    expected_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("sales_amount", DecimalType(18, 2), True),
        ]
    )

    expected_df = spark.createDataFrame(
        [("001", "Rahul", "Main Street, 10, 1234 AB", Decimal("2500.50"))],
        schema=expected_schema,
    )

    assert_df_equality(actual_df, expected_df)


def test_silver_writes_local_outputs(spark):
    """Run the Silver stage against local Bronze data and persist its outputs."""
    if not (BRONZE_OUTPUT_PATH / "dataset_one").is_dir():
        ingest_datasets(
            str(RAW_DATA_PATH / "dataset_one.csv"),
            str(RAW_DATA_PATH / "dataset_two.csv"),
            str(RAW_DATA_PATH / "dataset_three.csv"),
            str(BRONZE_OUTPUT_PATH),
        )

    run_silver_transformation(
        str(BRONZE_OUTPUT_PATH),
        str(SILVER_OUTPUT_PATH),
    )

    for dataset_name in ("dataset_one", "dataset_two", "dataset_three"):
        assert (SILVER_OUTPUT_PATH / dataset_name).is_dir()
