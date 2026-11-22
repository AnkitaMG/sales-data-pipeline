from pathlib import Path

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)
from sales_data.ingestion.bronze_ingestion import ingest_datasets

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_PATH = PROJECT_ROOT / "data" / "input" / "raw"
BRONZE_OUTPUT_PATH = PROJECT_ROOT / "data" / "output" / "bronze"


def test_bronze_ingestion(spark):
    """Verify Bronze ingestion, row counts, schemas and DataFrame contents."""
    dataset_one, dataset_two, dataset_three = ingest_datasets(
        str(RAW_DATA_PATH / "dataset_one.csv"),
        str(RAW_DATA_PATH / "dataset_two.csv"),
        str(RAW_DATA_PATH / "dataset_three.csv"),
        str(BRONZE_OUTPUT_PATH),
    )

    # Verify row counts
    assert dataset_one.count() == 1000
    assert dataset_two.count() == 1000
    assert dataset_three.count() == 10000

    # Verify all columns have StringType
    for dataframe in (dataset_one, dataset_two, dataset_three):
        assert all(
            field.dataType.typeName() == "string" for field in dataframe.schema.fields
        )

    # Expected schema for dataset one
    expected_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("area", StringType(), True),
            StructField("calls_made", StringType(), True),
            StructField("calls_successful", StringType(), True),
        ]
    )

    # Get two actual rows to verify Chispa DataFrame comparison
    actual_sample = dataset_one.orderBy("id").limit(2)

    expected_sample = spark.createDataFrame(
        actual_sample.collect(),
        schema=expected_schema,
    )

    # Chispa DataFrame comparison
    assert_df_equality(
        actual_sample,
        expected_sample,
    )
