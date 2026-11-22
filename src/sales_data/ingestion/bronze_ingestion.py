import logging
from datetime import datetime, timezone

from pyspark.sql import functions as F

from sales_data.utils.spark import get_spark_session

from sales_data.utils.schemas import (
    DATASET_ONE_SCHEMA,
    DATASET_TWO_SCHEMA,
    DATASET_THREE_SCHEMA,
)
from sales_data.common.data_io import load_csv_data, write_parquet_data

logger = logging.getLogger(__name__)


def ingest_datasets(
    dataset_one_path,
    dataset_two_path,
    dataset_three_path,
    bronze_output_path,
    error_output_path=None,
    return_quarantine_count=False,
):
    logger.info("Starting Bronze ingestion")

    spark = get_spark_session()

    dataset_one_df = load_csv_data(spark, dataset_one_path, DATASET_ONE_SCHEMA)
    dataset_two_df = load_csv_data(spark, dataset_two_path, DATASET_TWO_SCHEMA)
    dataset_three_df = load_csv_data(spark, dataset_three_path, DATASET_THREE_SCHEMA)

    invalid_call_count = F.coalesce(
        F.col("calls_successful").cast("long") > F.col("calls_made").cast("long"),
        F.lit(False),
    )
    invalid_calls_df = dataset_one_df.filter(invalid_call_count)
    quarantine_count = invalid_calls_df.count()
    bronze_dataset_one_df = dataset_one_df

    if quarantine_count and error_output_path:
        run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        quarantine_path = f"{error_output_path.rstrip('/')}/dataset_one/{run_timestamp}"
        logger.warning(
            "Quarantining %s dataset_one row(s) with calls_successful "
            "greater than calls_made to %s",
            quarantine_count,
            quarantine_path,
        )
        invalid_calls_df.write.mode("errorifexists").parquet(quarantine_path)
        bronze_dataset_one_df = dataset_one_df.filter(~invalid_call_count)

    write_parquet_data(
        bronze_dataset_one_df,
        f"{bronze_output_path}/dataset_one",
    )
    write_parquet_data(dataset_two_df, f"{bronze_output_path}/dataset_two")
    write_parquet_data(dataset_three_df, f"{bronze_output_path}/dataset_three")

    logger.info("Bronze ingestion completed successfully")

    results = (dataset_one_df, dataset_two_df, dataset_three_df)
    if return_quarantine_count:
        return (*results, quarantine_count)
    return results


if __name__ == "__main__":
    ingest_datasets()
