"""Data quality checks for the sales data pipeline."""
import logging
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("sales_data")

EXPECTED_ROW_COUNTS = {
    "dataset_one": 1000,
    "dataset_two": 1000,
    "dataset_three": 10000,
}

ID_COLUMNS = {
    "dataset_one": "id",
    "dataset_two": "id",
    "dataset_three": "id",
}

NUMERIC_COLUMNS = {
    "dataset_one": ["calls_made", "calls_successful"],
    "dataset_two": ["sales_amount"],
    "dataset_three": ["age", "quantity"],
}


def _record_issue(issues: List[str], message: str) -> None:
    """Log a validation warning and record the issue."""
    logger.warning(message)
    issues.append(message)


def _check_basic(
    datasets: dict,
    issues: List[str],
) -> None:
    """Run basic row-count, ID and numeric-field checks."""

    for dataset_name, df in datasets.items():
        expected_count = EXPECTED_ROW_COUNTS[dataset_name]
        actual_count = df.count()

        if actual_count != expected_count:
            _record_issue(
                issues,
                f"{dataset_name}: expected {expected_count} rows, "
                f"found {actual_count}.",
            )

        id_column = ID_COLUMNS[dataset_name]

        null_ids = df.filter(F.col(id_column).isNull()).count()
        if null_ids:
            _record_issue(
                issues,
                f"{dataset_name}: found {null_ids} null values in '{id_column}'.",
            )

        duplicate_ids = (
            df.groupBy(id_column)
            .count()
            .filter((F.col("count") > 1) | F.col(id_column).isNull())
            .count()
        )

        if duplicate_ids:
            _record_issue(
                issues,
                f"{dataset_name}: found {duplicate_ids} duplicate ID groups "
                f"in '{id_column}'.",
            )

        for column_name in NUMERIC_COLUMNS[dataset_name]:
            negative_count = df.filter(F.col(column_name).cast("double") < 0).count()

            if negative_count:
                _record_issue(
                    issues,
                    f"{dataset_name}: found {negative_count} negative values "
                    f"in '{column_name}'.",
                )


def _check_referential_integrity(
    dataset_one: DataFrame,
    dataset_three: DataFrame,
    issues: List[str],
) -> None:
    """Check that each transaction caller_id matches exactly one employee ID."""

    employee_ids = (
        dataset_one.groupBy("id")
        .count()
        .withColumnRenamed("id", "employee_id")
        .withColumnRenamed("count", "employee_match_count")
    )

    transactions = dataset_three.alias("transactions")
    employees = employee_ids.alias("employees")

    unmatched_or_ambiguous = (
        transactions.join(
            employees,
            F.col("transactions.caller_id") == F.col("employees.employee_id"),
            "left",
        )
        .filter(
            F.col("employees.employee_match_count").isNull()
            | (F.col("employees.employee_match_count") != 1)
        )
        .count()
    )

    if unmatched_or_ambiguous:
        _record_issue(
            issues,
            f"dataset_three: found {unmatched_or_ambiguous} transactions "
            "whose caller_id does not match exactly one dataset_one ID.",
        )


def _check_call_counts(
    dataset_one: DataFrame,
    issues: List[str],
    quarantined_count: int = 0,
) -> None:
    """Check for call-count violations that were not quarantined."""

    invalid_count = dataset_one.filter(
        F.col("calls_successful").cast("long") > F.col("calls_made").cast("long")
    ).count()

    unquarantined_count = max(0, invalid_count - quarantined_count)
    if unquarantined_count:
        _record_issue(
            issues,
            f"dataset_one: found {unquarantined_count} non-quarantined "
            "records where calls_successful is greater than calls_made.",
        )


def _check_addresses(
    dataset_two: DataFrame,
    issues: List[str],
) -> None:
    """Validate addresses in ``street name, house number, ZIP code`` format."""

    parts = F.split(F.col("address"), r"\s*,\s*")
    part_count = F.size(parts)
    street_name = F.trim(parts.getItem(0))
    house_number = F.trim(parts.getItem(1))
    zip_code = F.trim(parts.getItem(2))
    street_pattern = r"^[\p{L}\p{N}]+(?:[\s\p{L}\p{N}]*[\p{L}\p{N}]+)?$"
    house_number_pattern = r"^[0-9]+$"
    zip_pattern = r"^[0-9]{4} [A-Z]{2}$"

    valid_format = (
        (part_count == 3)
        & street_name.rlike(street_pattern)
        & house_number.rlike(house_number_pattern)
        & zip_code.rlike(zip_pattern)
    )

    invalid_count = dataset_two.filter(
        F.col("address").isNull() | ~valid_format
    ).count()

    if invalid_count:
        _record_issue(
            issues,
            f"dataset_two: found {invalid_count} addresses with an invalid format.",
        )


def run_data_quality_checks(
    dataset_one: DataFrame,
    dataset_two: DataFrame,
    dataset_three: DataFrame,
    strict: bool = False,
    quarantined_call_count: int = 0,
) -> None:
    """
    Run basic and optional intermediate data quality checks.

    :param dataset_one: Employee expertise and call statistics.
    :param dataset_two: Employee personal and sales information.
    :param dataset_three: Sales transaction records.
    :param strict: Stop execution if any check fails.
    :raises ValueError: If strict mode is enabled and checks fail.
    """

    datasets = {
        "dataset_one": dataset_one,
        "dataset_two": dataset_two,
        "dataset_three": dataset_three,
    }

    issues: List[str] = []

    logger.info("Starting basic data quality checks.")
    _check_basic(datasets, issues)

    logger.info("Starting intermediate data quality checks.")
    _check_referential_integrity(dataset_one, dataset_three, issues)
    _check_call_counts(
        dataset_one,
        issues,
        quarantined_count=quarantined_call_count,
    )
    _check_addresses(dataset_two, issues)

    if issues:
        logger.warning("Data quality checks found %s issue(s).", len(issues))

        if strict:
            raise ValueError(
                f"Data quality validation failed with {len(issues)} issue(s). "
                "Strict mode is enabled. Details: " + " | ".join(issues)
            )
    else:
        logger.info("All data quality checks passed.")
