import argparse
from collections.abc import Sequence

from sales_data.utils import config
from sales_data.utils.spark import get_spark_session
from sales_data.ingestion.bronze_ingestion import ingest_datasets
from sales_data.transformations.silver_transform import run_silver_transformation
from sales_data.transformations.gold_transform import run_gold_transformation
from sales_data.validation.data_quality import run_data_quality_checks
from sales_data.utils.logger import setup_logger


def _parse_bool(value: str) -> bool:
    """Parse a command-line boolean value."""
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    raise argparse.ArgumentTypeError(
        "expected a boolean value: true/false, yes/no, on/off, or 1/0"
    )


def main(argv: Sequence[str] | None = None):
    parser = argparse.ArgumentParser(description="Sales Data Pipeline")
    parser.add_argument(
        "--job",
        required=True,
        choices=["bronze", "silver", "gold"],
    )
    parser.add_argument("--base-path", default=config.BASE_PATH)
    parser.add_argument(
        "--input-path",
        help="Layer input directory; defaults to the path derived from --base-path.",
    )
    parser.add_argument(
        "--output-path",
        help="Layer output directory; defaults to the path derived from --base-path.",
    )
    parser.add_argument(
        "--errors-path",
        help="Bronze quarantine output directory.",
    )

    parser.add_argument(
        "--strict-data-quality",
        nargs="?",
        const=True,
        type=_parse_bool,
        default=True,
        help=(
            "Fail the Bronze job when data-quality checks fail. Accepts "
            "true/false (default: true); use --no-strict-data-quality to continue."
        ),
    )
    parser.add_argument("--dataset-one")
    parser.add_argument("--dataset-two")
    parser.add_argument("--dataset-three")
    parser.add_argument(
        "--no-strict-data-quality",
        dest="strict_data_quality",
        action="store_false",
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(argv)

    logger = setup_logger()
    spark = get_spark_session()

    try:
        if args.job == "bronze":
            logger.info("Starting Bronze job.")
            base_path = args.base_path.rstrip("/")
            input_path = args.input_path or f"{base_path}/input"
            output_path = args.output_path or f"{base_path}/output/bronze"
            error_path = args.errors_path or f"{base_path}/output/errors"

            (
                dataset_one,
                dataset_two,
                dataset_three,
                quarantined_call_count,
            ) = ingest_datasets(
                args.dataset_one or f"{input_path}/dataset_one.csv",
                args.dataset_two or f"{input_path}/dataset_two.csv",
                args.dataset_three or f"{input_path}/dataset_three.csv",
                output_path,
                error_output_path=error_path,
                return_quarantine_count=True,
            )

            run_data_quality_checks(
                dataset_one,
                dataset_two,
                dataset_three,
                strict=args.strict_data_quality,
                quarantined_call_count=quarantined_call_count,
            )

        elif args.job == "silver":
            logger.info("Starting Silver job.")

            run_silver_transformation(
                args.input_path or f"{args.base_path.rstrip('/')}/output/bronze",
                args.output_path or f"{args.base_path.rstrip('/')}/output/silver",
            )

        elif args.job == "gold":
            logger.info("Starting Gold job.")

            run_gold_transformation(
                args.input_path or f"{args.base_path.rstrip('/')}/output/silver",
                args.output_path or f"{args.base_path.rstrip('/')}/output/gold",
            )

        logger.info("%s job completed successfully.", args.job)

    except Exception:
        logger.exception("%s job failed.", args.job)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
