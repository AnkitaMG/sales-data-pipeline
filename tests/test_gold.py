from decimal import Decimal
from pathlib import Path

from chispa.dataframe_comparer import assert_df_equality

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DoubleType,
)

from sales_data.transformations.gold_transform import (
    create_it_data,
    create_marketing_address_info,
    create_department_breakdown,
    create_top_3,
    create_top_3_products_netherlands,
    create_best_salesperson_per_country,
    run_gold_transformation,
)
from sales_data.transformations.silver_transform import run_silver_transformation
from sales_data.ingestion.bronze_ingestion import ingest_datasets

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_PATH = PROJECT_ROOT / "data" / "input" / "raw"
BRONZE_OUTPUT_PATH = PROJECT_ROOT / "data" / "output" / "bronze"
SILVER_OUTPUT_PATH = PROJECT_ROOT / "data" / "output" / "silver"
GOLD_OUTPUT_PATH = PROJECT_ROOT / "data" / "output" / "gold"


def create_dataset_one(spark):
    """Create sample employee call statistics."""
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("area", StringType(), True),
            StructField("calls_made", IntegerType(), True),
            StructField("calls_successful", IntegerType(), True),
        ]
    )

    return spark.createDataFrame(
        [
            ("001", "IT", 100, 90),
            ("002", "IT", 100, 80),
            ("003", "IT", 100, 70),
            ("004", "Marketing", 100, 85),
            ("005", "Marketing", 100, 60),
        ],
        schema,
    )


def create_dataset_two(spark):
    """Create sample employee sales information."""
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("sales_amount", DecimalType(18, 2), True),
        ]
    )

    return spark.createDataFrame(
        [
            ("001", "Alice", "Main Street, 10, 1234 AB", Decimal("5000.00")),
            ("002", "Bob", "High Street, 20, 2345 CD", Decimal("4000.00")),
            ("003", "Charlie", "Park Road, 30, 3456 EF", Decimal("3000.00")),
            ("004", "David", "River Road, 40, 4567 GH", Decimal("2500.00")),
            ("005", "Emma", "Lake Road, 50, 5678 IJ", Decimal("1500.00")),
        ],
        schema,
    )


def create_dataset_three(spark):
    """Create sample sales transactions."""
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("caller_id", StringType(), True),
            StructField("company", StringType(), True),
            StructField("recipient", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("country", StringType(), True),
            StructField("product_sold", StringType(), True),
            StructField("quantity", IntegerType(), True),
        ]
    )

    return spark.createDataFrame(
        [
            ("T001", "001", "Company A", "John", 30, "Netherlands", "Laptop", 10),
            ("T002", "001", "Company B", "Peter", 40, "Netherlands", "Laptop", 5),
            ("T003", "002", "Company C", "Anna", 35, "Netherlands", "Phone", 20),
            ("T004", "004", "Company D", "Mark", 45, "Netherlands", "Laptop", 30),
            ("T005", "004", "Company E", "Lisa", 28, "Germany", "Tablet", 15),
            ("T006", "003", "Company F", "Tom", 50, "Germany", "Phone", 8),
        ],
        schema,
    )


def test_create_it_data(spark):
    """Test IT filtering and descending sales order."""
    actual = create_it_data(
        create_dataset_one(spark),
        create_dataset_two(spark),
    )

    assert actual.count() == 3

    names = [row["name"] for row in actual.select("name").collect()]

    assert names == ["Alice", "Bob", "Charlie"]


def test_create_marketing_address_info(spark):
    """Test Marketing employee address extraction."""
    actual = create_marketing_address_info(
        create_dataset_one(spark),
        create_dataset_two(spark),
    )

    expected = spark.createDataFrame(
        [
            ("River Road", "40", "4567 GH"),
            ("Lake Road", "50", "5678 IJ"),
        ],
        ["street", "house_number", "zip_code"],
    )

    assert_df_equality(
        actual.orderBy("zip_code"),
        expected.orderBy("zip_code"),
    )


def test_create_department_breakdown(spark):
    """Test department-level sales and call success calculations."""
    actual = create_department_breakdown(
        create_dataset_one(spark),
        create_dataset_two(spark),
    )

    rows = {row["department"]: row for row in actual.collect()}

    assert rows["IT"]["total_sales_amount"] == Decimal("12000.00")
    assert rows["IT"]["call_success_percentage"] == 80.0

    assert rows["Marketing"]["total_sales_amount"] == Decimal("4000.00")
    assert rows["Marketing"]["call_success_percentage"] == 72.5


def test_create_top_3(spark):
    """Test top employees with success rate above 75 percent."""
    actual = create_top_3(
        create_dataset_one(spark),
        create_dataset_two(spark),
    )

    expected_schema = StructType(
        [
            StructField("department", StringType(), True),
            StructField("name", StringType(), True),
            StructField("sales_amount", DecimalType(18, 2), True),
            StructField("success_percentage", DoubleType(), True),
            StructField("rank", IntegerType(), False),
        ]
    )

    expected = spark.createDataFrame(
        [
            ("IT", "Alice", Decimal("5000.00"), 90.0, 1),
            ("IT", "Bob", Decimal("4000.00"), 80.0, 2),
            ("Marketing", "David", Decimal("2500.00"), 85.0, 1),
        ],
        schema=expected_schema,
    )

    assert_df_equality(
        actual.orderBy("department", "rank"),
        expected.orderBy("department", "rank"),
    )


def test_create_top_3_products_netherlands(spark):
    """Test top products by quantity per department in Netherlands."""
    actual = create_top_3_products_netherlands(
        create_dataset_one(spark),
        create_dataset_two(spark),
        create_dataset_three(spark),
    )

    rows = {
        (row["department"], row["product_sold"]): row["total_quantity_sold"]
        for row in actual.collect()
    }

    assert rows[("IT", "Phone")] == 20
    assert rows[("IT", "Laptop")] == 15
    assert rows[("Marketing", "Laptop")] == 30

    assert actual.filter(actual.department == "Marketing").count() == 1


def test_create_best_salesperson_per_country(spark):
    """Test best salesperson by quantity sold per country."""
    actual = create_best_salesperson_per_country(
        create_dataset_one(spark),
        create_dataset_two(spark),
        create_dataset_three(spark),
    )

    rows = {row["country"]: row["name"] for row in actual.collect()}

    assert rows["Netherlands"] == "David"
    assert rows["Germany"] == "David"


def test_gold_writes_local_outputs(spark):
    """Run the Gold stage against local Silver data and persist its outputs."""
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

    outputs = run_gold_transformation(
        str(SILVER_OUTPUT_PATH),
        str(GOLD_OUTPUT_PATH),
    )

    for output_name in outputs:
        assert (GOLD_OUTPUT_PATH / output_name).is_dir()
