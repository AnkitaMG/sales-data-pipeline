from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from sales_data.utils.spark import get_spark_session

from sales_data.common.data_io import (
    read_parquet_data,
    write_csv_data,
)
from sales_data.utils.logger import setup_logger

logger = setup_logger()


def join_employee_data(
    dataset_one: DataFrame,
    dataset_two: DataFrame,
) -> DataFrame:
    """
    Join employee call statistics with personal and sales information.

    :param dataset_one: Employee department and call statistics.
    :param dataset_two: Employee name, address and sales information.
    :return: Combined employee DataFrame.
    """
    logger.info("Joining employee datasets")
    return dataset_one.join(dataset_two, on="id", how="inner")


def create_it_data(
    dataset_one: DataFrame,
    dataset_two: DataFrame,
) -> DataFrame:
    """
    Return the top 100 IT employees ordered by sales amount.

    :param dataset_one: Employee call statistics.
    :param dataset_two: Employee sales information.
    :return: Top 100 IT employees.
    """
    logger.info("Creating IT data output")

    return (
        join_employee_data(dataset_one, dataset_two)
        .filter(F.col("area") == "IT")
        .orderBy(F.col("sales_amount").desc())
        .limit(100)
    )


def create_marketing_address_info(
    dataset_one: DataFrame,
    dataset_two: DataFrame,
) -> DataFrame:
    """
    Return Marketing employee addresses with ZIP codes separated.

    Expected address format:
    street name, house number, ZIP code.

    :param dataset_one: Employee call statistics.
    :param dataset_two: Employee sales information.
    :return: Marketing addresses with separate ZIP code column.
    """
    logger.info("Creating Marketing address output")

    marketing = join_employee_data(dataset_one, dataset_two).filter(
        F.col("area") == "Marketing"
    )

    address_parts = F.split(F.col("address"), r"\s*,\s*")

    # Keep only complete, three-part addresses. Returning null placeholders for malformed or missing addresses creates blank rows in the CSV output.
    valid_marketing = marketing.filter(
        (F.size(address_parts) == 3)
        & address_parts.getItem(0).isNotNull()
        & (F.trim(address_parts.getItem(0)) != "")
        & address_parts.getItem(1).isNotNull()
        & (F.trim(address_parts.getItem(1)) != "")
        & address_parts.getItem(2).isNotNull()
        & (F.trim(address_parts.getItem(2)) != "")
    )

    return valid_marketing.select(
        address_parts.getItem(0).alias("street"),
        address_parts.getItem(1).alias("house_number"),
        address_parts.getItem(2).alias("zip_code"),
    )


def create_department_breakdown(
    dataset_one: DataFrame,
    dataset_two: DataFrame,
) -> DataFrame:
    """
    Calculate sales amount and call success percentage per department.

    Success percentage is calculated as:
    total successful calls / total calls made * 100.

    :param dataset_one: Employee call statistics.
    :param dataset_two: Employee sales information.
    :return: Department-level sales and call success metrics.
    """
    logger.info("Creating department breakdown output")

    employees = join_employee_data(dataset_one, dataset_two)

    breakdown = (
        employees.groupBy("area")
        .agg(
            F.sum("sales_amount").alias("total_sales"),
            F.sum("calls_made").alias("total_calls_made"),
            F.sum("calls_successful").alias("total_calls_successful"),
        )
        .withColumn(
            "success_percentage",
            F.when(
                F.col("total_calls_made") > 0,
                F.col("total_calls_successful") / F.col("total_calls_made") * 100,
            ).otherwise(F.lit(0.0)),
        )
    )

    return breakdown.select(
        F.col("area").alias("department"),
        F.round("total_sales", 2).alias("total_sales_amount"),
        F.round("success_percentage", 2).alias("call_success_percentage"),
    )


def create_top_3(
    dataset_one: DataFrame,
    dataset_two: DataFrame,
) -> DataFrame:
    """
    Find the top three employees per department with a call
    success rate greater than 75 percent.

    Ranking is based on call success rate, then sales amount.

    :param dataset_one: Employee call statistics.
    :param dataset_two: Employee sales information.
    :return: Top three qualifying employees per department.
    """
    logger.info("Creating top 3 employee output")

    employees = (
        join_employee_data(dataset_one, dataset_two)
        .withColumn(
            "success_percentage",
            F.when(
                F.col("calls_made") > 0,
                F.col("calls_successful") / F.col("calls_made") * 100,
            ).otherwise(F.lit(0.0)),
        )
        .filter(F.col("success_percentage") > 75)
    )

    window = Window.partitionBy("area").orderBy(
        F.col("success_percentage").desc(),
        F.col("sales_amount").desc(),
    )

    return (
        employees.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= 3)
        .select(
            F.col("area").alias("department"),
            "name",
            F.col("sales_amount").alias("sales_amount"),
            F.round("success_percentage", 2).alias("success_percentage"),
            "rank",
        )
    )


def join_transactions_to_employees(
    dataset_one: DataFrame,
    dataset_two: DataFrame,
    dataset_three: DataFrame,
) -> DataFrame:
    """
    Join transactions with employee information.

    :param dataset_one: Employee call statistics.
    :param dataset_two: Employee sales information.
    :param dataset_three: Sales transactions.
    :return: Transactions enriched with employee name and department.
    """
    logger.info("Joining transactions with employee information")

    employees = join_employee_data(
        dataset_one,
        dataset_two,
    )

    employee_info = employees.select(
        F.col("id").alias("employee_id"),
        "name",
        "area",
    ).alias("e")

    transactions = dataset_three.alias("t")

    return transactions.join(
        employee_info,
        F.col("t.caller_id") == F.col("e.employee_id"),
        "inner",
    ).select(
        "t.*",
        F.col("e.name").alias("name"),
        F.col("e.area").alias("area"),
    )


def create_top_3_products_netherlands(
    dataset_one: DataFrame,
    dataset_two: DataFrame,
    dataset_three: DataFrame,
) -> DataFrame:
    """
    Find the three most sold products per department in the Netherlands.

    Products are ranked by the sum of transaction quantities.

    :param dataset_one: Employee call statistics.
    :param dataset_two: Employee sales information.
    :param dataset_three: Sales transactions.
    :return: Top three products per department.
    """
    logger.info("Creating Netherlands product output")

    transactions = join_transactions_to_employees(
        dataset_one,
        dataset_two,
        dataset_three,
    )

    product_sales = (
        transactions.filter(F.upper(F.trim(F.col("country"))) == "NETHERLANDS")
        .groupBy("area", "product_sold")
        .agg(F.sum("quantity").alias("total_quantity_sold"))
    )

    window = Window.partitionBy("area").orderBy(
        F.col("total_quantity_sold").desc(),
        F.col("product_sold").asc(),
    )

    return (
        product_sales.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= 3)
        .select(
            F.col("area").alias("department"),
            "product_sold",
            "total_quantity_sold",
            "rank",
        )
    )


def create_best_salesperson_per_country(
    dataset_one: DataFrame,
    dataset_two: DataFrame,
    dataset_three: DataFrame,
) -> DataFrame:
    """
    Find the salesperson with the highest quantity sold per country.

    :param dataset_one: Employee call statistics.
    :param dataset_two: Employee sales information.
    :param dataset_three: Sales transactions.
    :return: Best salesperson per country.
    """
    logger.info("Creating best salesperson output")

    transactions = join_transactions_to_employees(
        dataset_one,
        dataset_two,
        dataset_three,
    )

    salesperson_sales = transactions.groupBy(
        "country", "caller_id", "name", "area"
    ).agg(F.sum("quantity").alias("total_quantity_sold"))

    window = Window.partitionBy("country").orderBy(
        F.col("total_quantity_sold").desc(),
        F.col("name").asc(),
    )

    return (
        salesperson_sales.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .select(
            "country",
            "name",
            F.col("area").alias("department"),
            "total_quantity_sold",
        )
    )


def write_gold_data(
    dataframe: DataFrame,
    output_path: str,
) -> None:
    """
    Write a Gold DataFrame as CSV.

    :param dataframe: Gold output DataFrame.
    :param output_path: Destination directory.
    """
    logger.info("Writing Gold data to %s", output_path)
    write_csv_data(dataframe, output_path)


def run_gold_transformation(
    silver_path,
    gold_path,
) -> Dict[str, DataFrame]:
    """
    Execute all Gold transformations and write their outputs.

    :param spark: Active SparkSession.
    :param silver_path: Root directory containing Silver datasets.
    :param gold_path: Root directory for Gold outputs.
    :return: Dictionary containing all six Gold DataFrames.
    """
    logger.info("Starting Gold transformation")

    spark = get_spark_session()

    # Step 1: Read Silver datasets
    logger.info("Reading Silver datasets from %s", silver_path)

    dataset_one = read_parquet_data(spark, f"{silver_path}/dataset_one")

    dataset_two = read_parquet_data(spark, f"{silver_path}/dataset_two")

    dataset_three = read_parquet_data(spark, f"{silver_path}/dataset_three")

    # Step 2: Create Gold datasets
    logger.info("Creating all six Gold datasets")

    outputs = {
        "it_data": create_it_data(dataset_one, dataset_two),
        "marketing_address_info": create_marketing_address_info(
            dataset_one, dataset_two
        ),
        "department_breakdown": create_department_breakdown(dataset_one, dataset_two),
        "top_3": create_top_3(dataset_one, dataset_two),
        "top_3_most_sold_per_department_netherlands": create_top_3_products_netherlands(
            dataset_one, dataset_two, dataset_three
        ),
        "best_salesperson": create_best_salesperson_per_country(
            dataset_one, dataset_two, dataset_three
        ),
    }

    # Step 3: Write Gold datasets
    for output_name, dataframe in outputs.items():
        logger.info("Processing Gold output: %s", output_name)

        write_gold_data(
            dataframe,
            f"{gold_path}/{output_name}",
        )

        logger.info("Successfully generated: %s", output_name)

    logger.info("Gold transformation completed successfully")

    return outputs
