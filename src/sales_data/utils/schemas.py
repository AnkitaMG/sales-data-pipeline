from pyspark.sql.types import StructType, StructField, StringType


# Dataset 1: Employee call statistics
DATASET_ONE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("area", StringType(), True),
        StructField("calls_made", StringType(), True),
        StructField("calls_successful", StringType(), True),
    ]
)


# Dataset 2: Employee and sales information
DATASET_TWO_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("sales_amount", StringType(), True),
    ]
)


# Dataset 3: Sales transaction information
DATASET_THREE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("caller_id", StringType(), True),
        StructField("company", StringType(), True),
        StructField("recipient", StringType(), True),
        StructField("age", StringType(), True),
        StructField("country", StringType(), True),
        StructField("product_sold", StringType(), True),
        StructField("quantity", StringType(), True),
    ]
)
