from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """Get the existing SparkSession from the execution environment."""
    return SparkSession.builder.getOrCreate()
