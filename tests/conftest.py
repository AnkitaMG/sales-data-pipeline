import pytest

from sales_data.utils.spark import get_spark_session


@pytest.fixture(scope="session")
def spark():
    """Create one SparkSession for the test session."""

    session = get_spark_session()
    yield session
    session.stop()
