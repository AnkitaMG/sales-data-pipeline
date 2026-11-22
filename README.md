# Sales Data Processing Pipeline

## 1. Project Overview

This project implements a data processing pipeline using **Python and PySpark 3.5.0**.

It processes employee, sales, and transaction datasets using the Medallion Architecture, consisting of three layers:

- **Bronze:** Raw data ingestion and storage.
- **Silver:** Data cleaning and transformation.
- **Gold:** Business-level transformations and aggregations.

The pipeline generates multiple business-ready outputs in CSV format.

## 2. Technologies Used

- Python 3.10
- Apache Spark / PySpark 3.5.0
- Poetry – dependency and environment management
- Pytest – unit and integration testing
- Chispa – PySpark DataFrame testing
- Parquet – intermediate data storage
- CSV – input and final output format
- Python Logging – application logging

## 3. Project Structure

```text
sales-data-pipeline/
│
├── data/
│   ├── input/
│   │   └── raw/
│   │       ├── dataset_one.csv
│   │       ├── dataset_two.csv
│   │       └── dataset_three.csv
│   │
│   └── output/
│       ├── bronze/
│       ├── silver/
│       └── gold/
│
├── src/
│   └── sales_data/
│       ├── __init__.py
│       ├── main.py
│       │
│       ├── common/
│       │   ├── __init__.py
│       │   └── data_io.py
│       │
│       ├── ingestion/
│       │   ├── __init__.py
│       │   └── bronze_ingestion.py
│       │
│       ├── transformations/
│       │   ├── __init__.py
│       │   ├── silver_transform.py
│       │   └── gold_transform.py
│       │
│       ├── validation/
│       │   └── __init__.py
│       │   └── data_quality.py
│       │
│       └── utils/
│           ├── __init__.py
│           └── config.py
│           └── logger.py
│           └── schemas.py
│           └── spark.py
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
│
├── resources/
│   └── sales_job.yml
│
├── logs/
├── .pre-commit-config.yaml
├── databricks.yml
├── pyproject.toml
├── poetry.lock
└── README.md
```

## 4. Input Datasets

The pipeline processes three CSV datasets.

| Dataset | Description |
|---|---|
| dataset_one.csv | Employee ID, expertise area, calls made, and successful calls |
| dataset_two.csv | Employee ID, employee name, address, and sales amount |
| dataset_three.csv | Transaction ID, caller ID, company, recipient, age, country, product sold, and quantity |

All input datasets are stored under:

```text
data/input/raw/
```

For Databricks, upload the three CSV files to the `input/` directory of a
Unity Catalog volume. The Bronze job expects `dataset_one.csv`,
`dataset_two.csv`, and `dataset_three.csv` under its configured input path.
Each layer has independent input and output bundle variables, so the pipeline
can read from and write to different locations.

Bronze quarantines rows where `calls_successful` exceeds `calls_made` as
Parquet under `output/errors/dataset_one/<UTC timestamp>/`. Those rows are
excluded from the Bronze dataset; other strict quality checks still apply.

## 5. Pipeline Architecture

The pipeline follows the Medallion Architecture.

### 5.1 Bronze Layer – Data Ingestion

**Implementation:** `src/sales_data/ingestion/bronze_ingestion.py`

The Bronze layer:

- Reads the three raw CSV datasets.
- Applies predefined schemas from `schemas.py`.
- Preserves all source columns as strings.
- Stores the ingested datasets in Parquet format.

**Bronze output:**

```text
data/output/bronze/
├── dataset_one/
├── dataset_two/
└── dataset_three/
```

### 5.2 Silver Layer – Data Cleaning and Transformation

**Implementation:** `src/sales_data/transformations/silver_transform.py`

The Silver layer:

- Reads datasets from the Bronze layer.
- Trims whitespace from string columns.
- Converts empty strings into null values.
- Casts numeric columns to appropriate data types.
- Removes duplicate records in the current implementation.
- Writes the transformed datasets in Parquet format.

**Silver output:**

```text
data/output/silver/
├── dataset_one/
├── dataset_two/
└── dataset_three/
```

### 5.3 Gold Layer – Business Transformations

**Implementation:** `src/sales_data/transformations/gold_transform.py`

The Gold layer joins employee and transaction datasets and generates business-level outputs.

| Output | Description |
|---|---|
| it_data | Top 100 IT employees ordered by sales amount |
| marketing_address_info | Marketing employee addresses split into street, house number, and ZIP code |
| department_breakdown | Total sales amount and call success percentage per department |
| top_3 | Top three employees per department with call success rate above 75% |
| top_3_most_sold_per_department_netherlands | Top three products by quantity sold per department in the Netherlands |
| best_salesperson | Best salesperson per country based on total quantity sold |

**Gold output:**

```text
data/output/gold/
├── it_data/
├── marketing_address_info/
├── department_breakdown/
├── top_3/
├── top_3_most_sold_per_department_netherlands/
└── best_salesperson/
```

Each Gold output is written as a CSV dataset.

## 6. Prerequisites

Install the following:

- Python 3.10
- Poetry
- Java 11 or a compatible Java runtime for Spark 3.5.0

Verify the installations:

```bash
python3.10 --version
poetry --version
java -version
```

## 7. Setup and Installation

### Step 1: Clone the repository

```bash
git clone https://github.com/AnkitaMG/sales-data-pipeline.git
cd sales-data-pipeline
```

### Step 2: Configure Python environment

```bash
poetry env use python3.10
```

### Step 3: Install dependencies

```bash
poetry install
```

This installs the project dependencies defined in `pyproject.toml`.

All commands below can be executed using `poetry run`, without manually activating the virtual environment.

## 8. Running the Pipeline

Run the stages in order; each stage writes its output to the matching
folder under `data/output/`.

### 8.1 Run Bronze Ingestion

Reads the raw CSV files, applies schemas, and generates Bronze Parquet datasets.
Output: `data/output/bronze/`.

```bash
poetry run pytest tests/test_bronze.py
```

### 8.2 Run Silver Transformation

Reads Bronze datasets, performs cleaning and type conversions, and generates Silver Parquet datasets.
Output: `data/output/silver/`.

```bash
poetry run pytest tests/test_silver.py
```

### 8.3 Run Gold Transformation

Reads Silver datasets, performs business transformations, and generates Gold CSV outputs.
Output: `data/output/gold/`.

```bash
poetry run pytest tests/test_gold.py
```

**Execution order:** Bronze → Silver → Gold.

### Databricks layer paths

The bundle exposes `bronze_input_path`, `bronze_output_path`,
`silver_input_path`, `silver_output_path`, `gold_input_path`, and
`gold_output_path`. Bronze quarantine rows use `bronze_errors_path`; Bronze
strict quality behavior is controlled by `strict_data_quality`, which defaults
to `true`. Override the bundle variables at deployment time to set defaults.
Each layer job has only its own parameters. The orchestrator passes the
configured values to each layer task. The orchestrator exposes only the
`strict_data_quality` parameter so strict checking can be changed per run;
running an individual layer job lets you override that layer's path parameters
in its Run now dialog.

```bash
databricks bundle deploy -t dev \
  --var="bronze_input_path=/Volumes/main/default/sales/input" \
  --var="bronze_output_path=/Volumes/main/default/sales/output/bronze" \
  --var="bronze_errors_path=/Volumes/main/default/sales/output/errors" \
  --var="bronze_strict_data_quality=true" \
  --var="silver_input_path=/Volumes/main/default/sales/output/bronze" \
  --var="silver_output_path=/Volumes/main/default/sales/output/silver" \
  --var="gold_input_path=/Volumes/main/default/sales/output/silver" \
  --var="gold_output_path=/Volumes/main/default/sales/output/gold"

databricks bundle run -t dev sales_pipeline

# For a one-off run that logs quality warnings and continues:
databricks bundle run -t dev \
  --params "strict_data_quality=false" \
  sales_pipeline

# To run one layer with per-run path overrides:
databricks bundle run -t dev \
  --params "bronze_input_path=/Volumes/main/default/sales/input,bronze_output_path=/Volumes/main/default/sales/output/bronze,bronze_errors_path=/Volumes/main/default/sales/output/errors" \
  bronze_job
```

To run Bronze without halting when a data quality check fails, override the
strict parameter:

```bash
databricks bundle run -t dev \
  --params "strict_data_quality=false" \
  bronze_job
```

## 9. Running Tests

The project uses Pytest for unit and integration testing and Chispa for comparing PySpark DataFrames.

The Bronze, Silver, and Gold test files also write their layer outputs under
`data/output/`. Silver and Gold tests create any missing upstream layer first,
so each can be run on its own.

### 9.1 Run all tests

```bash
poetry run pytest
```

### 9.2 Run all tests with detailed output

```bash
poetry run pytest -v
```

### 9.3 Run Bronze tests

```bash
poetry run pytest tests/test_bronze.py -v
```

### 9.4 Run Silver tests

```bash
poetry run pytest tests/test_silver.py
```

### 9.5 Run Gold tests

```bash
poetry run pytest tests/test_gold.py -v
```

### 9.6 Display application logs during tests

```bash
poetry run pytest -v -o log_cli=true --log-cli-level=INFO
```

## 10. Code quality and CI

Ruff checks lint rules and formatting, mypy checks the CLI entry module, and
pre-commit runs those checks before commits. GitHub Actions runs the same
quality checks and the test suite on pushes and pull requests.

```bash
poetry run ruff check .
poetry run ruff format --check .
poetry run mypy
poetry run pre-commit install
```

### Example test result

```text
======================== test session starts ========================
collected 6 items

tests/test_gold.py ......                                     [100%]

========================= 6 passed ================================
```

## 11. Logging

The application uses Python's built-in logging module and always logs to the
console. Rotating file logging can be enabled with `SALES_DATA_LOG_FILE`.
Log files rotate at 5 MiB by default, keeping five backup files. Configure the
rotation size and backup count with `SALES_DATA_LOG_MAX_BYTES` and
`SALES_DATA_LOG_BACKUP_COUNT`.

For example, in zsh or bash:

```bash
export SALES_DATA_LOG_FILE="logs/sales-data.log"
export SALES_DATA_LOG_MAX_BYTES=10485760
export SALES_DATA_LOG_BACKUP_COUNT=3
poetry run python scripts/run_bronze.py
```

In Databricks environments with read-only local storage, leave
`SALES_DATA_LOG_FILE` unset and use the task's driver logs.

## 12. Dependency Management with Poetry

All project dependencies are managed through Poetry.

Add a production dependency:

```bash
poetry add package-name
```

Add a development dependency:

```bash
poetry add --group dev package-name
```

View installed dependencies:

```bash
poetry show
```

Update dependencies:

```bash
poetry update
```

## 13. Future Improvements

- Deploy the pipeline in Databricks using CI.
- Pytest improvements