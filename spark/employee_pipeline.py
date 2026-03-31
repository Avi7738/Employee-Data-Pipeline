import os
import re
import logging
from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, DateType

# Logging setup 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("EmployeePipeline")

# Config
PG_HOST     = os.getenv("PG_HOST",     "postgres")
PG_PORT     = os.getenv("PG_PORT",     "5432")
PG_DB       = os.getenv("PG_DB",       "employee_db")
PG_USER     = os.getenv("PG_USER",     "admin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "admin123")

JDBC_URL    = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_DRIVER = "org.postgresql.Driver"

INPUT_CSV   = "/opt/spark-data/employees_raw.csv"
TODAY       = date.today().isoformat()      # e.g. "2026-03-30"

# Spark session

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EmployeeDataPipeline")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

# Step 1: Read raw CSV

def read_raw(spark: SparkSession) -> DataFrame:
    log.info("Reading raw CSV: %s", INPUT_CSV)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")   # keep everything as string initially
        .option("multiLine", "true")      # handles quoted fields with commas
        .option("escape", '"')
        .csv(INPUT_CSV)
    )
    log.info("Raw record count: %d", df.count())
    return df

# Step 2: Data Quality – flag bad rows 

EMAIL_REGEX = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"

def apply_quality_flags(df: DataFrame) -> DataFrame:
    """
    Adds boolean flag columns:
      _missing_critical  – required fields are null / empty
      _duplicate         – employee_id appears more than once (keep first)
      _invalid_email     – email does not match RFC-ish pattern
      _future_hire_date  – hire_date is in the future
    """
    log.info("Applying data-quality flags …")

    # 2a. Missing critical fields 
    df = df.withColumn(
        "_missing_critical",
        F.col("employee_id").isNull()
        | (F.trim(F.col("employee_id")) == "")
        | F.col("first_name").isNull()
        | (F.trim(F.col("first_name")) == "")
        | F.col("last_name").isNull()
        | (F.trim(F.col("last_name")) == "")
        | F.col("email").isNull()
        | (F.trim(F.col("email")) == "")
        | F.col("hire_date").isNull()
        | (F.trim(F.col("hire_date")) == ""),
    )

    # 2b. Duplicate employee_id (flag all but the first occurrence)
    from pyspark.sql.window import Window
    win = Window.partitionBy("employee_id").orderBy(F.monotonically_increasing_id())
    df = df.withColumn("_row_num", F.row_number().over(win))
    df = df.withColumn("_duplicate", F.col("_row_num") > 1).drop("_row_num")

    # 2c. Invalid email
    df = df.withColumn(
        "_invalid_email",
        ~F.col("email").rlike(EMAIL_REGEX),
    )

    # 2d. Future hire date
    df = df.withColumn(
        "_hire_date_parsed",
        F.to_date(F.col("hire_date"), "yyyy-MM-dd"),
    )
    df = df.withColumn(
        "_future_hire_date",
        F.col("_hire_date_parsed") > F.lit(TODAY),
    )

    return df

# Step 3: Split clean vs rejected 

def split_clean_rejected(df: DataFrame):
    """
    Returns (clean_df, rejected_df).
    A row is rejected if it has a missing critical field, is a duplicate,
    has an invalid email, OR has a future hire date.
    """
    reject_cond = (
        F.col("_missing_critical")
        | F.col("_duplicate")
        | F.col("_invalid_email")
        | F.col("_future_hire_date")
    )

    rejected = df.filter(reject_cond).select(
        F.concat_ws("|", *[c for c in df.columns if not c.startswith("_")]).alias("raw_data"),
        F.when(F.col("_missing_critical"),  "Missing critical field")
         .when(F.col("_duplicate"),         "Duplicate employee_id")
         .when(F.col("_invalid_email"),     "Invalid email format")
         .when(F.col("_future_hire_date"),  "Future hire date")
         .otherwise("Multiple issues").alias("reject_reason"),
    )

    clean = df.filter(~reject_cond).drop(
        "_missing_critical", "_duplicate", "_invalid_email",
        "_future_hire_date", "_hire_date_parsed",
    )

    log.info("Clean rows   : %d", clean.count())
    log.info("Rejected rows: %d", rejected.count())
    return clean, rejected

# Step 4: Transformations

def clean_salary(salary_col):
    """Strips $, commas → numeric string → cast to Decimal."""
    return (
        F.regexp_replace(
            F.regexp_replace(salary_col, r"[\$,]", ""),
            r"\s+", ""
        )
    )


def salary_band(salary_col):
    """Junior < 50k | Mid 50k–80k | Senior > 80k."""
    return (
        F.when(salary_col < 50_000, "Junior")
         .when(salary_col <= 80_000, "Mid")
         .otherwise("Senior")
    )


def apply_transformations(df: DataFrame) -> DataFrame:
    log.info("Applying transformations …")

    # Name standardisation (Proper Case)
    df = (
        df
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name",  F.initcap(F.trim(F.col("last_name"))))
    )

    # Email → lowercase
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))

    # Salary cleaning → Decimal 
    df = (
        df
        .withColumn("salary_str",    clean_salary(F.col("salary")))
        .withColumn("salary",
                    F.col("salary_str").cast(DecimalType(10, 2)))
        .drop("salary_str")
    )

    # Salary band
    df = df.withColumn("salary_band", salary_band(F.col("salary")))

    # Date columns → proper DateType
    df = (
        df
        .withColumn("hire_date",  F.to_date(F.col("hire_date"),  "yyyy-MM-dd"))
        .withColumn("birth_date", F.to_date(F.col("birth_date"), "yyyy-MM-dd"))
    )

    # Age calculation (years from birth_date to today)
    df = df.withColumn(
        "age",
        F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365.25)
         .cast(IntegerType()),
    )

    # Tenure calculation (years from hire_date to today)
    df = df.withColumn(
        "tenure_years",
        F.round(F.datediff(F.current_date(), F.col("hire_date")) / 365.25, 1)
         .cast(DecimalType(3, 1)),
    )

    # Department / status normalisation
    df = (
        df
        .withColumn("department", F.initcap(F.trim(F.lower(F.col("department")))))
        .withColumn("status",     F.initcap(F.trim(F.lower(F.col("status")))))
    )

    # Numeric cast for IDs
    df = (
        df
        .withColumn("employee_id", F.col("employee_id").cast(IntegerType()))
        .withColumn("manager_id",  F.col("manager_id").cast(IntegerType()))
    )

    return df

# Step 5: Enrichment 

def apply_enrichment(df: DataFrame) -> DataFrame:
    log.info("Adding enrichment columns …")

    # full_name
    df = df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")),
    )

    # email_domain  (part after @)
    df = df.withColumn(
        "email_domain",
        F.regexp_extract(F.col("email"), r"@(.+)$", 1),
    )

    return df

# Step 6: Select final columns in DB order 

FINAL_COLUMNS = [
    "employee_id", "first_name", "last_name", "full_name",
    "email", "email_domain", "hire_date", "job_title", "department",
    "salary", "salary_band", "manager_id",
    "address", "city", "state", "zip_code",
    "birth_date", "age", "tenure_years", "status",
]

def select_final(df: DataFrame) -> DataFrame:
    return df.select(*FINAL_COLUMNS)

# Step 7: Write to PostgreSQL

def write_to_postgres(df: DataFrame, table: str, mode: str = "append") -> None:
    log.info("Writing %d rows to table '%s' …", df.count(), table)
    (
        df.write
        .format("jdbc")
        .option("url",      JDBC_URL)
        .option("dbtable",  table)
        .option("user",     PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver",   JDBC_DRIVER)
        .mode(mode)
        .save()
    )
    log.info("✅  Write to '%s' complete.", table)

# Main

def main():
    log.info("=== Employee Data Pipeline starting ===")
    spark = create_spark_session()

    try:
        # 1. Read
        raw_df = read_raw(spark)

        # 2. Quality flags
        flagged_df = apply_quality_flags(raw_df)

        # 3. Split
        clean_df, rejected_df = split_clean_rejected(flagged_df)

        # 4. Transform
        transformed_df = apply_transformations(clean_df)

        # 5. Enrich
        enriched_df = apply_enrichment(transformed_df)

        # 6. Final column selection
        final_df = select_final(enriched_df)
        final_df = final_df.dropDuplicates(["employee_id"])
        final_df.cache()

        # 7. Show sample for verification
        log.info("Sample output (5 rows):")
        final_df.show(5, truncate=False)

        # 8. Write clean data
        write_to_postgres(final_df, "employees_clean", mode="append")

        # 9. Write rejected data
        write_to_postgres(rejected_df, "employees_rejected", mode="append")

        log.info("=== Pipeline finished successfully ===")

    except Exception as exc:
        log.exception("Pipeline failed: %s", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
