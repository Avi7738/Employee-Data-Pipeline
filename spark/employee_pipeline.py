import os
import logging
from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DecimalType

# Logging
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
TODAY       = date.today().isoformat()

EMAIL_REGEX = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"

# Step 1: Spark Session
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EmployeeDataPipeline")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

# Step 2: Read raw CSV
def read_raw(spark: SparkSession) -> DataFrame:
    log.info("Reading raw CSV: %s", INPUT_CSV)
    df = (
        spark.read
        .option("header",    "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape",    '"')
        .csv(INPUT_CSV)
    )
    log.info("Raw record count: %d", df.count())
    return df

# Step 3: Data Quality Flags
def apply_quality_flags(df: DataFrame) -> DataFrame:
    """
    Adds boolean flag columns:
      _missing_critical  - required fields are null/empty
      _duplicate         - employee_id OR email appears more than once (keep first)
      _invalid_email     - email does not match valid format
      _future_hire_date  - hire_date is in the future
    """
    log.info("Applying data-quality flags ...")

    # 3a. Missing critical fields
    df = df.withColumn(
        "_missing_critical",
        F.col("employee_id").isNull() | (F.trim(F.col("employee_id")) == "")
        | F.col("first_name").isNull()  | (F.trim(F.col("first_name")) == "")
        | F.col("last_name").isNull()   | (F.trim(F.col("last_name")) == "")
        | F.col("email").isNull()       | (F.trim(F.col("email")) == "")
        | F.col("hire_date").isNull()   | (F.trim(F.col("hire_date")) == ""),
    )

    # 3b. Duplicate employee_id - keep first occurrence only
    win_id = Window.partitionBy("employee_id").orderBy(F.monotonically_increasing_id())
    df = df.withColumn("_rn_id", F.row_number().over(win_id))
    df = df.withColumn("_dup_id", F.col("_rn_id") > 1).drop("_rn_id")

    # 3c. Duplicate email (normalise to lowercase first) - keep first occurrence only
    df = df.withColumn("_email_norm", F.lower(F.trim(F.col("email"))))
    win_email = Window.partitionBy("_email_norm").orderBy(F.monotonically_increasing_id())
    df = df.withColumn("_rn_email", F.row_number().over(win_email))
    df = df.withColumn("_dup_email", F.col("_rn_email") > 1).drop("_rn_email", "_email_norm")

    # Combined duplicate flag
    df = df.withColumn("_duplicate", F.col("_dup_id") | F.col("_dup_email")) \
           .drop("_dup_id", "_dup_email")

    # 3d. Invalid email format
    df = df.withColumn("_invalid_email", ~F.col("email").rlike(EMAIL_REGEX))

    # 3e. Future hire date
    df = df.withColumn("_hire_date_parsed", F.to_date(F.col("hire_date"), "yyyy-MM-dd"))
    df = df.withColumn("_future_hire_date", F.col("_hire_date_parsed") > F.lit(TODAY))

    return df

# Step 4: Split clean vs rejected
def split_clean_rejected(df: DataFrame):
    reject_cond = (
        F.col("_missing_critical")
        | F.col("_duplicate")
        | F.col("_invalid_email")
        | F.col("_future_hire_date")
    )

    raw_cols = [c for c in df.columns if not c.startswith("_")]

    rejected = df.filter(reject_cond).select(
        F.concat_ws("|", *raw_cols).alias("raw_data"),
        F.when(F.col("_missing_critical"), "Missing critical field")
         .when(F.col("_duplicate"),        "Duplicate employee_id or email")
         .when(F.col("_invalid_email"),    "Invalid email format")
         .when(F.col("_future_hire_date"), "Future hire date")
         .otherwise("Multiple issues").alias("reject_reason"),
    )

    clean = df.filter(~reject_cond).drop(
        "_missing_critical", "_duplicate", "_invalid_email",
        "_future_hire_date", "_hire_date_parsed",
    )

    log.info("Clean rows   : %d", clean.count())
    log.info("Rejected rows: %d", rejected.count())
    return clean, rejected

# Step 5: Transformations
def apply_transformations(df: DataFrame) -> DataFrame:
    log.info("Applying transformations ...")

    # Name standardisation - Proper Case
    df = (
        df
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name",  F.initcap(F.trim(F.col("last_name"))))
    )

    # Email - lowercase
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))

    # Salary - strip $, commas -> Decimal
    df = (
        df
        .withColumn("salary_str",
                    F.regexp_replace(F.regexp_replace(F.col("salary"), r"[\$,]", ""), r"\s+", ""))
        .withColumn("salary", F.col("salary_str").cast(DecimalType(10, 2)))
        .drop("salary_str")
    )

    # Salary band
    df = df.withColumn(
        "salary_band",
        F.when(F.col("salary") < 50000,  "Junior")
         .when(F.col("salary") <= 80000, "Mid")
         .otherwise("Senior"),
    )

    # Date columns
    df = (
        df
        .withColumn("hire_date",  F.to_date(F.col("hire_date"),  "yyyy-MM-dd"))
        .withColumn("birth_date", F.to_date(F.col("birth_date"), "yyyy-MM-dd"))
    )

    # Age (years from birth_date to today)
    df = df.withColumn(
        "age",
        F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365.25).cast(IntegerType()),
    )

    # Tenure (years from hire_date to today)
    df = df.withColumn(
        "tenure_years",
        F.round(F.datediff(F.current_date(), F.col("hire_date")) / 365.25, 1)
         .cast(DecimalType(3, 1)),
    )

    # Normalise department and status
    df = (
        df
        .withColumn("department", F.initcap(F.trim(F.lower(F.col("department")))))
        .withColumn("status",     F.initcap(F.trim(F.lower(F.col("status")))))
    )

    # Cast numeric IDs
    df = (
        df
        .withColumn("employee_id", F.col("employee_id").cast(IntegerType()))
        .withColumn("manager_id",  F.col("manager_id").cast(IntegerType()))
    )

    return df

# Step 6: Enrichment
def apply_enrichment(df: DataFrame) -> DataFrame:
    log.info("Adding enrichment columns ...")

    df = df.withColumn("full_name",
                       F.concat_ws(" ", F.col("first_name"), F.col("last_name")))

    df = df.withColumn("email_domain",
                       F.regexp_extract(F.col("email"), r"@(.+)$", 1))

    return df

# Step 7: Select final columns
FINAL_COLUMNS = [
    "employee_id", "first_name", "last_name", "full_name",
    "email", "email_domain", "hire_date", "job_title", "department",
    "salary", "salary_band", "manager_id",
    "address", "city", "state", "zip_code",
    "birth_date", "age", "tenure_years", "status",
]

def select_final(df: DataFrame) -> DataFrame:
    return df.select(*FINAL_COLUMNS)

# Step 8: Write to PostgreSQL (IDEMPOTENT - overwrite on every run)
def write_to_postgres(df: DataFrame, table: str) -> None:
    """
    Clears the table and re-inserts all rows on every run.
    truncate=true preserves the table DDL (indexes, constraints, views).
    mode=overwrite triggers the truncate-then-insert behaviour in Spark JDBC.
    Running this job 10 times produces the same result as running it once.
    """
    log.info("Writing %d rows to '%s' (truncate + insert) ...", df.count(), table)
    (
        df.write
        .format("jdbc")
        .option("url",      JDBC_URL)
        .option("dbtable",  table)
        .option("user",     PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver",   JDBC_DRIVER)
        .option("truncate", "true")   # TRUNCATE rows, keep DDL/indexes/constraints
        .mode("overwrite")
        .save()
    )
    log.info("Write to '%s' complete.", table)

# Main
def main():
    log.info("=== Employee Data Pipeline starting ===")
    spark = create_spark_session()

    try:
        raw_df        = read_raw(spark)
        flagged_df    = apply_quality_flags(raw_df)
        clean_df, rejected_df = split_clean_rejected(flagged_df)
        transformed_df = apply_transformations(clean_df)
        enriched_df   = apply_enrichment(transformed_df)
        final_df      = select_final(enriched_df)
        final_df.cache()

        log.info("Sample output (5 rows):")
        final_df.show(5, truncate=False)

        write_to_postgres(final_df,    "employees_clean")
        write_to_postgres(rejected_df, "employees_rejected")

        log.info("=== Pipeline finished successfully ===")

    except Exception as exc:
        log.exception("Pipeline failed: %s", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
