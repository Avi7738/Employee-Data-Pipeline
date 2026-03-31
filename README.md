# Employee Data Pipeline

An end-to-end data engineering pipeline using **Apache Spark**, **PostgreSQL**, and **Docker**.

---

## Table of Contents
1. [Project Structure](#project-structure)
2. [Prerequisites](#prerequisites)
3. [Quick Start (Step-by-Step)](#quick-start)
4. [Task Details](#task-details)
   - [Task 1 – Environment Setup](#task-1--environment-setup)
   - [Task 2 – Data Generation](#task-2--data-generation)
   - [Task 3 – Spark Cleaning & Transformation](#task-3--spark-cleaning--transformation)
   - [Task 4 – Database Loading](#task-4--database-loading)
5. [Data Quality Rules](#data-quality-rules)
6. [Transformation Logic](#transformation-logic)
7. [Verifying Results](#verifying-results)
8. [Troubleshooting](#troubleshooting)

---

## Project Structure

```
employee-pipeline/
├── docker-compose.yml              # Orchestrates Spark + PostgreSQL
├── drivers/
│   └── postgresql-42.7.3.jar       # JDBC driver (download via script)
├── data/
│   └── employees_raw.csv           # Generated sample input data
├── scripts/
│   ├── generate_employees.py       # Task 2 – data generation
│   └── download_jdbc_driver.sh     # Downloads JDBC JAR
├── spark/
│   └── employee_pipeline.py        # Task 3+4 – Spark job
└── sql/
    ├── init.sql                    # Task 4.1 – DB schema (auto-run by Docker)
    └── verify_data.sql             # Queries to confirm output
```

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Docker | 24+ | https://docs.docker.com/get-docker/ |
| Docker Compose | v2 | Included with Docker Desktop |
| Python | 3.10+ | https://python.org (local only, for data generation) |
| pip package: `faker` | latest | `pip install faker` |

---

## Quick Start

Follow every step in order.

### Step 1 – Clone / set up the project folder

```bash
# If starting fresh, create the folder structure
git clone <your-repo-url> employee-pipeline
cd employee-pipeline
```

### Step 2 – Download the PostgreSQL JDBC driver

Spark needs this JAR to talk to PostgreSQL. Run **once**:

```bash
bash scripts/download_jdbc_driver.sh
```

This places `postgresql-42.7.3.jar` inside the `drivers/` folder.

### Step 3 – Generate sample data

```bash
# Install the Faker library (only needed locally, not inside Docker)
pip install faker

# Generate  data/employees_raw.csv  (1 250 rows with intentional quality issues)
python scripts/generate_employees.py
```

Expected output:
```
✅  Generated 1250 rows → data/employees_raw.csv
   Duplicates injected : 50
   Total unique IDs    : 1200
```

### Step 4 – Start Docker services

```bash
docker-compose up -d
```

This starts:
- **PostgreSQL** on port `5432` — schema is created automatically via `sql/init.sql`
- **Spark Master** on port `8080` (Web UI) and `7077`
- **Spark Worker** connected to the master

Check everything is running:
```bash
docker-compose ps
```

All three containers should show status `Up`.

### Step 5 – Wait for PostgreSQL to be healthy

```bash
docker-compose logs postgres | grep "ready to accept connections"
```

You should see `database system is ready to accept connections`.

### Step 6 – Run the Spark pipeline

```bash
docker exec spark_master \
  spark-submit \
  --jars /opt/spark-drivers/postgresql-42.7.3.jar \
  /opt/spark-jobs/employee_pipeline.py
```

The job will log progress and finish with:
```
=== Pipeline finished successfully ===
```

### Step 7 – Verify results

Connect to PostgreSQL and run verification queries:

```bash
docker exec -it employee_postgres \
  psql -U admin -d employee_db -f /dev/stdin < sql/verify_data.sql
```

Or open an interactive session:
```bash
docker exec -it employee_postgres psql -U admin -d employee_db
```

Then paste queries from `sql/verify_data.sql`.

### Step 8 – Spark Web UI (optional)

Open your browser: **http://localhost:8080**

You can see the completed Spark application, DAG, and stage timings.

### Step 9 – Shut down

```bash
docker-compose down           # stop containers (keeps data volume)
docker-compose down -v        # stop AND delete the PostgreSQL data volume
```

---

## Task Details

### Task 1 – Environment Setup

`docker-compose.yml` provisions:

| Service | Image | Ports |
|---------|-------|-------|
| `postgres` | `postgres:15` | 5432 |
| `spark-master` | `bitnami/spark:3.5` | 8080, 7077 |
| `spark-worker` | `bitnami/spark:3.5` | — |

All services share the `pipeline_net` bridge network so Spark can reach PostgreSQL at hostname `postgres`.

The JDBC JAR in `./drivers` is volume-mounted to `/opt/spark-drivers` inside both Spark containers.

### Task 2 – Data Generation

`scripts/generate_employees.py` generates `data/employees_raw.csv` with **1 250 rows** (1 200 unique + 50 duplicates) and these intentional quality issues:

| Issue | How injected |
|-------|-------------|
| Duplicate `employee_id` | 50 rows share an ID with an earlier row |
| Missing critical fields | ~5 % of `email`, `hire_date`, `first_name` blanked |
| Invalid email formats | `missing @`, `double @@`, `missing domain` |
| Future `hire_date` | ~5 % of rows set 30–730 days in the future |
| Currency symbols in salary | Mix of `$75,000`, `75,000`, `75000`, `$75000` |
| Mixed case names | Random ALL CAPS or all lower |
| Mixed case departments / status | `IT`, `it`, `ANALYTICS`, `inactive`, `ACTIVE` |
| Missing non-critical fields | `manager_id`, `address`, `city` ~3–10 % null |
| Missing `birth_date` | ~3 % rows |

### Task 3 – Spark Cleaning & Transformation

`spark/employee_pipeline.py` pipeline stages:

```
read_raw → apply_quality_flags → split_clean/rejected
       → apply_transformations → apply_enrichment
       → select_final → write_to_postgres
```

See [Data Quality Rules](#data-quality-rules) and [Transformation Logic](#transformation-logic) below.

### Task 4 – Database Loading

- **`employees_clean`** – all valid, transformed rows
- **`employees_rejected`** – bad rows with a `reject_reason` column
- Schema is created by `sql/init.sql` which PostgreSQL runs automatically at first start

---

## Data Quality Rules

| Check | Action |
|-------|--------|
| `employee_id`, `first_name`, `last_name`, `email`, `hire_date` are blank | Row → `employees_rejected` |
| `employee_id` appears more than once | Keep first occurrence; duplicates → `employees_rejected` |
| `email` does not match `^[\w._%+\-]+@[\w.\-]+\.[a-z]{2,}$` | Row → `employees_rejected` |
| `hire_date` > today | Row → `employees_rejected` |

---

## Transformation Logic

| Field | Transformation |
|-------|---------------|
| `first_name`, `last_name` | `initcap()` (Proper Case) |
| `email` | `lower()` + `trim()` |
| `salary` | Strip `$` and `,` → cast to `DECIMAL(10,2)` |
| `salary_band` | `< 50 000` → Junior · `50 000–80 000` → Mid · `> 80 000` → Senior |
| `hire_date`, `birth_date` | Parse `yyyy-MM-dd` → `DATE` |
| `age` | `floor(datediff(today, birth_date) / 365.25)` |
| `tenure_years` | `round(datediff(today, hire_date) / 365.25, 1)` |
| `department`, `status` | `initcap(lower())` for consistent casing |
| `full_name` | `first_name + " " + last_name` |
| `email_domain` | Regex extract after `@` |

---

## Verifying Results

Run `sql/verify_data.sql` against `employee_db`. Key checks:

```sql
-- No future hire dates
SELECT COUNT(*) FROM employees_clean WHERE hire_date > CURRENT_DATE;  -- expect 0

-- No duplicate IDs
SELECT employee_id, COUNT(*) FROM employees_clean
GROUP BY employee_id HAVING COUNT(*) > 1;                             -- expect 0 rows

-- Salary band distribution
SELECT * FROM salary_band_distribution;

-- Department summary
SELECT * FROM dept_salary_summary;
```

---

## Troubleshooting

| Problem | Likely Cause | Fix |
|---------|-------------|-----|
| `Connection refused` to PostgreSQL | Container not healthy yet | Wait 20 s and retry; run `docker-compose logs postgres` |
| `ClassNotFoundException: org.postgresql.Driver` | JDBC JAR not in drivers/ | Run `bash scripts/download_jdbc_driver.sh` |
| Spark job exits immediately | Wrong path to script | Check volume mounts; path inside container is `/opt/spark-jobs/employee_pipeline.py` |
| `employees_raw.csv not found` | Data not generated | Run `python scripts/generate_employees.py` first |
| Port 8080 already in use | Another process | Change `"8080:8080"` to e.g. `"8888:8080"` in `docker-compose.yml` |
| `No space left on device` | Docker disk full | Run `docker system prune` |
