#!/usr/bin/env bash
# ============================================================
# download_jdbc_driver.sh
# Downloads the PostgreSQL JDBC driver JAR needed by Spark.
# Run ONCE before docker-compose up.
# ============================================================

set -euo pipefail

DRIVER_DIR="drivers"
JAR_NAME="postgresql-42.7.3.jar"
DOWNLOAD_URL="https://jdbc.postgresql.org/download/${JAR_NAME}"

mkdir -p "${DRIVER_DIR}"

if [[ -f "${DRIVER_DIR}/${JAR_NAME}" ]]; then
    echo "✅  Driver already present: ${DRIVER_DIR}/${JAR_NAME}"
    exit 0
fi

echo "⬇️   Downloading PostgreSQL JDBC driver …"
curl -L -o "${DRIVER_DIR}/${JAR_NAME}" "${DOWNLOAD_URL}"
echo "✅  Saved to ${DRIVER_DIR}/${JAR_NAME}"
