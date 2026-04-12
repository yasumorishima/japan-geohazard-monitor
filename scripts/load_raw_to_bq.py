"""Upload raw fetcher data from SQLite to BigQuery.

Reads so2_column, cloud_fraction, geomag_hourly from the local SQLite DB
and uploads to BQ dataset `geohazard` using WRITE_TRUNCATE (full replace).
Each run uploads the complete SQLite state so BQ always reflects the latest.

Usage (CI):
    GCP_SA_KEY=... python3 scripts/load_raw_to_bq.py
"""
import logging
import os
import sqlite3
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PROJECT = "data-platform-490901"
DATASET = "geohazard"
DB_PATH = os.environ.get("GEOHAZARD_DB_PATH", "./data/geohazard.db")

# observed_at / received_at are stored as TEXT in SQLite (e.g. "2011-01-06" or ISO datetime).
# Use STRING in BQ to avoid parse failures. BQ can cast STRING→TIMESTAMP in queries.
TABLES = {
    "so2_column": {
        "query": "SELECT observed_at, cell_lat, cell_lon, so2_du, received_at FROM so2_column",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "cell_lat", "type": "FLOAT"},
            {"name": "cell_lon", "type": "FLOAT"},
            {"name": "so2_du", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "cloud_fraction": {
        "query": "SELECT observed_at, cell_lat, cell_lon, cloud_frac, received_at FROM cloud_fraction",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "cell_lat", "type": "FLOAT"},
            {"name": "cell_lon", "type": "FLOAT"},
            {"name": "cloud_frac", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "geomag_hourly": {
        "query": "SELECT station, observed_at, h_nt, d_nt, z_nt, f_nt, received_at FROM geomag_hourly",
        "schema": [
            {"name": "station", "type": "STRING"},
            {"name": "observed_at", "type": "STRING"},
            {"name": "h_nt", "type": "FLOAT"},
            {"name": "d_nt", "type": "FLOAT"},
            {"name": "z_nt", "type": "FLOAT"},
            {"name": "f_nt", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
}

# Minimum row count to proceed with upload. Prevents wiping BQ data
# when the DB is empty or corrupted.
MIN_ROWS_TO_UPLOAD = 1000


def main():
    sa_key = os.environ.get("GCP_SA_KEY")
    if not sa_key:
        logger.error("GCP_SA_KEY not set — skipping BQ upload")
        sys.exit(1)

    key_path = "/tmp/gcp_sa_key.json"
    try:
        with open(key_path, "w") as f:
            f.write(sa_key)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

        from google.cloud import bigquery

        client = bigquery.Client(project=PROJECT)

        if not os.path.exists(DB_PATH):
            logger.error("SQLite DB not found at %s", DB_PATH)
            sys.exit(1)

        conn = sqlite3.connect(DB_PATH)

        for table_name, config in TABLES.items():
            count = conn.execute(
                "SELECT COUNT(*) FROM " + table_name  # table names are hardcoded above
            ).fetchone()[0]
            if count < MIN_ROWS_TO_UPLOAD:
                logger.info("%s: %d rows (< %d minimum) — skipping to protect BQ data",
                            table_name, count, MIN_ROWS_TO_UPLOAD)
                continue

            logger.info("%s: loading %d rows to BQ...", table_name, count)

            bq_table = f"{PROJECT}.{DATASET}.{table_name}"
            schema = [bigquery.SchemaField(s["name"], s["type"]) for s in config["schema"]]

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                schema=schema,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )

            cursor = conn.execute(config["query"])
            columns = [desc[0] for desc in cursor.description]

            CHUNK_SIZE = 50000
            total_uploaded = 0

            # First chunk — WRITE_TRUNCATE
            rows = cursor.fetchmany(CHUNK_SIZE)
            if rows:
                json_rows = [dict(zip(columns, row)) for row in rows]
                job = client.load_table_from_json(json_rows, bq_table, job_config=job_config)
                job.result()
                total_uploaded += len(json_rows)
                logger.info("  %s: %d/%d rows uploaded (truncate)", table_name, total_uploaded, count)

            # Remaining chunks — WRITE_APPEND
            append_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=schema,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            while True:
                rows = cursor.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                json_rows = [dict(zip(columns, row)) for row in rows]
                job = client.load_table_from_json(json_rows, bq_table, job_config=append_config)
                job.result()
                total_uploaded += len(json_rows)
                logger.info("  %s: %d/%d rows uploaded", table_name, total_uploaded, count)

            table_info = client.get_table(bq_table)
            logger.info("  %s: done — %d rows, %.1f MB in BQ",
                         table_name, table_info.num_rows, table_info.num_bytes / 1024 / 1024)

        conn.close()
        logger.info("All tables uploaded to BQ")

    finally:
        if os.path.exists(key_path):
            os.remove(key_path)


if __name__ == "__main__":
    main()
