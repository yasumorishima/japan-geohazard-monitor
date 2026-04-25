"""Upload raw fetcher data from SQLite to BigQuery.

Reads ALL data tables from the local SQLite DB and uploads to BQ dataset
`geohazard` using WRITE_TRUNCATE (full replace).
Each run uploads the complete SQLite state so BQ always reflects the latest.

Usage (CI):
    GCP_SA_KEY=... python3 scripts/load_raw_to_bq.py
"""
import gzip
import json
import logging
import math
import os
import sqlite3
import sys
import tempfile

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
    "earthquakes": {
        "query": "SELECT source, event_id, occurred_at, latitude, longitude, depth_km, magnitude, magnitude_type, max_intensity, location_ja, location_en, received_at FROM earthquakes",
        "schema": [
            {"name": "source", "type": "STRING"},
            {"name": "event_id", "type": "STRING"},
            {"name": "occurred_at", "type": "STRING"},
            {"name": "latitude", "type": "FLOAT"},
            {"name": "longitude", "type": "FLOAT"},
            {"name": "depth_km", "type": "FLOAT"},
            {"name": "magnitude", "type": "FLOAT"},
            {"name": "magnitude_type", "type": "STRING"},
            {"name": "max_intensity", "type": "STRING"},
            {"name": "location_ja", "type": "STRING"},
            {"name": "location_en", "type": "STRING"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "tec": {
        "query": "SELECT latitude, longitude, tec_tecu, epoch, product_type, received_at FROM tec",
        "schema": [
            {"name": "latitude", "type": "FLOAT"},
            {"name": "longitude", "type": "FLOAT"},
            {"name": "tec_tecu", "type": "FLOAT"},
            {"name": "epoch", "type": "STRING"},
            {"name": "product_type", "type": "STRING"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "geomag_kp": {
        "query": "SELECT time_tag, kp, a_running, station_count, received_at FROM geomag_kp",
        "schema": [
            {"name": "time_tag", "type": "STRING"},
            {"name": "kp", "type": "FLOAT"},
            {"name": "a_running", "type": "FLOAT"},
            {"name": "station_count", "type": "INTEGER"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "focal_mechanisms": {
        "query": "SELECT source, event_id, occurred_at, latitude, longitude, depth_km, magnitude, strike1, dip1, rake1, strike2, dip2, rake2, moment_nm, received_at FROM focal_mechanisms",
        "schema": [
            {"name": "source", "type": "STRING"},
            {"name": "event_id", "type": "STRING"},
            {"name": "occurred_at", "type": "STRING"},
            {"name": "latitude", "type": "FLOAT"},
            {"name": "longitude", "type": "FLOAT"},
            {"name": "depth_km", "type": "FLOAT"},
            {"name": "magnitude", "type": "FLOAT"},
            {"name": "strike1", "type": "FLOAT"},
            {"name": "dip1", "type": "FLOAT"},
            {"name": "rake1", "type": "FLOAT"},
            {"name": "strike2", "type": "FLOAT"},
            {"name": "dip2", "type": "FLOAT"},
            {"name": "rake2", "type": "FLOAT"},
            {"name": "moment_nm", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "gnss_tec": {
        "query": "SELECT latitude, longitude, tec_tecu, dtec_tecu, roti, epoch, source, received_at FROM gnss_tec",
        "schema": [
            {"name": "latitude", "type": "FLOAT"},
            {"name": "longitude", "type": "FLOAT"},
            {"name": "tec_tecu", "type": "FLOAT"},
            {"name": "dtec_tecu", "type": "FLOAT"},
            {"name": "roti", "type": "FLOAT"},
            {"name": "epoch", "type": "STRING"},
            {"name": "source", "type": "STRING"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "modis_lst": {
        "query": "SELECT latitude, longitude, lst_kelvin, lst_day_kelvin, lst_night_kelvin, quality, observed_date, product, received_at FROM modis_lst",
        "schema": [
            {"name": "latitude", "type": "FLOAT"},
            {"name": "longitude", "type": "FLOAT"},
            {"name": "lst_kelvin", "type": "FLOAT"},
            {"name": "lst_day_kelvin", "type": "FLOAT"},
            {"name": "lst_night_kelvin", "type": "FLOAT"},
            {"name": "quality", "type": "STRING"},
            {"name": "observed_date", "type": "STRING"},
            {"name": "product", "type": "STRING"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "ulf_magnetic": {
        "query": "SELECT station, observed_at, h_nt, d_nt, z_nt, f_nt, received_at FROM ulf_magnetic",
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
    "cosmic_ray": {
        "query": "SELECT station, observed_at, counts_per_sec, received_at FROM cosmic_ray",
        "schema": [
            {"name": "station", "type": "STRING"},
            {"name": "observed_at", "type": "STRING"},
            {"name": "counts_per_sec", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "lightning": {
        "query": "SELECT observed_at, cell_lat, cell_lon, stroke_count, mean_intensity, received_at FROM lightning",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "cell_lat", "type": "FLOAT"},
            {"name": "cell_lon", "type": "FLOAT"},
            {"name": "stroke_count", "type": "INTEGER"},
            {"name": "mean_intensity", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "iss_lis_lightning": {
        "query": "SELECT observed_at, cell_lat, cell_lon, flash_count, mean_radiance, received_at FROM iss_lis_lightning",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "cell_lat", "type": "FLOAT"},
            {"name": "cell_lon", "type": "FLOAT"},
            {"name": "flash_count", "type": "INTEGER"},
            {"name": "mean_radiance", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "lightning_thunder_hour": {
        "query": "SELECT observed_at, cell_lat, cell_lon, thunder_hours, received_at FROM lightning_thunder_hour",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "cell_lat", "type": "FLOAT"},
            {"name": "cell_lon", "type": "FLOAT"},
            {"name": "thunder_hours", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "lightning_lis_otd": {
        "query": "SELECT observed_at, cell_lat, cell_lon, flash_rate, received_at FROM lightning_lis_otd",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "cell_lat", "type": "FLOAT"},
            {"name": "cell_lon", "type": "FLOAT"},
            {"name": "flash_rate", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "olr": {
        "query": "SELECT observed_at, cell_lat, cell_lon, olr_wm2, received_at FROM olr",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "cell_lat", "type": "FLOAT"},
            {"name": "cell_lon", "type": "FLOAT"},
            {"name": "olr_wm2", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "earth_rotation": {
        "query": "SELECT observed_at, x_arcsec, y_arcsec, dut1_s, lod_ms, received_at FROM earth_rotation",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "x_arcsec", "type": "FLOAT"},
            {"name": "y_arcsec", "type": "FLOAT"},
            {"name": "dut1_s", "type": "FLOAT"},
            {"name": "lod_ms", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "solar_wind": {
        "query": "SELECT observed_at, bz_gsm_nt, speed_kms, density_cm3, pressure_npa, dst_nt, received_at FROM solar_wind",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "bz_gsm_nt", "type": "FLOAT"},
            {"name": "speed_kms", "type": "FLOAT"},
            {"name": "density_cm3", "type": "FLOAT"},
            {"name": "pressure_npa", "type": "FLOAT"},
            {"name": "dst_nt", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "gravity_mascon": {
        "query": "SELECT observed_at, cell_lat, cell_lon, lwe_thickness_cm, received_at FROM gravity_mascon",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "cell_lat", "type": "FLOAT"},
            {"name": "cell_lon", "type": "FLOAT"},
            {"name": "lwe_thickness_cm", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "soil_moisture": {
        "query": "SELECT observed_at, cell_lat, cell_lon, sm_m3m3, received_at FROM soil_moisture",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "cell_lat", "type": "FLOAT"},
            {"name": "cell_lon", "type": "FLOAT"},
            {"name": "sm_m3m3", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "tide_gauge": {
        "query": "SELECT station_id, observed_at, sea_level_mm, latitude, longitude, received_at FROM tide_gauge",
        "schema": [
            {"name": "station_id", "type": "STRING"},
            {"name": "observed_at", "type": "STRING"},
            {"name": "sea_level_mm", "type": "FLOAT"},
            {"name": "latitude", "type": "FLOAT"},
            {"name": "longitude", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "ocean_color": {
        "query": "SELECT observed_at, cell_lat, cell_lon, chlor_a_mg_m3, received_at FROM ocean_color",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "cell_lat", "type": "FLOAT"},
            {"name": "cell_lon", "type": "FLOAT"},
            {"name": "chlor_a_mg_m3", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "nightlight": {
        "query": "SELECT observed_at, cell_lat, cell_lon, radiance_nwcm2sr, received_at FROM nightlight",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "cell_lat", "type": "FLOAT"},
            {"name": "cell_lon", "type": "FLOAT"},
            {"name": "radiance_nwcm2sr", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "goes_xray": {
        "query": "SELECT observed_at, xray_long_wm2, xray_short_wm2, flare_class FROM goes_xray",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "xray_long_wm2", "type": "FLOAT"},
            {"name": "xray_short_wm2", "type": "FLOAT"},
            {"name": "flare_class", "type": "STRING"},
        ],
    },
    "goes_proton": {
        "query": "SELECT observed_at, proton_10mev_max, proton_60mev_max FROM goes_proton",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "proton_10mev_max", "type": "FLOAT"},
            {"name": "proton_60mev_max", "type": "FLOAT"},
        ],
    },
    "tidal_stress": {
        "query": "SELECT observed_at, tidal_shear_pa, tidal_normal_pa, lunar_distance_km, lunar_phase FROM tidal_stress",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "tidal_shear_pa", "type": "FLOAT"},
            {"name": "tidal_normal_pa", "type": "FLOAT"},
            {"name": "lunar_distance_km", "type": "FLOAT"},
            {"name": "lunar_phase", "type": "FLOAT"},
        ],
    },
    "particle_flux": {
        "query": "SELECT observed_at, electron_2mev_max, electron_800kev_max FROM particle_flux",
        "schema": [
            {"name": "observed_at", "type": "STRING"},
            {"name": "electron_2mev_max", "type": "FLOAT"},
            {"name": "electron_800kev_max", "type": "FLOAT"},
        ],
    },
    "dart_pressure": {
        "query": "SELECT station_id, observed_at, water_height_m, measurement_type, latitude, longitude, received_at FROM dart_pressure",
        "schema": [
            {"name": "station_id", "type": "STRING"},
            {"name": "observed_at", "type": "STRING"},
            {"name": "water_height_m", "type": "FLOAT"},
            {"name": "measurement_type", "type": "STRING"},
            {"name": "latitude", "type": "FLOAT"},
            {"name": "longitude", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "ioc_sea_level": {
        "query": "SELECT station_code, station_name, observed_at, sea_level_m, latitude, longitude, received_at FROM ioc_sea_level",
        "schema": [
            {"name": "station_code", "type": "STRING"},
            {"name": "station_name", "type": "STRING"},
            {"name": "observed_at", "type": "STRING"},
            {"name": "sea_level_m", "type": "FLOAT"},
            {"name": "latitude", "type": "FLOAT"},
            {"name": "longitude", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "snet_waveform": {
        "query": "SELECT station_id, date_str, segment_hour, sensor_type, rms_z, rms_h, hv_ratio, lf_power, hf_power, spectral_slope, vlf_power, vlf_hv_ratio, n_samples, latitude, longitude, cable_segment, received_at FROM snet_waveform",
        "schema": [
            {"name": "station_id", "type": "STRING"},
            {"name": "date_str", "type": "STRING"},
            {"name": "segment_hour", "type": "INTEGER"},
            {"name": "sensor_type", "type": "STRING"},
            {"name": "rms_z", "type": "FLOAT"},
            {"name": "rms_h", "type": "FLOAT"},
            {"name": "hv_ratio", "type": "FLOAT"},
            {"name": "lf_power", "type": "FLOAT"},
            {"name": "hf_power", "type": "FLOAT"},
            {"name": "spectral_slope", "type": "FLOAT"},
            {"name": "vlf_power", "type": "FLOAT"},
            {"name": "vlf_hv_ratio", "type": "FLOAT"},
            {"name": "n_samples", "type": "INTEGER"},
            {"name": "latitude", "type": "FLOAT"},
            {"name": "longitude", "type": "FLOAT"},
            {"name": "cable_segment", "type": "STRING"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
    "satellite_em": {
        "query": "SELECT source, observed_at, latitude, longitude, elf_power_db, vlf_power_db, electron_density, ion_temperature, received_at FROM satellite_em",
        "schema": [
            {"name": "source", "type": "STRING"},
            {"name": "observed_at", "type": "STRING"},
            {"name": "latitude", "type": "FLOAT"},
            {"name": "longitude", "type": "FLOAT"},
            {"name": "elf_power_db", "type": "FLOAT"},
            {"name": "vlf_power_db", "type": "FLOAT"},
            {"name": "electron_density", "type": "FLOAT"},
            {"name": "ion_temperature", "type": "FLOAT"},
            {"name": "received_at", "type": "STRING"},
        ],
    },
}

# Minimum row count to proceed with upload. Prevents wiping BQ data
# when the DB is empty or corrupted.
MIN_ROWS_TO_UPLOAD = 1000


def _sanitize(v):
    # json.dumps emits bare NaN/Infinity tokens which BQ rejects.
    if isinstance(v, float) and not math.isfinite(v):
        return None
    return v


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

        failed_tables = []
        for table_name, config in TABLES.items():
            try:
                # Check if table exists in SQLite before querying
                exists = conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,)
                ).fetchone()[0]
                if not exists:
                    logger.info("%s: table not found in SQLite — skipping", table_name)
                    continue

                count = conn.execute(
                    "SELECT COUNT(*) FROM " + table_name  # table names are hardcoded above
                ).fetchone()[0]
                if count < MIN_ROWS_TO_UPLOAD:
                    logger.info("%s: %d rows (< %d minimum) — skipping to protect BQ data",
                                table_name, count, MIN_ROWS_TO_UPLOAD)
                    continue

                logger.info("%s: streaming %d rows to gzip NDJSON...", table_name, count)

                bq_table = f"{PROJECT}.{DATASET}.{table_name}"
                schema = [bigquery.SchemaField(s["name"], s["type"]) for s in config["schema"]]

                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    schema=schema,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                )

                cursor = conn.execute(config["query"])
                columns = [desc[0] for desc in cursor.description]

                # Stream all rows to a single gzipped NDJSON tempfile, then one BQ load job.
                # Avoids per-chunk BQ job overhead (2-3s each) which previously
                # made ulf_magnetic 9.1M rows take 15min+ across 182 jobs.
                # gzip keeps runner disk usage bounded (~5-10x reduction).
                tmp_dir = os.path.join(os.path.dirname(DB_PATH), "bq_tmp")
                os.makedirs(tmp_dir, exist_ok=True)
                fd, tmp_path = tempfile.mkstemp(suffix=".ndjson.gz", prefix=f"{table_name}_", dir=tmp_dir)
                os.close(fd)
                try:
                    written = 0
                    with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
                        while True:
                            rows = cursor.fetchmany(50000)
                            if not rows:
                                break
                            for row in rows:
                                obj = {c: _sanitize(v) for c, v in zip(columns, row)}
                                f.write(json.dumps(obj, ensure_ascii=False, default=str))
                                f.write("\n")
                            written += len(rows)
                            logger.info("  %s: %d/%d rows written to NDJSON", table_name, written, count)

                    tmp_bytes = os.path.getsize(tmp_path)
                    logger.info("  %s: uploading %d rows (%.1f MB gzip) to BQ in single job...",
                                table_name, written, tmp_bytes / 1024 / 1024)
                    with open(tmp_path, "rb") as f:
                        job = client.load_table_from_file(f, bq_table, job_config=job_config)
                    job.result()

                    table_info = client.get_table(bq_table)
                    logger.info("  %s: done — %d rows, %.1f MB in BQ",
                                 table_name, table_info.num_rows, table_info.num_bytes / 1024 / 1024)
                finally:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
            except Exception as e:
                logger.error("%s: upload failed — %s", table_name, e)
                failed_tables.append(table_name)
                continue

        conn.close()
        if failed_tables:
            logger.error("BQ upload completed with %d failed tables: %s",
                         len(failed_tables), ", ".join(failed_tables))
            sys.exit(1)
        logger.info("All tables uploaded to BQ")

    finally:
        if os.path.exists(key_path):
            os.remove(key_path)


if __name__ == "__main__":
    main()
