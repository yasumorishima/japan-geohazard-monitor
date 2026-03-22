"""Load feature_matrix.json and ML results to BigQuery.

Called from CI after ML prediction phase. Uploads:
1. feature_matrix → geohazard.feature_matrix (WRITE_TRUNCATE per phase)
2. ML metadata → geohazard.feature_matrix_metadata (WRITE_APPEND, phase history)
3. Feature non-zero rates → geohazard.feature_nonzero_rates (WRITE_APPEND, bug detection)
"""
import json
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PROJECT = "data-platform-490901"
DATASET = "geohazard"


def load_feature_matrix(client, fm_path: Path, phase: str):
    """Load feature_matrix.json to BQ, flattened to 1 row per (timestep, cell)."""
    import pandas as pd
    from google.cloud import bigquery

    logger.info("Loading %s ...", fm_path)
    with open(fm_path) as f:
        data = json.load(f)

    meta = data["metadata"]
    feature_names = meta["feature_names"]
    grid_h, grid_w = meta["grid_h"], meta["grid_w"]
    lat_range = meta["grid_lat_range"]
    lon_range = meta["grid_lon_range"]
    cell_size = meta["cell_size_deg"]

    rows = []
    for ts in data["timesteps"]:
        t_days = ts["t_days"]
        for i in range(grid_h):
            lat = lat_range[0] + i * cell_size
            for j in range(grid_w):
                lon = lon_range[0] + j * cell_size
                feat = ts["features"][i][j]
                label = ts["labels"][i][j]
                row = {"t_days": t_days, "lat": lat, "lon": lon, "label": int(label)}
                for fi, fname in enumerate(feature_names):
                    row[fname] = feat[fi] if fi < len(feat) else None
                rows.append(row)

    df = pd.DataFrame(rows)
    logger.info("DataFrame: %d rows, %d cols, %.0f MB",
                len(df), len(df.columns), df.memory_usage(deep=True).sum() / 1024 / 1024)

    table_ref = f"{PROJECT}.{DATASET}.feature_matrix"
    schema = [
        bigquery.SchemaField("t_days", "INTEGER"),
        bigquery.SchemaField("lat", "FLOAT"),
        bigquery.SchemaField("lon", "FLOAT"),
        bigquery.SchemaField("label", "INTEGER"),
    ] + [bigquery.SchemaField(fname, "FLOAT") for fname in feature_names]

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    table = client.get_table(table_ref)
    logger.info("feature_matrix: %d rows, %.1f MB", table.num_rows, table.num_bytes / 1024 / 1024)

    return df, meta, feature_names


def load_metadata(client, meta: dict, phase: str, ml_result_path: Path = None):
    """Append phase metadata to BQ (AUC history tracking)."""
    import pandas as pd
    from google.cloud import bigquery

    cv_auc = None
    test_auc_rf = None
    stacking_auc = None
    n_features_active = meta.get("n_features", None)

    if ml_result_path and ml_result_path.exists():
        ml = json.loads(ml_result_path.read_text())
        cv_auc = ml.get("cv_auc_pooled")
        test_auc_rf = ml.get("test_auc", {}).get("rf")
        n_features_active = ml.get("n_features_active", n_features_active)

    meta_df = pd.DataFrame([{
        "phase": phase,
        "n_timesteps": meta["n_timesteps"],
        "grid_h": meta["grid_h"],
        "grid_w": meta["grid_w"],
        "n_features": len(meta["feature_names"]),
        "n_features_active": n_features_active,
        "cell_size_deg": meta["cell_size_deg"],
        "prediction_window_days": meta["prediction_window_days"],
        "min_target_mag": meta["min_target_mag"],
        "step_days": meta["step_days"],
        "total_positives": meta["total_positives"],
        "feature_names": json.dumps(meta["feature_names"]),
        "cv_auc": cv_auc,
        "test_auc_rf": test_auc_rf,
        "stacking_auc": stacking_auc,
        "loaded_at": pd.Timestamp.utcnow(),
    }])

    table_ref = f"{PROJECT}.{DATASET}.feature_matrix_metadata"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_dataframe(meta_df, table_ref, job_config=job_config)
    job.result()
    logger.info("Metadata appended: phase=%s cv_auc=%s test_auc=%s", phase, cv_auc, test_auc_rf)


def compute_nonzero_rates(client, df, feature_names: list, phase: str):
    """Compute and store per-feature non-zero rates for bug detection."""
    import pandas as pd
    from google.cloud import bigquery

    n_total = len(df)
    rates = []
    for fname in feature_names:
        if fname not in df.columns:
            continue
        nonzero = int((df[fname] != 0.0).sum())
        rates.append({
            "phase": phase,
            "feature_name": fname,
            "nonzero_count": nonzero,
            "total_count": n_total,
            "nonzero_pct": round(100 * nonzero / n_total, 2) if n_total > 0 else 0,
            "loaded_at": pd.Timestamp.utcnow(),
        })

    rates_df = pd.DataFrame(rates)
    table_ref = f"{PROJECT}.{DATASET}.feature_nonzero_rates"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_dataframe(rates_df, table_ref, job_config=job_config)
    job.result()

    # Alert on zero-hit features
    zero_features = rates_df[rates_df["nonzero_pct"] == 0.0]["feature_name"].tolist()
    if zero_features:
        logger.warning("ZERO-HIT features (0%% non-zero): %s", ", ".join(zero_features))
    else:
        logger.info("All features have non-zero values")

    logger.info("Non-zero rates: %d features logged", len(rates))
    return zero_features


def main():
    import glob

    phase = os.environ.get("PHASE", "unknown")
    results_dir = Path(os.environ.get("RESULTS_DIR", "results"))

    # Find feature_matrix
    fm_path = results_dir / "feature_matrix.json"
    if not fm_path.exists():
        logger.error("feature_matrix.json not found at %s", fm_path)
        sys.exit(1)

    # Find latest ML result
    ml_files = sorted(glob.glob(str(results_dir / "ml_prediction_*.json")))
    ml_result_path = Path(ml_files[-1]) if ml_files else None

    # Setup BQ client
    sa_key = os.environ.get("GCP_SA_KEY")
    if sa_key:
        import tempfile
        key_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        key_file.write(sa_key)
        key_file.close()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file.name

    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT)

    # 1. Load feature matrix
    df, meta, feature_names = load_feature_matrix(client, fm_path, phase)

    # 2. Load metadata with AUC
    load_metadata(client, meta, phase, ml_result_path)

    # 3. Compute and store non-zero rates
    zero_features = compute_nonzero_rates(client, df, feature_names, phase)

    if zero_features:
        logger.warning("Phase %s has %d ZERO-HIT features — check coordinate alignment",
                       phase, len(zero_features))

    logger.info("BQ load complete for phase %s", phase)


if __name__ == "__main__":
    main()
