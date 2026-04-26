"""
fetch_swarm_em.py - ESA Swarm A satellite ionospheric + magnetic perturbation fetcher.

Phase 19 (Phase 1 Step 5b, 2026-04-26): Replaces deprecated CSES portion of
fetch_cses_satellite.py with ESA Swarm A data via the official viresclient SDK.

Physical basis
--------------
ESA Swarm A is a low-Earth-orbit (~460km altitude, polar) magnetometry mission.
Two product streams are used:

  - SW_OPER_MAGA_LR_1B (1 Hz vector + scalar B field): combined with the
    CHAOS-Core geomagnetic main-field model, the residual |B_obs - B_CHAOS|
    flags lithospheric / ionospheric perturbations potentially correlated with
    crustal stress changes preceding earthquakes. We request residuals=True so
    VirES returns B_NEC_res_CHAOS-Core directly (no client-side subtraction).

  - SW_OPER_EFIA_LP_1B (Langmuir probe Ne, Te, 2 Hz native): plasma density /
    electron temperature anomalies are reported in literature as possible
    pre-seismic ionospheric signatures.

MAG and EFI are fetched independently and stored as SEPARATE rows (different
source labels) to avoid 1Hz/2Hz timestamp mis-alignment producing NaN-heavy
joins. Downstream BQ analysis can union the two source streams as needed.

Aggregation strategy
--------------------
Raw samples are grouped into orbit passes (contiguous samples within Japan bbox
lat=20-50, lon=120-155) using a delta-t > 60s gap detector, then per-pass
aggregates are stored.

Auth
----
SWARM_TOKEN env var (Bearer token from https://vires.services/accounts/tokens).
Auth failures (401/403) hard-fail rather than silently skipping chunks.

References
----------
- viresclient: https://github.com/ESA-VirES/VirES-Python-Client
- Swarm cookbook: https://notebooks.vires.services/
- CHAOS model: https://doi.org/10.1186/s40623-020-01252-9
"""

import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import numpy as np
import pandas as pd

try:
    from viresclient import SwarmRequest
except ImportError:
    print("ERROR: viresclient not installed. pip install viresclient", file=sys.stderr)
    sys.exit(1)


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


LAT_MIN, LAT_MAX = 20.0, 50.0
LON_MIN, LON_MAX = 120.0, 155.0

SWARM_START_DATE = datetime(2014, 1, 1, tzinfo=timezone.utc)

CHUNK_DAYS = 7

MAX_DAYS_PER_RUN = int(os.environ.get("SWARM_MAX_DAYS", "90"))

PASS_GAP_S = 60.0

LATENCY_BUFFER_DAYS = 2

VIRES_URL = "https://vires.services/ows"
SAMPLING_STEP = "PT1S"

CHAOS_RESIDUAL_COL = "B_NEC_res_CHAOS-Core"

DB_PATH = os.environ.get("GEOHAZARD_DB_PATH", "./data/geohazard.db")
TOKEN = os.environ.get("SWARM_TOKEN", "").strip()

SOURCE_MAG = "SWARM_A_MAG"
SOURCE_EFI = "SWARM_A_EFI"


def init_swarm_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS swarm_em (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            pass_id TEXT NOT NULL,
            duration_s REAL,
            sample_count INTEGER,
            lat_min REAL,
            lat_max REAL,
            lon_min REAL,
            lon_max REAL,
            ne_mean REAL,
            ne_std REAL,
            te_mean REAL,
            b_residual_mean REAL,
            b_residual_max REAL,
            received_at TEXT NOT NULL,
            UNIQUE(source, pass_id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_swarm_em_observed_at ON swarm_em(observed_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_swarm_em_source ON swarm_em(source)")
    conn.commit()


class SwarmAuthError(Exception):
    """Raised when VirES rejects authentication (401/403). Hard-fail."""


def _is_auth_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "401" in msg or "403" in msg or "unauthorized" in msg or "forbidden" in msg


def _fetch_one_collection(
    collection: str,
    measurements: List[str],
    models: Optional[List[str]],
    residuals: bool,
    start: datetime,
    end: datetime,
) -> Optional[pd.DataFrame]:
    try:
        req = SwarmRequest(url=VIRES_URL, token=TOKEN)
        req.set_collection(collection)
        req.set_products(
            measurements=measurements,
            models=models if models else [],
            residuals=residuals,
            sampling_step=SAMPLING_STEP,
        )
        req.set_range_filter("Latitude", LAT_MIN, LAT_MAX)
        req.set_range_filter("Longitude", LON_MIN, LON_MAX)
        data = req.get_between(
            start_time=start,
            end_time=end,
            asynchronous=False,
            show_progress=False,
        )
        return data.as_dataframe()
    except Exception as exc:
        if _is_auth_error(exc):
            logger.error("VirES auth failure (%s): %s", collection, exc)
            raise SwarmAuthError(str(exc)) from exc
        logger.warning("VirES request failed for %s %s..%s: %s", collection, start, end, exc)
        return None


def _split_passes(df: pd.DataFrame) -> pd.Series:
    dt = df.index.to_series().diff().dt.total_seconds()
    return (dt > PASS_GAP_S).cumsum().fillna(0).astype(int)


def parse_mag_passes(df: pd.DataFrame) -> List[dict]:
    if df is None or df.empty or not isinstance(df.index, pd.DatetimeIndex):
        return []
    df = df.sort_index()
    pass_label = _split_passes(df)
    has_residual = CHAOS_RESIDUAL_COL in df.columns

    out: List[dict] = []
    for pid, group in df.groupby(pass_label):
        if len(group) < 2:
            continue
        start_ts = group.index[0]
        duration = (group.index[-1] - start_ts).total_seconds()

        b_res_mean = b_res_max = None
        if has_residual:
            norms: List[float] = []
            for v in group[CHAOS_RESIDUAL_COL]:
                try:
                    arr = np.asarray(v, dtype=float)
                    if arr.shape == (3,) and not np.isnan(arr).any():
                        norms.append(float(np.linalg.norm(arr)))
                except (TypeError, ValueError):
                    continue
            if norms:
                b_res_mean = float(np.mean(norms))
                b_res_max = float(np.max(norms))

        out.append(_pass_record(
            source=SOURCE_MAG,
            start_ts=start_ts,
            pid=int(pid),
            duration_s=duration,
            sample_count=int(len(group)),
            group=group,
            ne_mean=None, ne_std=None, te_mean=None,
            b_residual_mean=b_res_mean,
            b_residual_max=b_res_max,
        ))
    return out


def parse_efi_passes(df: pd.DataFrame) -> List[dict]:
    if df is None or df.empty or not isinstance(df.index, pd.DatetimeIndex):
        return []
    df = df.sort_index()
    pass_label = _split_passes(df)

    out: List[dict] = []
    for pid, group in df.groupby(pass_label):
        if len(group) < 2:
            continue
        start_ts = group.index[0]
        duration = (group.index[-1] - start_ts).total_seconds()

        ne_mean = ne_std = te_mean = None
        if "Ne" in group.columns:
            ne_series = group["Ne"].dropna()
            if len(ne_series) > 0:
                ne_mean = float(ne_series.mean())
                if len(ne_series) > 1:
                    ne_std = float(ne_series.std())
        if "Te" in group.columns:
            te_series = group["Te"].dropna()
            if len(te_series) > 0:
                te_mean = float(te_series.mean())

        out.append(_pass_record(
            source=SOURCE_EFI,
            start_ts=start_ts,
            pid=int(pid),
            duration_s=duration,
            sample_count=int(len(group)),
            group=group,
            ne_mean=ne_mean, ne_std=ne_std, te_mean=te_mean,
            b_residual_mean=None, b_residual_max=None,
        ))
    return out


def _pass_record(
    source: str, start_ts, pid: int, duration_s: float, sample_count: int,
    group: pd.DataFrame,
    ne_mean, ne_std, te_mean, b_residual_mean, b_residual_max,
) -> dict:
    pass_uid = f"{source}_{start_ts.strftime('%Y%m%dT%H%M%S')}_{pid}"
    return {
        "source": source,
        "observed_at": start_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pass_id": pass_uid,
        "duration_s": duration_s,
        "sample_count": sample_count,
        "lat_min": float(group["Latitude"].min()) if "Latitude" in group.columns else None,
        "lat_max": float(group["Latitude"].max()) if "Latitude" in group.columns else None,
        "lon_min": float(group["Longitude"].min()) if "Longitude" in group.columns else None,
        "lon_max": float(group["Longitude"].max()) if "Longitude" in group.columns else None,
        "ne_mean": ne_mean,
        "ne_std": ne_std,
        "te_mean": te_mean,
        "b_residual_mean": b_residual_mean,
        "b_residual_max": b_residual_max,
    }


def insert_passes(conn: sqlite3.Connection, passes: List[dict]) -> int:
    if not passes:
        return 0
    cur = conn.cursor()
    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    inserted = 0
    for p in passes:
        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO swarm_em (
                    source, observed_at, pass_id, duration_s, sample_count,
                    lat_min, lat_max, lon_min, lon_max,
                    ne_mean, ne_std, te_mean,
                    b_residual_mean, b_residual_max, received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    p["source"], p["observed_at"], p["pass_id"],
                    p["duration_s"], p["sample_count"],
                    p["lat_min"], p["lat_max"], p["lon_min"], p["lon_max"],
                    p["ne_mean"], p["ne_std"], p["te_mean"],
                    p["b_residual_mean"], p["b_residual_max"],
                    received_at,
                ),
            )
            if cur.rowcount > 0:
                inserted += 1
        except sqlite3.Error as exc:
            logger.warning("Insert failed for pass %s: %s", p["pass_id"], exc)
    conn.commit()
    return inserted


def get_resume_date_for_source(conn: sqlite3.Connection, source: str) -> datetime:
    """Per-source resume. Prevents one source's progress masking another's gap
    when a transient request failure inserts only one collection's rows."""
    cur = conn.cursor()
    cur.execute("SELECT MAX(observed_at) FROM swarm_em WHERE source = ?", (source,))
    row = cur.fetchone()
    if row and row[0]:
        last_date = datetime.strptime(row[0][:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return last_date + timedelta(days=1)
    return SWARM_START_DATE


def _fetch_collection_window(
    conn: sqlite3.Connection,
    source: str,
    collection: str,
    measurements: List[str],
    models: Optional[List[str]],
    residuals: bool,
    parser,
) -> tuple:
    """Run one source's chunk loop. Returns (chunks_processed, rows_inserted)."""
    resume = get_resume_date_for_source(conn, source)
    end_total = min(
        resume + timedelta(days=MAX_DAYS_PER_RUN),
        datetime.now(timezone.utc) - timedelta(days=LATENCY_BUFFER_DAYS),
    )
    if resume >= end_total:
        logger.info("[%s] up to date: resume=%s end=%s", source, resume.date(), end_total.date())
        return 0, 0

    total_days = (end_total - resume).days
    logger.info(
        "[%s] window: %s -> %s (%d days, %d-day chunks)",
        source, resume.date(), end_total.date(), total_days, CHUNK_DAYS,
    )

    total_inserted = 0
    chunk_idx = 0
    cur_start = resume
    while cur_start < end_total:
        cur_end = min(cur_start + timedelta(days=CHUNK_DAYS), end_total)
        chunk_idx += 1
        df = _fetch_one_collection(
            collection=collection,
            measurements=measurements,
            models=models,
            residuals=residuals,
            start=cur_start,
            end=cur_end,
        )
        passes = parser(df) if df is not None else []
        inserted = insert_passes(conn, passes)
        total_inserted += inserted
        logger.info(
            "  [%s] chunk %d %s -> %s: passes=%d inserted=%d",
            source, chunk_idx, cur_start.date(), cur_end.date(),
            len(passes), inserted,
        )
        cur_start = cur_end
        time.sleep(0.5)

    return chunk_idx, total_inserted


def fetch_satellite() -> int:
    if not TOKEN:
        logger.error("SWARM_TOKEN env var is not set")
        return 2

    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    try:
        init_swarm_table(conn)

        try:
            mag_chunks, mag_inserted = _fetch_collection_window(
                conn=conn,
                source=SOURCE_MAG,
                collection="SW_OPER_MAGA_LR_1B",
                measurements=["F", "B_NEC"],
                models=["CHAOS-Core"],
                residuals=True,
                parser=parse_mag_passes,
            )
            efi_chunks, efi_inserted = _fetch_collection_window(
                conn=conn,
                source=SOURCE_EFI,
                collection="SW_OPER_EFIA_LP_1B",
                measurements=["Ne", "Te"],
                models=None,
                residuals=False,
                parser=parse_efi_passes,
            )
        except SwarmAuthError as exc:
            logger.error("Aborting: VirES authentication failed (%s)", exc)
            return 3

        logger.info(
            "Total inserted: MAG=%d (%d chunks), EFI=%d (%d chunks)",
            mag_inserted, mag_chunks, efi_inserted, efi_chunks,
        )
        return 0
    finally:
        conn.close()


def main() -> int:
    return fetch_satellite()


if __name__ == "__main__":
    sys.exit(main())
