"""Fetch InSAR ground deformation data from COMET LiCSAR.

InSAR (Interferometric Synthetic Aperture Radar) measures ground
deformation at mm-scale precision using satellite radar. Unlike GEONET
GPS (point measurements at 1,300 stations), InSAR provides continuous
spatial coverage of the entire Japan region.

Physical mechanism:
    Pre-seismic slow slip, aseismic creep, and strain accumulation
    produce subtle ground deformation (mm-cm) detectable by InSAR.
    GEONET GPS captures this at station locations, but InSAR fills
    the gaps between stations — especially offshore areas near
    subduction zones.

Data source: COMET LiCSAR (Looking Into Continents from Space with
    Synthetic Aperture Radar)
    - Pre-processed Sentinel-1 interferograms for tectonic regions
    - Free access, no authentication required
    - Covers Japan since Sentinel-1 launch (2014)

    Frame naming: OOOP_AAAAA_BBBBBB
        OOO = relative orbit number (001-175)
        P   = orbital direction (A=ascending, D=descending)
        AAAAA = colatitude identifier
        BBBBBB = burst count per sub-swath (2 digits each for IW1/IW2/IW3)

    Directory: https://gws-access.jasmin.ac.uk/public/nceo_geohazards/
               LiCSAR_products/{track_num}/{frame_id}/

    Interferogram products per pair (YYYYMMDD_YYYYMMDD/):
        geo.unw.tif  — unwrapped phase (radians)
        geo.cc.tif   — coherence (0-1)

    Phase-to-displacement: d_LOS = (lambda / 4*pi) * phase
        Sentinel-1 C-band wavelength = 0.05546 m

    Alternative: ASF DAAC (Alaska Satellite Facility) for raw Sentinel-1
    - Requires Earthdata login for SLC products
    - Processing requires ISCE/SNAP (heavy computation)

Target features:
    - insar_deformation_rate: LOS velocity anomaly per cell (mm/year deviation)

References:
    - Bürgmann et al. (2000) Ann. Rev. Earth Planet. Sci. 28:169-209
    - Lazecký et al. (2020) Remote Sensing 12:2430 (LiCSAR system)
    - Morishita (2021) Prog. Earth Planet. Sci. 8:6
      (34 LiCSAR frames for nationwide Japan deformation monitoring)
"""

import asyncio
import io
import logging
import os
import re
import struct
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import aiosqlite
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────
# LiCSAR JASMIN public directory
# ──────────────────────────────────────────────────────────
LICSAR_BASE = "https://gws-access.jasmin.ac.uk/public/nceo_geohazards/LiCSAR_products"

# Sentinel-1 C-band wavelength (m)
S1_WAVELENGTH_M = 0.05546

# Japan bounding box
JAPAN_LAT_MIN = 24.0
JAPAN_LAT_MAX = 46.0
JAPAN_LON_MIN = 122.0
JAPAN_LON_MAX = 150.0

# Spatial subsampling: take every Nth pixel to keep data manageable
# LiCSAR pixel spacing is ~100m; step=10 gives ~1km grid
PIXEL_STEP = 10

# Minimum coherence threshold — discard low-quality pixels
MIN_COHERENCE = 0.3

# Maximum interferometric pairs to process per frame per run
MAX_PAIRS_PER_FRAME = 5

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)

# ──────────────────────────────────────────────────────────
# Known LiCSAR frames covering Japan
# Source: Morishita (2021) Table 1 — 34 frames (18 asc + 16 desc)
# Frame ID format: OOOP_AAAAA_BBBBBB
#   OOO  = 3-digit orbit number
#   P    = A (ascending) or D (descending)
#   AAAAA = colatitude identifier
#   BBBBBB = burst count (IW1/IW2/IW3, 2 digits each)
#
# These cover the Japanese archipelago from Kyushu to Hokkaido.
# Ascending orbits pass Japan roughly SW→NE; descending NE→SW.
# ──────────────────────────────────────────────────────────

# Descending tracks over Japan (evening pass, ~20:40 UTC / 05:40 JST)
JAPAN_FRAMES_DESC = [
    "046D_05292_131313",  # S-Tohoku / N-Kanto
    "046D_05103_131313",  # Kanto / Chubu
    "046D_04914_131313",  # Kinki / Shikoku
    "047D_05353_131313",  # Tohoku
    "047D_05164_131313",  # N-Kanto / Chubu
    "047D_04975_131313",  # Kinki / Chugoku
    "048D_05225_131313",  # Tohoku
    "048D_05036_131313",  # Kanto / Chubu
    "048D_04847_131313",  # Kinki / Shikoku
    "119D_05412_131313",  # Hokkaido (south)
    "119D_05223_131313",  # Tohoku
    "119D_05034_131313",  # N-Kanto
    "120D_05284_131313",  # Tohoku / Hokkaido
    "120D_05095_131313",  # Kanto
    "175D_05292_131313",  # Kyushu / SW Japan
    "175D_05481_131313",  # Hokkaido
]

# Ascending tracks over Japan (morning pass, ~09:20 UTC / 18:20 JST)
JAPAN_FRAMES_ASC = [
    "010A_05574_131313",  # Hokkaido
    "010A_05385_131313",  # N-Tohoku
    "010A_05196_131313",  # S-Tohoku / N-Kanto
    "039A_05196_070603",  # Kanto
    "054A_05034_131313",  # Kanto / Chubu
    "054A_04845_131313",  # Kinki / Shikoku
    "083A_05385_131313",  # Tohoku
    "083A_05196_131313",  # Kanto
    "083A_05007_131313",  # Chubu / Kinki
    "112A_05385_131313",  # Tohoku
    "112A_05196_131313",  # Kanto
    "112A_05007_131313",  # Kinki / Shikoku
    "141A_05385_131313",  # Tohoku
    "141A_05196_131313",  # Kanto / Chubu
    "141A_05007_131313",  # Kinki / Chugoku
    "156A_04845_131313",  # Kyushu
    "170A_04675_131008",  # SW Kyushu / Okinawa
    "170A_05385_131313",  # Hokkaido
]

ALL_JAPAN_FRAMES = JAPAN_FRAMES_DESC + JAPAN_FRAMES_ASC


async def init_insar_table():
    """Create InSAR deformation table."""
    async with safe_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS insar_deformation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                frame_id TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                los_velocity_mm_yr REAL,
                coherence REAL,
                received_at TEXT NOT NULL,
                UNIQUE(frame_id, observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_insar_time
            ON insar_deformation(observed_at)
        """)
        await db.commit()


# ──────────────────────────────────────────────────────────
# LiCSAR JASMIN directory parsing
# ──────────────────────────────────────────────────────────

async def discover_frames_from_jasmin(session: aiohttp.ClientSession,
                                      track_num: int) -> list[str]:
    """Scrape JASMIN directory listing for frame IDs under a given track.

    URL pattern: {LICSAR_BASE}/{track_num}/
    Returns list of frame IDs (e.g. ['046D_05292_131313', ...]).
    """
    url = f"{LICSAR_BASE}/{track_num}/"
    try:
        async with session.get(url, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                return []
            text = await resp.text()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return []

    # Parse HTML directory listing for frame links
    # Pattern: href="046D_05292_131313/"
    frames = []
    for m in re.finditer(r'href="(\d{3}[AD]_\d{5}_\d{6})/"', text):
        frames.append(m.group(1))
    return frames


async def discover_interferograms(session: aiohttp.ClientSession,
                                   frame_id: str) -> list[str]:
    """List available interferometric pairs for a frame.

    Returns list of date pairs like ['20230101_20230113', ...],
    sorted newest-first.
    """
    track_num = frame_id[:3].lstrip("0") or "0"
    url = f"{LICSAR_BASE}/{track_num}/{frame_id}/interferograms/"
    try:
        async with session.get(url, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                return []
            text = await resp.text()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return []

    pairs = []
    for m in re.finditer(r'href="(\d{8}_\d{8})/"', text):
        pairs.append(m.group(1))

    # Sort newest-first (by second date in pair)
    pairs.sort(key=lambda p: p.split("_")[1], reverse=True)
    return pairs


# ──────────────────────────────────────────────────────────
# GeoTIFF reading (minimal struct-based, no GDAL/rasterio)
# ──────────────────────────────────────────────────────────

def _read_tiff_ifd(data: bytes, offset: int, endian: str):
    """Read a single IFD (Image File Directory) from TIFF data."""
    n_entries = struct.unpack_from(f"{endian}H", data, offset)[0]
    tags = {}
    for i in range(n_entries):
        entry_off = offset + 2 + i * 12
        tag_id, dtype, count, value_off = struct.unpack_from(
            f"{endian}HHII", data, entry_off
        )
        # For small values, the value is stored in the offset field itself
        if dtype == 3 and count == 1:  # SHORT
            val = struct.unpack_from(f"{endian}H", data, entry_off + 8)[0]
        elif dtype == 4 and count == 1:  # LONG
            val = value_off
        elif dtype == 11 and count == 1:  # FLOAT
            val = struct.unpack_from(f"{endian}f", data, entry_off + 8)[0]
        elif dtype == 12 and count == 1:  # DOUBLE
            val = struct.unpack_from(f"{endian}d", data, value_off)[0]
        elif dtype == 12 and count > 1:  # DOUBLE array
            val = [
                struct.unpack_from(f"{endian}d", data, value_off + j * 8)[0]
                for j in range(count)
            ]
        elif dtype == 3 and count > 1 and count * 2 <= 4:  # SHORT array in offset
            val = [
                struct.unpack_from(f"{endian}H", data, entry_off + 8 + j * 2)[0]
                for j in range(count)
            ]
        elif dtype == 3 and count > 1:  # SHORT array
            val = [
                struct.unpack_from(f"{endian}H", data, value_off + j * 2)[0]
                for j in range(count)
            ]
        else:
            val = value_off
        tags[tag_id] = val
    return tags


def parse_geotiff_float32(data: bytes):
    """Parse a GeoTIFF containing float32 raster data.

    Returns (width, height, pixel_data, geo_transform) where:
    - pixel_data is a flat list of float32 values (row-major)
    - geo_transform is (origin_x, pixel_size_x, 0, origin_y, 0, pixel_size_y)
      or None if ModelTiepointTag/ModelPixelScaleTag are absent.

    This is a minimal parser for LiCSAR GeoTIFF files which are
    single-band, uncompressed or stripped float32 rasters.
    """
    if len(data) < 8:
        raise ValueError("Too small for TIFF")

    # Byte order
    bo = data[:2]
    if bo == b"II":
        endian = "<"
    elif bo == b"MM":
        endian = ">"
    else:
        raise ValueError(f"Not a TIFF: {bo!r}")

    magic = struct.unpack_from(f"{endian}H", data, 2)[0]
    if magic != 42:
        raise ValueError(f"Not a TIFF (magic={magic})")

    ifd_offset = struct.unpack_from(f"{endian}I", data, 4)[0]
    tags = _read_tiff_ifd(data, ifd_offset, endian)

    width = tags.get(256, 0)   # ImageWidth
    height = tags.get(257, 0)  # ImageLength
    bits = tags.get(258, 32)   # BitsPerSample
    compression = tags.get(259, 1)  # Compression (1=none)
    sample_fmt = tags.get(339, 3)  # SampleFormat (3=float)

    if compression != 1:
        raise ValueError(f"Compressed TIFF (compression={compression}) not supported")
    if bits != 32 or sample_fmt != 3:
        raise ValueError(f"Expected float32, got bits={bits} fmt={sample_fmt}")

    # Get strip offsets and byte counts
    strip_offsets = tags.get(273, 0)  # StripOffsets
    strip_counts = tags.get(279, 0)  # StripByteCounts

    if isinstance(strip_offsets, int):
        strip_offsets = [strip_offsets]
    if isinstance(strip_counts, int):
        strip_counts = [strip_counts]

    # Read all strips
    pixel_data = []
    for off, cnt in zip(strip_offsets, strip_counts):
        n_pixels = cnt // 4
        for j in range(n_pixels):
            val = struct.unpack_from(f"{endian}f", data, off + j * 4)[0]
            pixel_data.append(val)

    # GeoTIFF tie points and pixel scale
    # Tag 33922 = ModelTiepointTag (I, J, K, X, Y, Z)
    # Tag 33550 = ModelPixelScaleTag (ScaleX, ScaleY, ScaleZ)
    tiepoint = tags.get(33922)
    pixel_scale = tags.get(33550)

    geo_transform = None
    if isinstance(tiepoint, list) and len(tiepoint) >= 6 and \
       isinstance(pixel_scale, list) and len(pixel_scale) >= 2:
        # origin_x = X - I * ScaleX, origin_y = Y + J * ScaleY
        origin_x = tiepoint[3] - tiepoint[0] * pixel_scale[0]
        origin_y = tiepoint[4] + tiepoint[1] * pixel_scale[1]
        geo_transform = (
            origin_x,       # top-left X (longitude)
            pixel_scale[0], # pixel width (degrees)
            0.0,
            origin_y,       # top-left Y (latitude)
            0.0,
            -pixel_scale[1] # pixel height (negative = north-up)
        )

    return width, height, pixel_data, geo_transform


def try_rasterio_parse(data: bytes):
    """Try parsing GeoTIFF with rasterio (if available).

    Returns (width, height, pixel_data_flat, geo_transform) or raises ImportError.
    """
    import rasterio
    with rasterio.open(io.BytesIO(data)) as src:
        arr = src.read(1)  # Band 1
        t = src.transform
        geo_transform = (t.c, t.a, t.b, t.f, t.d, t.e)
        flat = arr.flatten().tolist()
        return src.width, src.height, flat, geo_transform


def parse_geotiff(data: bytes):
    """Parse GeoTIFF with rasterio fallback to struct-based parser."""
    try:
        return try_rasterio_parse(data)
    except (ImportError, Exception):
        pass
    return parse_geotiff_float32(data)


# ──────────────────────────────────────────────────────────
# LiCSAR interferogram fetching & processing
# ──────────────────────────────────────────────────────────

async def fetch_geotiff(session: aiohttp.ClientSession, url: str) -> bytes | None:
    """Download a GeoTIFF file from JASMIN. Returns raw bytes or None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    return await resp.read()
                elif resp.status == 404:
                    return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.debug("GeoTIFF download failed: %s %s", url, type(e).__name__)
            await asyncio.sleep(2 ** attempt)
    return None


def extract_displacement_grid(unw_data: bytes, cc_data: bytes | None,
                               frame_id: str, date_pair: str):
    """Extract displacement and coherence values from interferogram GeoTIFFs.

    Converts unwrapped phase (radians) to LOS displacement (mm):
        d_LOS = (lambda / 4*pi) * phase * 1000

    Returns list of dicts with lat, lon, displacement_mm, coherence.
    Subsamples by PIXEL_STEP to keep data manageable.
    """
    import math

    try:
        w, h, unw_pixels, geo = parse_geotiff(unw_data)
    except (ValueError, struct.error) as e:
        logger.warning("  Failed to parse unw GeoTIFF for %s/%s: %s",
                       frame_id, date_pair, e)
        return []

    if geo is None:
        logger.warning("  No geotransform in unw GeoTIFF for %s/%s", frame_id, date_pair)
        return []

    # Parse coherence if available
    cc_pixels = None
    if cc_data:
        try:
            cw, ch, cc_pixels, _ = parse_geotiff(cc_data)
            if cw != w or ch != h:
                cc_pixels = None  # Dimension mismatch, ignore
        except (ValueError, struct.error):
            cc_pixels = None

    origin_x, px_w, _, origin_y, _, px_h = geo
    phase_to_mm = (S1_WAVELENGTH_M / (4.0 * math.pi)) * 1000.0

    rows = []
    for row in range(0, h, PIXEL_STEP):
        for col in range(0, w, PIXEL_STEP):
            idx = row * w + col
            if idx >= len(unw_pixels):
                continue

            phase = unw_pixels[idx]
            # Skip NaN / zero (masked areas)
            if phase != phase or phase == 0.0:  # NaN check + zero
                continue

            # Coherence filter
            coh = None
            if cc_pixels and idx < len(cc_pixels):
                coh = cc_pixels[idx]
                if coh != coh:  # NaN
                    coh = None
                elif coh < MIN_COHERENCE:
                    continue  # Low coherence — unreliable

            lon = origin_x + col * px_w
            lat = origin_y + row * px_h

            # Japan bounding box filter
            if not (JAPAN_LAT_MIN <= lat <= JAPAN_LAT_MAX
                    and JAPAN_LON_MIN <= lon <= JAPAN_LON_MAX):
                continue

            disp_mm = phase * phase_to_mm

            rows.append({
                "lat": round(lat, 3),
                "lon": round(lon, 3),
                "displacement_mm": round(disp_mm, 2),
                "coherence": round(coh, 3) if coh is not None else None,
            })

    return rows


async def process_frame(session: aiohttp.ClientSession,
                         frame_id: str, now_iso: str) -> int:
    """Process a single LiCSAR frame: discover pairs, download, extract.

    For each interferometric pair, computes displacement and stores
    as an approximate velocity (mm/yr) based on the temporal baseline.

    Returns number of records inserted.
    """
    from datetime import timedelta

    logger.info("  Processing frame: %s", frame_id)

    pairs = await discover_interferograms(session, frame_id)
    if not pairs:
        logger.info("    No interferograms found for %s", frame_id)
        return 0

    logger.info("    Found %d interferometric pairs, processing latest %d",
                len(pairs), min(len(pairs), MAX_PAIRS_PER_FRAME))

    track_num = frame_id[:3].lstrip("0") or "0"
    total_records = 0

    for pair in pairs[:MAX_PAIRS_PER_FRAME]:
        date1_str, date2_str = pair.split("_")

        # Compute temporal baseline (days)
        try:
            date1 = datetime.strptime(date1_str, "%Y%m%d")
            date2 = datetime.strptime(date2_str, "%Y%m%d")
            temporal_baseline_days = (date2 - date1).days
        except ValueError:
            continue

        if temporal_baseline_days <= 0:
            continue

        # Download unwrapped phase GeoTIFF
        unw_url = (f"{LICSAR_BASE}/{track_num}/{frame_id}/"
                   f"interferograms/{pair}/{pair}.geo.unw.tif")
        cc_url = (f"{LICSAR_BASE}/{track_num}/{frame_id}/"
                  f"interferograms/{pair}/{pair}.geo.cc.tif")

        unw_data = await fetch_geotiff(session, unw_url)
        if not unw_data:
            logger.debug("    %s: unw.tif not available", pair)
            continue

        # Coherence is optional but preferred
        cc_data = await fetch_geotiff(session, cc_url)

        grid = extract_displacement_grid(unw_data, cc_data, frame_id, pair)
        if not grid:
            logger.debug("    %s: no valid pixels extracted", pair)
            continue

        # Convert displacement to velocity (mm/year)
        # velocity = displacement / (temporal_baseline_days / 365.25)
        days_to_year = 365.25 / temporal_baseline_days

        records = []
        for pt in grid:
            velocity_mm_yr = round(pt["displacement_mm"] * days_to_year, 2)
            records.append((
                frame_id,
                date2_str[:4] + "-" + date2_str[4:6] + "-" + date2_str[6:8],
                pt["lat"],
                pt["lon"],
                velocity_mm_yr,
                pt["coherence"],
                now_iso,
            ))

        if records:
            async with safe_connect() as db:
                await db.executemany(
                    """INSERT OR IGNORE INTO insar_deformation
                       (frame_id, observed_at, cell_lat, cell_lon,
                        los_velocity_mm_yr, coherence, received_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    records,
                )
                await db.commit()
            total_records += len(records)
            logger.info("    %s: %d records (baseline %d days)",
                        pair, len(records), temporal_baseline_days)

        # Rate-limit between pairs
        await asyncio.sleep(1.0)

    return total_records


async def verify_frame_exists(session: aiohttp.ClientSession,
                                frame_id: str) -> bool:
    """Quick check: does this frame exist on JASMIN?"""
    track_num = frame_id[:3].lstrip("0") or "0"
    url = f"{LICSAR_BASE}/{track_num}/{frame_id}/"
    try:
        async with session.head(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            return resp.status == 200
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return False


async def discover_japan_frames_dynamic(session: aiohttp.ClientSession) -> list[str]:
    """Dynamically discover Japan frames from JASMIN directory listings.

    Scrapes the track-level directories for all known Japan-relevant
    Sentinel-1 orbits and finds frame IDs whose colatitude identifier
    falls within Japan's latitude range.

    Colatitude identifier (AAAAA) relates to geographic position:
    Japan (lat 24-46°N) corresponds to colatitude ~44-66°,
    which maps to identifiers roughly 04400-06600.
    """
    # Sentinel-1 tracks that pass over Japan
    # Descending: 46,47,48,119,120,175  Ascending: 10,39,54,83,112,141,156,170
    japan_tracks = [10, 39, 46, 47, 48, 54, 83, 112, 119, 120, 141, 156, 170, 175]

    discovered = []
    for track in japan_tracks:
        frames = await discover_frames_from_jasmin(session, track)
        for fid in frames:
            # Filter by colatitude range for Japan
            try:
                colat = int(fid.split("_")[1])
                # Japan colatitude range: ~04400 to ~06600
                if 4400 <= colat <= 6600:
                    discovered.append(fid)
            except (ValueError, IndexError):
                continue
        await asyncio.sleep(0.5)

    return discovered


async def main():
    await init_db()
    await init_insar_table()

    now = datetime.now(timezone.utc).isoformat()

    logger.info("=== InSAR Deformation Fetch (LiCSAR JASMIN) ===")

    async with aiohttp.ClientSession() as session:
        # Phase 1: Try hardcoded Japan frames first (fast, reliable)
        available_frames = []
        logger.info("Checking known Japan frames on JASMIN...")

        # Check a sample of frames to verify JASMIN connectivity
        sample = ALL_JAPAN_FRAMES[:5]
        check_tasks = [verify_frame_exists(session, f) for f in sample]
        results = await asyncio.gather(*check_tasks)

        if any(results):
            # JASMIN is reachable — verify all hardcoded frames
            all_tasks = [verify_frame_exists(session, f) for f in ALL_JAPAN_FRAMES]
            all_results = await asyncio.gather(*all_tasks)
            available_frames = [
                f for f, exists in zip(ALL_JAPAN_FRAMES, all_results) if exists
            ]
            logger.info("Verified %d / %d hardcoded frames exist on JASMIN",
                        len(available_frames), len(ALL_JAPAN_FRAMES))
        else:
            logger.info("JASMIN may be unreachable or frames moved, "
                        "trying dynamic discovery...")

        # Phase 2: If hardcoded frames didn't work, try dynamic discovery
        if not available_frames:
            discovered = await discover_japan_frames_dynamic(session)
            if discovered:
                logger.info("Dynamically discovered %d Japan frames", len(discovered))
                available_frames = discovered

        if not available_frames:
            logger.info(
                "No LiCSAR frames found on JASMIN for Japan. "
                "JASMIN may be temporarily unreachable or frame IDs may have "
                "changed. InSAR features will be excluded via dynamic selection."
            )
            return

        # Phase 3: Process frames (cap total to avoid excessive runtime)
        MAX_FRAMES_PER_RUN = 6
        frames_to_process = available_frames[:MAX_FRAMES_PER_RUN]

        logger.info("Processing %d frames (of %d available)",
                     len(frames_to_process), len(available_frames))

        total_records = 0
        for frame_id in frames_to_process:
            try:
                n = await process_frame(session, frame_id, now)
                total_records += n
            except Exception as e:
                logger.warning("  Frame %s failed: %s", frame_id, e)
            await asyncio.sleep(2.0)

        logger.info("InSAR fetch complete: %d records from %d frames",
                     total_records, len(frames_to_process))


if __name__ == "__main__":
    asyncio.run(main())
