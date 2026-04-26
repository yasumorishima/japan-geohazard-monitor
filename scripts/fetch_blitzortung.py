"""Deprecated stub for Blitzortung lightning fetcher.

DEPRECATION (2026-04-26):
    Blitzortung historical archive returns HTML access-restricted
    response for every queried month. University of Bonn sferics
    archive is EU-only (no Japan coverage). Blitzortung live API
    only exposes the most recent ~2 hours of strokes (no historical
    backfill capability). Daily historical lightning data from
    paid sources (GLD360 / ENTLN) is out of scope.

    The `lightning` table has been at 0 rows since the start of
    the project. Active lightning coverage is provided via:
        - iss_lis_lightning (NASA ISS LIS, 2017-2023)
        - lightning_thunder_hour (WWLLN ThunderHour)
        - lightning_lis_otd (NASA LIS/OTD monthly climatology)

    This fetcher is replaced by a no-op to avoid wasting CI time
    on a permanently-failing source. The historical implementation
    is preserved in the file's git history (~575 lines).

    See docs/DATA_QUALITY_ISSUES.md for context.
"""

import sys


def main() -> int:
    print(
        "Blitzortung lightning fetcher is deprecated -- archive access "
        "restricted, no historical fallback. No fetch performed. "
        "See docs/DATA_QUALITY_ISSUES.md for context.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
