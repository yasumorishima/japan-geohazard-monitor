"""Deprecated stub for S-net seafloor pressure fetcher.

DEPRECATION (2026-04-25):
    HinetPy network code 0120A is S-net *acceleration* data, NOT pressure.
    HinetPy does not expose any network code for S-net BPR (bottom pressure
    recorder) seafloor pressure measurements.
    See HinetPy/header.py for the canonical network registry:
    https://github.com/seisman/HinetPy/blob/master/HinetPy/header.py

    Without HinetPy support, S-net pressure data must be obtained via NIED
    direct data request, which is out of scope for an automated fetcher.
    See README.md "Deprecated data sources" for details.

This file is kept as a tombstone so external callers and the workflow
(if any reference slips through) get a clear deprecation message instead
of a 404 / decode failure. The fetcher exits 0 without touching the DB.
"""

import sys


def main() -> int:
    print(
        "snet_pressure fetcher is deprecated -- HinetPy 0120A is acceleration, "
        "not pressure. No fetch performed. See README.md for context.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
