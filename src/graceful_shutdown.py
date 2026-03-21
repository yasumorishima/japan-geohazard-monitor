"""Graceful shutdown handler for fetch scripts.

GitHub Actions sends SIGTERM on timeout. This module provides a flag
that fetch loops can check to break early, allowing already-fetched
data to be committed before the process exits.

Usage in fetch scripts:
    from graceful_shutdown import shutdown_requested, install_handler
    install_handler()

    for item in items:
        if shutdown_requested():
            logger.warning("Shutdown requested, saving progress...")
            break
        # ... fetch and commit ...
"""

import logging
import signal
import threading

logger = logging.getLogger(__name__)

_shutdown = threading.Event()


def install_handler():
    """Install SIGTERM/SIGINT handler that sets the shutdown flag."""
    def _handler(signum, frame):
        sig_name = signal.Signals(signum).name
        logger.warning("Received %s — finishing current batch and exiting", sig_name)
        _shutdown.set()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def shutdown_requested() -> bool:
    """Check if graceful shutdown has been requested."""
    return _shutdown.is_set()
