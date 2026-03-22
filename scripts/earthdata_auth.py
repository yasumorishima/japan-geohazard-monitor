"""NASA Earthdata authentication helper.

Authentication strategy (2026-03 update):
- Bearer token is now the primary method (URS Basic Auth API deprecated)
- LAADS DAAC (VIIRS, MODIS cloud) accepts Bearer directly
- OPeNDAP endpoints (GES DISC SO2) use redirect flow with Basic Auth at URS
- Both methods are tried: Bearer first, then Basic Auth redirect fallback

Environment variables:
  EARTHDATA_TOKEN     - Bearer token (generate at urs.earthdata.nasa.gov/profile)
  EARTHDATA_USERNAME  - URS username (for OPeNDAP redirect flow fallback)
  EARTHDATA_PASSWORD  - URS password (for OPeNDAP redirect flow fallback)
"""

import logging
import os
from urllib.parse import urljoin

import aiohttp

logger = logging.getLogger(__name__)

EARTHDATA_TOKEN = os.environ.get("EARTHDATA_TOKEN")
EARTHDATA_USERNAME = os.environ.get("EARTHDATA_USERNAME")
EARTHDATA_PASSWORD = os.environ.get("EARTHDATA_PASSWORD")

URS_HOST = "urs.earthdata.nasa.gov"


def _resolve_redirect(base_url: str, location: str) -> str:
    """Resolve a redirect Location header, handling relative paths."""
    if not location:
        return ""
    if location.startswith("http://") or location.startswith("https://"):
        return location
    return urljoin(base_url, location)


async def get_earthdata_session() -> aiohttp.ClientSession:
    """Create an aiohttp session for Earthdata access.

    No pre-authentication (URS profile API no longer accepts Basic Auth).
    Auth is handled per-request in earthdata_fetch/earthdata_fetch_bytes.
    """
    jar = aiohttp.CookieJar(unsafe=True)
    session = aiohttp.ClientSession(cookie_jar=jar)

    has_token = bool(EARTHDATA_TOKEN)
    has_credentials = EARTHDATA_USERNAME and EARTHDATA_PASSWORD

    if has_token:
        logger.info("Earthdata session created (Bearer token available, len=%d)", len(EARTHDATA_TOKEN))
    elif has_credentials:
        logger.info("Earthdata session created (username/password available, no token)")
    else:
        logger.info("No Earthdata credentials set")

    return session


async def earthdata_fetch(
    session: aiohttp.ClientSession,
    url: str,
    timeout: aiohttp.ClientTimeout | None = None,
) -> tuple[int, str]:
    """Fetch a URL with Earthdata auth handling.

    Strategy:
    1. Try Bearer token (works for LAADS DAAC, some direct APIs)
    2. If redirect to URS detected, use Basic Auth (works for OPeNDAP)
    3. Fallback: no auth

    Returns (status_code, response_text) tuple.
    """
    has_token = bool(EARTHDATA_TOKEN)
    has_credentials = EARTHDATA_USERNAME and EARTHDATA_PASSWORD

    if not has_token and not has_credentials:
        try:
            async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
                text = await resp.text()
                return resp.status, text
        except Exception as e:
            logger.warning("Fetch failed (no auth): %s", e)
            return 0, ""

    # --- Try Bearer token first ---
    if has_token:
        headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}
        try:
            async with session.get(url, headers=headers, timeout=timeout, allow_redirects=False) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return 200, text

                if resp.status in (301, 302, 303, 307, 308):
                    redirect_url = _resolve_redirect(
                        url, str(resp.headers.get("Location", "")))

                    if not redirect_url:
                        logger.warning("Redirect with empty Location header")
                        return resp.status, ""

                    if URS_HOST in redirect_url and has_credentials:
                        # OPeNDAP redirect: Bearer won't work, use Basic Auth
                        # Use fresh session to avoid cookie jar contamination
                        fresh_jar = aiohttp.CookieJar(unsafe=True)
                        async with aiohttp.ClientSession(cookie_jar=fresh_jar) as fresh_session:
                            return await _fetch_with_basic_auth(fresh_session, redirect_url, timeout)

                    # Non-URS redirect: follow with Bearer
                    async with session.get(
                        redirect_url, headers=headers,
                        allow_redirects=True, timeout=timeout,
                    ) as redir_resp:
                        text = await redir_resp.text()
                        return redir_resp.status, text

                # Other status codes (401, 403, etc.) - fall through to Basic Auth
                if resp.status in (401, 403) and has_credentials:
                    logger.info("Bearer rejected (HTTP %d), trying Basic Auth redirect", resp.status)
                else:
                    return resp.status, ""

        except (aiohttp.ClientError, TimeoutError) as e:
            logger.warning("Earthdata fetch error (Bearer): %s", e)
            if not has_credentials:
                return 0, ""

    # --- Fallback: Basic Auth redirect flow ---
    # Use a fresh session to avoid cookie contamination from Bearer attempt
    if has_credentials:
        jar = aiohttp.CookieJar(unsafe=True)
        async with aiohttp.ClientSession(cookie_jar=jar) as fallback_session:
            try:
                async with fallback_session.get(url, timeout=timeout, allow_redirects=False) as resp:
                    logger.info("BasicAuth fallback: HTTP %d for %s", resp.status, url[:80])
                    if resp.status == 200:
                        text = await resp.text()
                        return 200, text

                    if resp.status in (301, 302, 303, 307, 308):
                        redirect_url = _resolve_redirect(
                            url, str(resp.headers.get("Location", "")))
                        if not redirect_url:
                            return resp.status, ""
                        if URS_HOST in redirect_url:
                            return await _fetch_with_basic_auth(fallback_session, redirect_url, timeout)
                        # Non-URS redirect
                        async with fallback_session.get(
                            redirect_url, allow_redirects=True, timeout=timeout,
                        ) as redir_resp:
                            text = await redir_resp.text()
                            return redir_resp.status, text

                    logger.info("BasicAuth fallback: unexpected status %d", resp.status)
                    return resp.status, ""

            except (aiohttp.ClientError, TimeoutError) as e:
                logger.warning("Earthdata fetch error (Basic Auth): %s", e)
                return 0, ""

    return 0, ""


async def _fetch_with_basic_auth(
    session: aiohttp.ClientSession,
    urs_redirect_url: str,
    timeout: aiohttp.ClientTimeout | None,
) -> tuple[int, str]:
    """Follow URS redirect with Basic Auth credentials."""
    urs_auth = aiohttp.BasicAuth(EARTHDATA_USERNAME, EARTHDATA_PASSWORD)
    async with session.get(
        urs_redirect_url, auth=urs_auth,
        allow_redirects=True, timeout=timeout,
    ) as auth_resp:
        content_type = auth_resp.headers.get("Content-Type", "")
        if "html" in content_type.lower():
            body = await auth_resp.text()
            if "<form" in body.lower() or "login" in body.lower():
                logger.info("Earthdata BasicAuth: login page returned (bad credentials)")
                return 401, ""
            # Any HTML response from a data endpoint is an auth/access failure
            # (e.g. GES DISC error page, EULA acceptance page)
            logger.info(
                "Earthdata BasicAuth: HTML returned instead of data "
                "(HTTP %d, len=%d, preview=%s)",
                auth_resp.status, len(body), repr(body[:120]),
            )
            return 401, ""
        text = await auth_resp.text()
        return auth_resp.status, text


async def earthdata_fetch_bytes(
    session: aiohttp.ClientSession,
    url: str,
    timeout: aiohttp.ClientTimeout | None = None,
) -> tuple[int, bytes]:
    """Fetch binary data (NetCDF, HDF5, etc.) with Earthdata auth.

    Strategy: Bearer token first, Basic Auth redirect fallback.
    """
    has_token = bool(EARTHDATA_TOKEN)
    has_credentials = EARTHDATA_USERNAME and EARTHDATA_PASSWORD

    # --- Try Bearer token first ---
    if has_token:
        headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}
        try:
            async with session.get(url, headers=headers, timeout=timeout, allow_redirects=False) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    return 200, data

                if resp.status in (301, 302, 303, 307, 308):
                    redirect_url = _resolve_redirect(
                        url, str(resp.headers.get("Location", "")))

                    if not redirect_url:
                        logger.warning("Redirect with empty Location header")
                        return resp.status, b""

                    if URS_HOST in redirect_url and has_credentials:
                        fresh_jar = aiohttp.CookieJar(unsafe=True)
                        async with aiohttp.ClientSession(cookie_jar=fresh_jar) as fresh_session:
                            return await _fetch_bytes_with_basic_auth(fresh_session, redirect_url, timeout)

                    # Non-URS redirect: follow with Bearer
                    async with session.get(
                        redirect_url, headers=headers,
                        allow_redirects=True, timeout=timeout,
                    ) as redir_resp:
                        data = await redir_resp.read()
                        return redir_resp.status, data

                if resp.status in (401, 403) and has_credentials:
                    logger.info("Bearer rejected (HTTP %d), trying Basic Auth redirect", resp.status)
                else:
                    return resp.status, b""

        except (aiohttp.ClientError, TimeoutError) as e:
            logger.warning("Earthdata fetch_bytes error (Bearer): %s", e)
            if not has_credentials:
                return 0, b""

    # --- Fallback: Basic Auth redirect flow ---
    # Use a fresh session to avoid cookie contamination from Bearer attempt
    if has_credentials:
        jar = aiohttp.CookieJar(unsafe=True)
        async with aiohttp.ClientSession(cookie_jar=jar) as fallback_session:
            try:
                async with fallback_session.get(url, timeout=timeout, allow_redirects=False) as resp:
                    logger.info("BasicAuth fallback (bytes): HTTP %d for %s", resp.status, url[:80])
                    if resp.status == 200:
                        data = await resp.read()
                        return 200, data

                    if resp.status in (301, 302, 303, 307, 308):
                        redirect_url = _resolve_redirect(
                            url, str(resp.headers.get("Location", "")))
                        if not redirect_url:
                            return resp.status, b""
                        if URS_HOST in redirect_url:
                            return await _fetch_bytes_with_basic_auth(fallback_session, redirect_url, timeout)
                        async with fallback_session.get(
                            redirect_url, allow_redirects=True, timeout=timeout,
                        ) as redir_resp:
                            data = await redir_resp.read()
                            return redir_resp.status, data

                    logger.info("BasicAuth fallback (bytes): unexpected status %d", resp.status)
                    return resp.status, b""

            except (aiohttp.ClientError, TimeoutError) as e:
                logger.warning("Earthdata fetch_bytes error (Basic Auth): %s", e)
                return 0, b""

    # No auth
    try:
        async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
            data = await resp.read()
            return resp.status, data
    except (aiohttp.ClientError, TimeoutError) as e:
        logger.warning("Earthdata fetch_bytes error: %s", e)
        return 0, b""


async def _fetch_bytes_with_basic_auth(
    session: aiohttp.ClientSession,
    urs_redirect_url: str,
    timeout: aiohttp.ClientTimeout | None,
) -> tuple[int, bytes]:
    """Follow URS redirect with Basic Auth credentials (binary)."""
    urs_auth = aiohttp.BasicAuth(EARTHDATA_USERNAME, EARTHDATA_PASSWORD)
    async with session.get(
        urs_redirect_url, auth=urs_auth,
        allow_redirects=True, timeout=timeout,
    ) as auth_resp:
        if auth_resp.status == 200:
            data = await auth_resp.read()
            return 200, data
        return auth_resp.status, b""
