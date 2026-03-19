"""NASA Earthdata authentication helper.

NASA's OPeNDAP/DAAC endpoints use OAuth2 redirect flow:
1. Request to data endpoint -> 302 redirect to urs.earthdata.nasa.gov
2. Authenticate at URS -> 302 redirect back with session cookie
3. Data returned with cookie

Two authentication methods supported:
A. Username/Password (recommended for OPeNDAP redirect flow):
   - EARTHDATA_USERNAME + EARTHDATA_PASSWORD env vars
   - Creates .netrc-style BasicAuth for URS redirect
   - Works with all OPeNDAP endpoints
B. Bearer Token (for direct API access like AppEEARS):
   - EARTHDATA_TOKEN env var
   - Works for POST/GET to endpoints that accept Bearer directly
   - Does NOT work for OPeNDAP redirect flow

The core problem with Bearer tokens: aiohttp's allow_redirects=True follows
redirects but strips the Authorization header on cross-origin redirects.
Bearer tokens sent to gesdisc.eosdis.nasa.gov never reach urs.earthdata.nasa.gov.
Username/password via BasicAuth at URS solves this cleanly.
"""

import logging
import os

import aiohttp

logger = logging.getLogger(__name__)

EARTHDATA_TOKEN = os.environ.get("EARTHDATA_TOKEN")
EARTHDATA_USERNAME = os.environ.get("EARTHDATA_USERNAME")
EARTHDATA_PASSWORD = os.environ.get("EARTHDATA_PASSWORD")

URS_HOST = "urs.earthdata.nasa.gov"


async def get_earthdata_session() -> aiohttp.ClientSession:
    """Create an aiohttp session with Earthdata authentication.

    If EARTHDATA_USERNAME/PASSWORD are set, pre-authenticates at URS
    to establish session cookies. Otherwise falls back to Bearer token.
    """
    # unsafe=True allows cookies for IP-based URLs and cross-domain
    jar = aiohttp.CookieJar(unsafe=True)
    session = aiohttp.ClientSession(cookie_jar=jar)

    if EARTHDATA_USERNAME and EARTHDATA_PASSWORD:
        # Pre-authenticate at URS with Basic auth to get session cookies
        try:
            auth = aiohttp.BasicAuth(EARTHDATA_USERNAME, EARTHDATA_PASSWORD)
            auth_url = f"https://{URS_HOST}/api/users/{EARTHDATA_USERNAME}"
            async with session.get(auth_url, auth=auth) as resp:
                if resp.status == 200:
                    logger.info("Earthdata session authenticated via username/password")
                else:
                    logger.warning("Earthdata auth failed: HTTP %d", resp.status)
        except Exception as e:
            logger.warning("Earthdata auth error: %s", e)

    elif EARTHDATA_TOKEN:
        # Fallback: Bearer token (limited to direct API endpoints)
        try:
            auth_url = f"https://{URS_HOST}/api/users/tokens"
            headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}
            async with session.get(auth_url, headers=headers) as resp:
                if resp.status == 200:
                    logger.info("Earthdata session authenticated via Bearer token")
                else:
                    logger.warning("Earthdata auth (Bearer) failed: HTTP %d", resp.status)
        except Exception as e:
            logger.warning("Earthdata auth error: %s", e)

    else:
        logger.info("No Earthdata credentials set (EARTHDATA_USERNAME/PASSWORD or EARTHDATA_TOKEN)")

    return session


async def earthdata_fetch(
    session: aiohttp.ClientSession,
    url: str,
    timeout: aiohttp.ClientTimeout | None = None,
) -> tuple[int, str]:
    """Fetch a URL with Earthdata auth handling.

    For OPeNDAP redirect flow:
    1. Request data URL (gets 302 to URS)
    2. If we have username/password, send BasicAuth to URS
    3. URS redirects back to data URL with auth code
    4. Session cookies handle the rest

    Returns (status_code, response_text) tuple.
    """
    has_credentials = EARTHDATA_USERNAME and EARTHDATA_PASSWORD
    has_token = bool(EARTHDATA_TOKEN)

    if not has_credentials and not has_token:
        # No auth at all - just do a plain GET
        try:
            async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
                text = await resp.text()
                return resp.status, text
        except Exception as e:
            logger.warning("Fetch failed (no auth): %s", e)
            return 0, ""

    # --- Method A: Username/Password (handles OPeNDAP redirect flow) ---
    if has_credentials:
        urs_auth = aiohttp.BasicAuth(EARTHDATA_USERNAME, EARTHDATA_PASSWORD)

        # Step 1: Initial request with auto-redirect disabled
        try:
            async with session.get(url, timeout=timeout, allow_redirects=False) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return 200, text

                if resp.status in (301, 302, 303, 307, 308):
                    redirect_url = str(resp.headers.get("Location", ""))

                    if URS_HOST in redirect_url:
                        # Step 2: Authenticate at URS with Basic auth
                        # allow_redirects=True so URS can redirect back to data URL
                        async with session.get(
                            redirect_url,
                            auth=urs_auth,
                            allow_redirects=True,
                            timeout=timeout,
                        ) as auth_resp:
                            # Check if we got data or HTML login page
                            content_type = auth_resp.headers.get("Content-Type", "")
                            if "html" in content_type.lower():
                                body = await auth_resp.text()
                                if "<form" in body.lower() or "login" in body.lower():
                                    logger.warning("Earthdata auth: still on login page (bad credentials?)")
                                    return 401, ""
                                # Some data comes as HTML (e.g., OPeNDAP catalog)
                                return auth_resp.status, body
                            text = await auth_resp.text()
                            return auth_resp.status, text
                    else:
                        # Non-URS redirect, follow normally
                        async with session.get(
                            redirect_url,
                            allow_redirects=True,
                            timeout=timeout,
                        ) as redir_resp:
                            text = await redir_resp.text()
                            return redir_resp.status, text

                return resp.status, ""

        except (aiohttp.ClientError, TimeoutError) as e:
            logger.warning("Earthdata fetch error: %s", e)
            return 0, ""

    # --- Method B: Bearer Token (direct API only, no redirect support) ---
    headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}

    try:
        # Try direct access with Bearer header
        async with session.get(url, headers=headers, timeout=timeout, allow_redirects=False) as resp:
            if resp.status == 200:
                text = await resp.text()
                return 200, text

            if resp.status in (301, 302, 303, 307, 308):
                redirect_url = str(resp.headers.get("Location", ""))

                if URS_HOST in redirect_url:
                    # Bearer tokens don't work well with redirect flow
                    # Try sending Bearer to URS anyway (works for some endpoints)
                    async with session.get(
                        redirect_url,
                        headers=headers,
                        allow_redirects=True,
                        timeout=timeout,
                    ) as auth_resp:
                        content_type = auth_resp.headers.get("Content-Type", "")
                        if "html" in content_type.lower():
                            return 401, ""
                        text = await auth_resp.text()
                        return auth_resp.status, text
                else:
                    async with session.get(
                        redirect_url,
                        headers=headers,
                        allow_redirects=True,
                        timeout=timeout,
                    ) as redir_resp:
                        text = await redir_resp.text()
                        return redir_resp.status, text

            return resp.status, ""

    except (aiohttp.ClientError, TimeoutError) as e:
        logger.warning("Earthdata fetch error (Bearer): %s", e)
        return 0, ""


async def earthdata_fetch_bytes(
    session: aiohttp.ClientSession,
    url: str,
    timeout: aiohttp.ClientTimeout | None = None,
) -> tuple[int, bytes]:
    """Fetch binary data (NetCDF, HDF5, etc.) with Earthdata auth.

    Same redirect handling as earthdata_fetch but returns bytes.
    """
    has_credentials = EARTHDATA_USERNAME and EARTHDATA_PASSWORD

    if has_credentials:
        urs_auth = aiohttp.BasicAuth(EARTHDATA_USERNAME, EARTHDATA_PASSWORD)

        try:
            async with session.get(url, timeout=timeout, allow_redirects=False) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    return 200, data

                if resp.status in (301, 302, 303, 307, 308):
                    redirect_url = str(resp.headers.get("Location", ""))
                    if URS_HOST in redirect_url:
                        async with session.get(
                            redirect_url,
                            auth=urs_auth,
                            allow_redirects=True,
                            timeout=timeout,
                        ) as auth_resp:
                            if auth_resp.status == 200:
                                data = await auth_resp.read()
                                return 200, data
                            return auth_resp.status, b""
                    else:
                        async with session.get(
                            redirect_url,
                            allow_redirects=True,
                            timeout=timeout,
                        ) as redir_resp:
                            data = await redir_resp.read()
                            return redir_resp.status, data

                return resp.status, b""

        except (aiohttp.ClientError, TimeoutError) as e:
            logger.warning("Earthdata fetch_bytes error: %s", e)
            return 0, b""

    # Bearer fallback
    headers = {}
    if EARTHDATA_TOKEN:
        headers["Authorization"] = f"Bearer {EARTHDATA_TOKEN}"

    try:
        async with session.get(url, headers=headers, timeout=timeout, allow_redirects=True) as resp:
            data = await resp.read()
            return resp.status, data
    except (aiohttp.ClientError, TimeoutError) as e:
        logger.warning("Earthdata fetch_bytes error: %s", e)
        return 0, b""
