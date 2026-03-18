"""NASA Earthdata authentication helper.

NASA's OPeNDAP/DAAC endpoints use OAuth2 redirect flow:
1. Request to data endpoint -> 302 redirect to urs.earthdata.nasa.gov
2. Authenticate at URS -> 302 redirect back with session cookie
3. Data returned with cookie

With Bearer tokens, we need to:
- First authenticate at URS to get session cookies
- Then use those cookies for data requests

The core problem: aiohttp's allow_redirects=True follows redirects but
strips the Authorization header on cross-origin redirects (security).
So a Bearer token sent to gesdisc.eosdis.nasa.gov never reaches
urs.earthdata.nasa.gov during the auth redirect.

Solution: Disable auto-redirects, detect the URS redirect, and manually
send the Bearer token to URS, then follow back with session cookies.
"""

import logging
import os

import aiohttp

logger = logging.getLogger(__name__)

EARTHDATA_TOKEN = os.environ.get("EARTHDATA_TOKEN")


async def get_earthdata_session() -> aiohttp.ClientSession:
    """Create an aiohttp session with Earthdata authentication.

    Uses Bearer token to pre-authenticate at URS, storing cookies
    in the session's cookie jar for subsequent data requests.
    """
    if not EARTHDATA_TOKEN:
        return aiohttp.ClientSession()

    # unsafe=True allows cookies for IP-based URLs and cross-domain
    jar = aiohttp.CookieJar(unsafe=True)
    session = aiohttp.ClientSession(cookie_jar=jar)

    # Pre-authenticate: hit the URS token endpoint to establish session
    try:
        auth_url = "https://urs.earthdata.nasa.gov/api/users/tokens"
        headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}
        async with session.get(auth_url, headers=headers) as resp:
            if resp.status == 200:
                logger.info("Earthdata session authenticated via Bearer token")
            else:
                logger.warning("Earthdata auth failed: HTTP %d", resp.status)
    except Exception as e:
        logger.warning("Earthdata auth error: %s", e)

    return session


async def earthdata_fetch(
    session: aiohttp.ClientSession,
    url: str,
    timeout: aiohttp.ClientTimeout | None = None,
) -> tuple[int, str]:
    """Fetch a URL with Earthdata auth handling.

    Handles the redirect flow by:
    1. Making initial request (may redirect to URS)
    2. If redirected, adding Bearer token to URS request
    3. Following back to data URL with session cookies

    Returns (status_code, response_text) tuple.
    """
    if not EARTHDATA_TOKEN:
        # No token - just do a plain GET
        async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
            text = await resp.text()
            return resp.status, text

    headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}

    # Disable auto-redirects so we can add auth to the redirect target
    async with session.get(url, timeout=timeout, allow_redirects=False) as resp:
        if resp.status == 200:
            text = await resp.text()
            return 200, text

        if resp.status in (301, 302, 303, 307, 308):
            redirect_url = str(resp.headers.get("Location", ""))

            if "urs.earthdata.nasa.gov" in redirect_url:
                # Auth redirect - send Bearer token to URS
                async with session.get(
                    redirect_url,
                    headers=headers,
                    allow_redirects=True,
                    timeout=timeout,
                ) as auth_resp:
                    if auth_resp.status == 200:
                        content_type = auth_resp.headers.get("Content-Type", "")
                        if "html" in content_type.lower():
                            # Still on auth page - token may be invalid
                            return 401, ""
                        text = await auth_resp.text()
                        return 200, text
                    return auth_resp.status, ""
            else:
                # Non-auth redirect, follow normally with auth header
                async with session.get(
                    redirect_url,
                    headers=headers,
                    allow_redirects=True,
                    timeout=timeout,
                ) as redir_resp:
                    text = await redir_resp.text()
                    return redir_resp.status, text

        if resp.status in (401, 403):
            return resp.status, ""

        return resp.status, ""
