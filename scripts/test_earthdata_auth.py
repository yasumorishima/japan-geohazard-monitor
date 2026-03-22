"""Quick Earthdata credential validation test.

Tests actual data access flows (not just API endpoints):
1. LAADS DAAC file download with Bearer token (VIIRS/MODIS style)
2. OPeNDAP redirect flow with Basic Auth (GES DISC SO2 style)
3. CMR search API (no auth, just connectivity)

Run: python3 scripts/test_earthdata_auth.py
Env: EARTHDATA_USERNAME, EARTHDATA_PASSWORD, EARTHDATA_TOKEN (optional)
"""
import asyncio
import os
import sys

import aiohttp


async def main():
    username = os.environ.get("EARTHDATA_USERNAME", "")
    password = os.environ.get("EARTHDATA_PASSWORD", "")
    token = os.environ.get("EARTHDATA_TOKEN", "")

    print(f"EARTHDATA_USERNAME set: {bool(username)} (len={len(username)})")
    print(f"EARTHDATA_PASSWORD set: {bool(password)} (len={len(password)})")
    print(f"EARTHDATA_TOKEN set: {bool(token)} (len={len(token)})")

    ok = True

    if not username or not password:
        print("\nERROR: EARTHDATA_USERNAME or EARTHDATA_PASSWORD not set")
        sys.exit(1)

    jar = aiohttp.CookieJar(unsafe=True)
    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(cookie_jar=jar) as session:
        auth = aiohttp.BasicAuth(username, password)

        # Test 1: LAADS DAAC with Bearer token
        # This is the actual flow used by VIIRS/MODIS cloud fraction fetchers
        print("\n--- Test 1: LAADS DAAC Bearer Token ---")
        if token:
            try:
                # Small MODIS file listing (not download)
                laads_url = "https://ladsweb.modaps.eosdis.nasa.gov/api/v2/content/details/allData/61/MOD08_D3/2024/001"
                headers = {"Authorization": f"Bearer {token}"}
                async with session.get(laads_url, headers=headers, timeout=timeout) as resp:
                    print(f"HTTP {resp.status}")
                    if resp.status == 200:
                        text = (await resp.text())[:300]
                        if "invalid_credentials" in text or "error" in text[:50].lower():
                            print(f"FAIL: LAADS returned 200 but body contains error")
                            print(f"Body: {text}")
                            ok = False
                        else:
                            print(f"PASS: LAADS API accessible")
                            print(f"Preview: {text[:200]}")
                    elif resp.status == 401:
                        body = (await resp.text())[:300]
                        print(f"FAIL: Bearer token rejected by LAADS")
                        print(f"Body: {body}")
                        ok = False
                    elif resp.status == 403:
                        body = (await resp.text())[:300]
                        print(f"FAIL: Bearer token forbidden (may need LAADS app approval)")
                        print(f"Body: {body}")
                        ok = False
                    else:
                        body = (await resp.text())[:300]
                        print(f"UNEXPECTED HTTP {resp.status}: {body}")
            except Exception as e:
                print(f"ERROR: {e}")
        else:
            print("SKIP: EARTHDATA_TOKEN not set")

        # Test 2: GES DISC OPeNDAP actual data download (not catalog)
        # Must test a real .ascii data URL, not contents.html (which is public)
        print("\n--- Test 2: GES DISC OPeNDAP Data Download ---")
        try:
            # Actual SO2 data subset (single cell, tiny response)
            test_url = (
                "https://acdisc.gesdisc.eosdis.nasa.gov/opendap/HDF-EOS5/"
                "Aura_OMI_Level2G/OMSO2G.003/2024/"
                "OMI-Aura_L2G-OMSO2G_2024m0101_v003-2024m0103t164059.he5.ascii"
                "?ColumnAmountSO2_PBL[0:0][500:500][1250:1250]"
            )
            # Try with Bearer first (expect redirect to URS)
            headers_t2 = {"Authorization": f"Bearer {token}"} if token else {}
            async with session.get(test_url, headers=headers_t2, allow_redirects=False, timeout=timeout) as resp:
                print(f"Initial: HTTP {resp.status}")
                if resp.status in (301, 302, 303, 307, 308):
                    redirect_url = str(resp.headers.get("Location", ""))
                    is_urs = "urs.earthdata" in redirect_url
                    print(f"Redirect to URS: {is_urs}")
                    if is_urs:
                        # Use fresh session for Basic Auth (matches production flow)
                        jar2 = aiohttp.CookieJar(unsafe=True)
                        async with aiohttp.ClientSession(cookie_jar=jar2) as auth_session:
                            async with auth_session.get(
                                redirect_url, auth=auth,
                                allow_redirects=True, timeout=timeout,
                            ) as auth_resp:
                                ct = auth_resp.headers.get("Content-Type", "")
                                body = (await auth_resp.text())[:500]
                                print(f"After auth: HTTP {auth_resp.status} CT={ct[:50]}")
                                if auth_resp.status == 200:
                                    if "<html" in body[:200].lower():
                                        print("FAIL: Got HTML instead of data (EULA/access issue?)")
                                        print(f"Preview: {body[:200]}")
                                        ok = False
                                    else:
                                        print("PASS: Got ASCII data via OPeNDAP")
                                        print(f"Preview: {body[:150]}")
                                else:
                                    print(f"FAIL: HTTP {auth_resp.status}")
                                    print(f"Body: {body[:300]}")
                                    ok = False
                    else:
                        print(f"Non-URS redirect: {redirect_url[:100]}")
                elif resp.status == 200:
                    body = (await resp.text())[:500]
                    if "<html" in body[:200].lower():
                        print("FAIL: Got HTML instead of data (auth not applied?)")
                        ok = False
                    else:
                        print("PASS: Direct access returned data")
                        print(f"Preview: {body[:150]}")
                elif resp.status in (401, 403):
                    print(f"FAIL: Auth rejected (HTTP {resp.status})")
                    # Fall through to Basic Auth test below
                    if username and password:
                        print("Trying direct Basic Auth fallback...")
                        jar3 = aiohttp.CookieJar(unsafe=True)
                        async with aiohttp.ClientSession(cookie_jar=jar3) as fb_session:
                            async with fb_session.get(test_url, timeout=timeout, allow_redirects=False) as fb_resp:
                                if fb_resp.status in (301, 302, 303, 307, 308):
                                    redir = str(fb_resp.headers.get("Location", ""))
                                    async with fb_session.get(redir, auth=auth, allow_redirects=True, timeout=timeout) as fb_auth:
                                        fb_body = (await fb_auth.text())[:500]
                                        if fb_auth.status == 200 and "<html" not in fb_body[:200].lower():
                                            print(f"PASS: BasicAuth fallback succeeded")
                                            print(f"Preview: {fb_body[:150]}")
                                        else:
                                            print(f"FAIL: BasicAuth fallback HTTP {fb_auth.status}")
                                            if "<html" in fb_body[:200].lower():
                                                print(f"HTML preview: {fb_body[:200]}")
                                            ok = False
                                else:
                                    print(f"FAIL: No redirect in fallback (HTTP {fb_resp.status})")
                                    ok = False
                    else:
                        ok = False
                elif resp.status == 404:
                    print("WARN: Test file not found (not an auth issue, may need filename update)")
                else:
                    body = (await resp.text())[:300]
                    print(f"UNEXPECTED HTTP {resp.status}: {body}")
        except Exception as e:
            print(f"ERROR: {e}")
            ok = False

        # Test 3: Earthdata token via profile API (Bearer)
        print("\n--- Test 3: URS Token API ---")
        if token:
            try:
                async with session.get(
                    "https://urs.earthdata.nasa.gov/api/users/tokens",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=timeout,
                ) as resp:
                    print(f"HTTP {resp.status}")
                    if resp.status == 200:
                        data = await resp.json()
                        print(f"Token count: {len(data)}")
                        if data:
                            exp = data[0].get("expiration_date", "unknown")
                            print(f"Expiration: {exp}")
                        print("PASS: Bearer token valid")
                    else:
                        body = (await resp.text())[:300]
                        print(f"INFO: HTTP {resp.status} (API may require different auth)")
                        print(f"Body: {body}")
            except Exception as e:
                print(f"ERROR: {e}")
        else:
            print("SKIP: No token")

        # Test 4: CMR API (no auth, connectivity check)
        print("\n--- Test 4: CMR API (no auth) ---")
        try:
            cmr_url = (
                "https://cmr.earthdata.nasa.gov/search/granules.json"
                "?short_name=isslis_v2_fin&page_size=1"
            )
            async with session.get(cmr_url, timeout=timeout) as resp:
                print(f"HTTP {resp.status}")
                if resp.status == 200:
                    data = await resp.json()
                    n = len(data.get("feed", {}).get("entry", []))
                    print(f"PASS: CMR returned {n} granule(s)")
                else:
                    print(f"FAIL: {(await resp.text())[:200]}")
        except Exception as e:
            print(f"ERROR: {e}")

    print(f"\n{'='*40}")
    print(f"Result: {'ALL PASS' if ok else 'SOME FAILED'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
