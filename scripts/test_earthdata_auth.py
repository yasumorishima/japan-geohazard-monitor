"""Quick Earthdata credential validation test.

Tests actual data access flows (not just API endpoints):
1. LAADS DAAC content listing (connectivity only: this listing is public, it does not test auth)
2. Earthdata Cloud OPeNDAP data subset with the Bearer token (SO2; rejects a missing/expired token)
3. URS token list with Basic auth (tests the password; checks EARTHDATA_TOKEN is a live token)
4. CMR search API (no auth, just connectivity)

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
        print("\n--- Test 1: LAADS DAAC listing (public; connectivity only) ---")
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

        # Test 2: Earthdata Cloud OPeNDAP actual data download (not catalog)
        # GES DISC retired its on-premises OPeNDAP (HTTP 410, seen 2026-11-13).
        # Cloud OPeNDAP answers 403 for a bad/expired Bearer token and 302 with none,
        # so a 200 with ASCII data proves the token works for SO2.
        print("\n--- Test 2: Cloud OPeNDAP Data Download (Bearer) ---")
        if token:
            try:
                test_url = (
                    "https://opendap.earthdata.nasa.gov/collections/C3685693295-GES_DISC/granules/OMSO2G.004%3AOMI-Aura_L2G-OMSO2G_2024m0101_v004-2025m0904t091514.he5.ascii"
                    "?/HDFEOS/GRIDS/OMI_Total_Column_Amount_SO2/Data_Fields/ColumnAmountSO2_PBL[0:0][500:500][1250:1250]"
                )
                headers_t2 = {"Authorization": f"Bearer {token}"}
                async with session.get(test_url, headers=headers_t2, allow_redirects=False, timeout=timeout) as resp:
                    body = (await resp.text())[:500]
                    print(f"HTTP {resp.status}")
                    if resp.status == 200 and body.startswith("Dataset:") and "ColumnAmountSO2_PBL" in body:
                        print("PASS: Got ASCII data via cloud OPeNDAP")
                        print(f"Preview: {body[:150]}")
                    else:
                        print("FAIL: no ASCII data (403 = token rejected or expired, 302 = no token)")
                        print(f"Body: {body[:200]}")
                        ok = False
            except Exception as e:
                print(f"ERROR: {e}")
                ok = False
        else:
            print("FAIL: EARTHDATA_TOKEN not set")
            ok = False

        # Test 3: URS token list (Basic auth = the account password)
        # The token API does not accept a Bearer token (always 401); it needs Basic auth.
        print("\n--- Test 3: URS Token API (Basic auth) ---")
        try:
            async with session.get(
                "https://urs.earthdata.nasa.gov/api/users/tokens",
                auth=auth, timeout=timeout,
            ) as resp:
                print(f"HTTP {resp.status}")
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    print(f"PASS: password accepted, account has {len(data)} token(s)")
                    mine = [t for t in data if token and t.get("access_token") == token]
                    if mine:
                        print(f"PASS: EARTHDATA_TOKEN is a live token, expires {mine[0].get('expiration_date', 'unknown')}")
                    else:
                        print("FAIL: EARTHDATA_TOKEN is not among the account's live tokens (expired or replaced)")
                        ok = False
                else:
                    print("FAIL: password rejected by the token API")
                    ok = False
        except Exception as e:
            print(f"ERROR: {e}")
            ok = False

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
