"""Quick Earthdata credential validation test.

Tests actual data access flows (not just API endpoints):
1. LAADS DAAC content listing (connectivity only: this listing is public, it does not test auth)
2. Earthdata Cloud OPeNDAP data subset with the Bearer token (SO2; 403 for a bad/expired token)
3. URS token list with Basic auth (tests the password; checks EARTHDATA_TOKEN is a live token)
The exit code fails only when neither the token nor the password works (it gates
several fetchers, some of which use the password only); one-sided problems warn.
4. CMR search API (no auth, just connectivity)

Run: python3 scripts/test_earthdata_auth.py
Env: EARTHDATA_USERNAME, EARTHDATA_PASSWORD, EARTHDATA_TOKEN (optional)
"""
import asyncio
import json
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
                            print(f"WARN: LAADS returned 200 but body contains error")
                            print(f"Body: {text}")
                            pass  # Test 1 is connectivity only; it does not decide the exit code
                        else:
                            print(f"PASS: LAADS API accessible")
                            print(f"Preview: {text[:200]}")
                    elif resp.status == 401:
                        body = (await resp.text())[:300]
                        print(f"WARN: Bearer token rejected by LAADS")
                        print(f"Body: {body}")
                        pass  # Test 1 is connectivity only; it does not decide the exit code
                    elif resp.status == 403:
                        body = (await resp.text())[:300]
                        print(f"WARN: Bearer token forbidden (may need LAADS app approval)")
                        print(f"Body: {body}")
                        pass  # Test 1 is connectivity only; it does not decide the exit code
                    else:
                        body = (await resp.text())[:300]
                        print(f"UNEXPECTED HTTP {resp.status}: {body}")
            except Exception as e:
                print(f"ERROR: {e}")
        else:
            print("SKIP: EARTHDATA_TOKEN not set")

        # Tests 2 and 3 decide the exit code together: it gates SO2, cloud,
        # ISS LIS, WWLLN, LIS/OTD and nightlight in backfill.yml / analysis.yml.
        # Some of those log in with the password only, and earthdata_fetch falls
        # back from a rejected token to Basic auth, so the run fails only when
        # NEITHER the token NOR the password works. One-sided problems warn.
        token_ok = None     # None = not tested / inconclusive
        password_ok = None

        async def _get_with_retry(url, **kw):
            """GET with 3 tries on 5xx / network errors. Returns (status, text)."""
            last = None
            for attempt in range(1, 4):
                try:
                    async with session.get(url, timeout=timeout, **kw) as resp:
                        text = await resp.text()
                        if resp.status < 500:
                            return resp.status, text
                        last = (resp.status, text)
                except Exception as e:
                    last = (None, f"{type(e).__name__}: {e}")
                if attempt < 3:
                    await asyncio.sleep(5 * attempt)
            return last

        # Test 2: Earthdata Cloud OPeNDAP actual data download (not catalog)
        # GES DISC retired its on-premises OPeNDAP (HTTP 410, seen 2026-11-13).
        # Cloud OPeNDAP answers 403 for a bad/expired Bearer token and 302 with none,
        # so a 200 with ASCII data proves the token works for SO2.
        print("\n--- Test 2: Cloud OPeNDAP Data Download (Bearer) ---")
        if token:
            test_url = (
                    "https://opendap.earthdata.nasa.gov/collections/C3685693295-GES_DISC/granules/OMSO2G.004%3AOMI-Aura_L2G-OMSO2G_2024m0101_v004-2025m0904t091514.he5.ascii"
                    "?/HDFEOS/GRIDS/OMI_Total_Column_Amount_SO2/Data_Fields/ColumnAmountSO2_PBL[0:0][500:500][1250:1250]"
            )
            status, body = await _get_with_retry(
                test_url, headers={"Authorization": f"Bearer {token}"}, allow_redirects=False,
            )
            print(f"HTTP {status}")
            if status == 200 and body.startswith("Dataset:") and "ColumnAmountSO2_PBL" in body:
                print("PASS: Got ASCII data via cloud OPeNDAP")
                print(f"Preview: {body[:150]}")
                token_ok = True
            elif status in (302, 401, 403):
                print("FAIL: token rejected or expired (403) / not applied (302)")
                token_ok = False
            elif status == 404:
                print("WARN: test granule not found (reprocessed?) -- inconclusive, not an auth failure")
            else:
                print(f"WARN: inconclusive ({status}): {body[:200]}")
        else:
            print("SKIP: EARTHDATA_TOKEN not set")
            token_ok = False

        # Test 3: URS token list (Basic auth = the account password)
        # The token API does not accept a Bearer token (always 401); it needs Basic auth.
        print("\n--- Test 3: URS Token API (Basic auth) ---")
        status, body = await _get_with_retry(
            "https://urs.earthdata.nasa.gov/api/users/tokens", auth=auth,
        )
        print(f"HTTP {status}")
        try:
            data = json.loads(body) if status == 200 else None
        except ValueError:
            data = None
        if status == 200 and not isinstance(data, list):
            print(f"WARN: inconclusive (200 but not a token list): {(body or '')[:200]}")
        elif status == 200:
            password_ok = True
            print(f"PASS: password accepted, account has {len(data)} token(s)")
            mine = [t for t in data if token and isinstance(t, dict) and t.get("access_token") == token]
            if mine:
                print(f"PASS: EARTHDATA_TOKEN is a live token, expires {mine[0].get('expiration_date', 'unknown')}")
            elif token:
                print("WARN: EARTHDATA_TOKEN is not among the account's live tokens (expired or replaced)")
                if token_ok is None:
                    token_ok = False
        elif status in (401, 403):
            print("FAIL: password rejected by the token API")
            password_ok = False
        else:
            print(f"WARN: inconclusive ({status}): {(body or '')[:200]}")

        if token_ok is False:
            print("::warning::EARTHDATA_TOKEN does not work (expired, replaced or unset). "
                  "SO2 (cloud OPeNDAP) needs the token and will stop; password-based "
                  "fetchers still run. Renew at https://urs.earthdata.nasa.gov/profile")
        if password_ok is False:
            print("::warning::EARTHDATA_PASSWORD was rejected by Earthdata Login")
        if token_ok is False and password_ok is False:
            print("FAIL: neither the token nor the password works")
            ok = False
        elif token_ok is None and password_ok is None:
            print("FAIL: auth could not be confirmed either way (both tests inconclusive)")
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
