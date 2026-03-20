"""Quick Earthdata credential validation test.

Tests:
1. URS profile API with Basic auth (username/password)
2. OPeNDAP redirect flow (GES DISC SO2 catalog)
3. Bearer token validation

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
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(cookie_jar=jar) as session:
        auth = aiohttp.BasicAuth(username, password)

        # Test 1: URS profile API
        print("\n--- Test 1: URS Basic Auth ---")
        try:
            async with session.get(
                f"https://urs.earthdata.nasa.gov/api/users/{username}",
                auth=auth, timeout=timeout,
            ) as resp:
                print(f"HTTP {resp.status}")
                if resp.status == 200:
                    data = await resp.json()
                    print(f"User: {data.get('uid', '?')}")
                    print("PASS: Credentials valid")
                elif resp.status == 401:
                    body = (await resp.text())[:500]
                    print(f"FAIL: Invalid username or password")
                    print(f"Response headers: {dict(resp.headers)}")
                    print(f"Response body: {body}")
                    ok = False
                else:
                    print(f"UNEXPECTED: {(await resp.text())[:200]}")
                    ok = False
        except Exception as e:
            print(f"ERROR: {e}")
            ok = False

        # Test 2: OPeNDAP redirect flow
        print("\n--- Test 2: OPeNDAP Redirect Flow (GES DISC) ---")
        test_url = "https://measures.gesdisc.eosdis.nasa.gov/opendap/SO2/OMSO2e.003/"
        try:
            async with session.get(test_url, allow_redirects=False, timeout=timeout) as resp:
                print(f"Initial request: HTTP {resp.status}")
                if resp.status in (301, 302):
                    redirect_url = resp.headers.get("Location", "")
                    print(f"Redirect to URS: {'urs.earthdata' in redirect_url}")

                    async with session.get(
                        redirect_url, auth=auth,
                        allow_redirects=True, timeout=timeout,
                    ) as auth_resp:
                        ct = auth_resp.headers.get("Content-Type", "")
                        body = (await auth_resp.text())[:500]
                        print(f"After auth: HTTP {auth_resp.status} CT={ct[:40]}")
                        if auth_resp.status == 200 and "html" in ct.lower():
                            if "login" in body.lower() or "<form" in body.lower():
                                print("FAIL: Still on login page")
                                ok = False
                            else:
                                print("PASS: OPeNDAP catalog accessible")
                                # Show first 200 chars
                                print(f"Preview: {body[:200]}")
                        elif auth_resp.status == 200:
                            print("PASS: Data accessible")
                        else:
                            print(f"FAIL: HTTP {auth_resp.status}")
                            print(f"Body: {body[:200]}")
                            ok = False
                elif resp.status == 200:
                    print("PASS: Direct access (no redirect)")
                else:
                    print(f"UNEXPECTED: HTTP {resp.status}")
        except Exception as e:
            print(f"ERROR: {e}")
            ok = False

        # Test 3: Bearer token validation
        if token:
            print("\n--- Test 3: Bearer Token Validation ---")
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
                            print(f"Token expiration: {exp}")
                        print("PASS: Bearer token valid")
                    elif resp.status == 401:
                        print("FAIL: Bearer token invalid/expired")
                        ok = False
                    else:
                        body = (await resp.text())[:200]
                        print(f"UNEXPECTED: {body}")
            except Exception as e:
                print(f"ERROR: {e}")
        else:
            print("\n--- Test 3: Bearer Token Validation ---")
            print("SKIP: EARTHDATA_TOKEN not set")

        # Test 4: CoastWatch ERDDAP (no auth, just verify connectivity)
        print("\n--- Test 4: CoastWatch ERDDAP (no auth) ---")
        try:
            erddap_url = (
                "https://coastwatch.pfeg.noaa.gov/erddap/griddap/erdMH1chlamday.csv"
                "?chlorophyll[(2024-01-16T00:00:00Z)][(35.0):(35.5)][(140.0):(140.5)]"
            )
            async with session.get(erddap_url, timeout=timeout) as resp:
                print(f"HTTP {resp.status}")
                if resp.status == 200:
                    text = (await resp.text())[:300]
                    print(f"PASS: {text[:150]}")
                else:
                    print(f"FAIL: {(await resp.text())[:200]}")
        except Exception as e:
            print(f"ERROR: {e}")

    print(f"\n{'='*40}")
    print(f"Result: {'ALL PASS' if ok else 'SOME FAILED'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
