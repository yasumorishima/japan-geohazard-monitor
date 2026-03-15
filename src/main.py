"""Entry point: run all collectors and web server concurrently."""

import asyncio
import logging

import aiohttp
import uvicorn

from collectors.amedas import AMeDASCollector
from collectors.earthquake_jma import JMACollector
from collectors.earthquake_p2p import P2PQuakeCollector
from collectors.earthquake_usgs import USGSCollector
from collectors.geomag import GeomagCollector
from collectors.geonet import GEONETCollector
from collectors.sst import SSTCollector
from collectors.tec import TECCollector
from collectors.volcano import VolcanoCollector
from db import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def run_server():
    config = uvicorn.Config("api:app", host="0.0.0.0", port=8003, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    await init_db()

    collectors = [
        USGSCollector(),
        P2PQuakeCollector(),
        JMACollector(),
        AMeDASCollector(),
        GeomagCollector(),
        VolcanoCollector(),
        SSTCollector(),
        TECCollector(),
        GEONETCollector(),
    ]

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            *[c.run(session) for c in collectors],
            run_server(),
        )


if __name__ == "__main__":
    asyncio.run(main())
