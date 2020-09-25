import sys
import logging
import asyncio
from .hub import TaransayHub

if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        level=logging.DEBUG,
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    hub = TaransayHub()
    asyncio.run(hub.main(delay=60))
