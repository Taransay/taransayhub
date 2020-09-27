import sys
import logging
import asyncio
import click
from . import __version__, PROGRAM
from .hub import TaransayHub


@click.command()
@click.option(
    "--config-file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    required=True,
    help="Path to configuration file (YAML formatted) with configuration.",
)
@click.option("-v", "--verbose", count=True, default=0, help="Increase verbosity.")
@click.version_option(version=__version__, prog_name=PROGRAM)
def hub(config_file, verbose):
    # Ensure verbosity is between DEBUG and WARNING.
    verbosity = max(logging.WARNING - verbose * 10, logging.DEBUG)

    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        level=verbosity,
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    hub = TaransayHub(config_file)
    asyncio.run(hub.main())


if __name__ == "__main__":
    hub()
