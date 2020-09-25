import sys
import logging
import asyncio
import click
import yaml
from . import __version__, PROGRAM
from .hub import TaransayHub


def CommandWithConfigFile(config_file_param_name):
    """Load click with pre-populated values from a config file.

    https://stackoverflow.com/questions/46358797/python-click-supply-arguments-and-options-from-a-configuration-file
    """

    class CustomCommandClass(click.Command):
        def invoke(self, ctx):
            config_file = ctx.params[config_file_param_name]

            with open(config_file) as f:
                config_data = yaml.safe_load(f)

                for param, value in ctx.params.items():
                    if value is None and param in config_data:
                        ctx.params[param] = config_data[param]

            # Add parsed config file to call.
            ctx.params["parsed_config_data"] = config_data

            return super().invoke(ctx)

    return CustomCommandClass


@click.command(cls=CommandWithConfigFile("config_file"))
@click.option("-d", "--device-path", type=click.Path(), help="Path to serial device.")
@click.option(
    "-b", "--baud-rate", type=click.IntRange(1), help="Serial device baud rate."
)
@click.option("--post-url", type=str, help="URL to post data to.")
@click.option("--backup-dir", type=click.Path(file_okay=False, writable=True))
@click.option(
    "--config-file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    required=True,
    help=(
        "Path to configuration file (YAML formatted) with node configuration. Other keys "
        "in this file can be used to pre-populate the other options in this call."
    ),
)
@click.option("-v", "--verbose", count=True, default=0, help="Increase verbosity.")
@click.version_option(version=__version__, prog_name=PROGRAM)
def hub(
    device_path,
    baud_rate,
    post_url,
    backup_dir,
    config_file,
    parsed_config_data,
    verbose,
):
    # Ensure verbosity is between DEBUG and WARNING.
    verbosity = max(logging.WARNING - verbose * 10, logging.DEBUG)

    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        level=verbosity,
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    # Extract node configuration.
    nodes = parsed_config_data["nodes"]

    hub = TaransayHub(nodes, device_path, baud_rate, post_url, backup_dir)
    asyncio.run(hub.main(delay=60))


if __name__ == "__main__":
    hub()
