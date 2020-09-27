"""Hub for data forwarding."""

import sys
import signal
from pathlib import Path
import pickle
from collections import deque
import asyncio
from functools import partial
import logging
import yaml
import serial_asyncio
from .data import ingest_data, send_queue_data

LOGGER = logging.getLogger(__name__)


class TaransayHub:
    """Taransay forwarder.

    This opens a serial connection to the attached Taransay Base board and asynchronously
    fills a queue with received data. Another asynchronous job periodically empties the
    queue and sends it to the configured remote server.
    """

    def __init__(self, config_file):
        self._config_file = Path(config_file)
        self.nodes = None
        self.device_path = None
        self.backup_path = None
        self.baud_rate = None
        self.post_url = None
        self.queue = None

        self._load_config()
        self._load_queue()

    def _load_config(self):
        LOGGER.debug(f"Loading configuration from {self._config_file}")

        with self._config_file.open("r") as fobj:
            config_data = yaml.safe_load(fobj)

        self.nodes = config_data["nodes"]
        self.device_path = Path(config_data["device_path"])
        self.backup_path = Path(config_data["backup_path"])
        self.baud_rate = int(config_data["baud_rate"])
        self.post_url = str(config_data["post_url"])
        self.post_period = int(config_data["post_period"])
        self.post_max_fail_backoff = int(config_data["post_max_fail_backoff"])
        self.post_data_compress = bool(config_data["post_data_compress"])

    async def main(self):
        LOGGER.info(f"Starting {self.__class__.__name__}")

        loop = asyncio.get_running_loop()

        # Set up kill signal handlers.
        for signame in ("SIGINT", "SIGTERM"):
            loop.add_signal_handler(
                getattr(signal, signame), partial(self._handle_exit, signame, loop)
            )

        # Set up config reload handler.
        loop.add_signal_handler(
            signal.SIGHUP, partial(self._handle_reload, signame, loop)
        )

        # Create the serial reader stream.
        reader, _ = await serial_asyncio.open_serial_connection(
            url=str(self.device_path), baudrate=self.baud_rate
        )
        LOGGER.info(f"Reader created at {self.device_path} @ {self.baud_rate} byte/s")

        # Create the received message handler.
        received = self._reciever(reader)
        # Create the queue worker.
        worked = self._worker()

        # Wait until the callables finish, or an exception is thrown.
        finished, pending = await asyncio.wait(
            [received, worked], return_when=asyncio.FIRST_EXCEPTION
        )

        # Report any thrown exceptions.
        for task in finished:
            if task.exception():
                LOGGER.error(f"{task} got an exception: {task.exception()}")

        self._backup_queue()

        LOGGER.info("Finished")

    def _handle_exit(self, signame, loop):
        """Handle a shutdown.

        https://docs.python.org/3.8/library/asyncio-eventloop.html#set-signal-handlers-for-sigint-and-sigterm
        """
        LOGGER.info(f"Received {signame} signal")
        self._backup_queue()

        LOGGER.debug("Stopping event loop")
        loop.stop()

        LOGGER.info("Exiting")
        sys.exit(0)

    def _handle_reload(self, signame, loop):
        """Handle a configuration reload.

        Note: if this interrupts a routine that is currently reading the configuration values, it could get
        a mix of old and new configuration values.
        """
        LOGGER.info(f"Received {signame} signal")
        LOGGER.debug(
            "Note: device path and baud rate cannot be altered without restart"
        )
        self._load_config()

    async def _reciever(self, reader):
        while True:
            raw_msg = await reader.readline()
            msg = raw_msg.strip().decode()
            LOGGER.debug(f"Received message '{msg}'.")
            ingest_data(msg, self.queue, self.nodes)

    async def _worker(self):
        """Post data to remote server.

        Uses exponential back-off on failures.
        """
        nfails = 0

        while True:
            # Wait for a while.
            delay = 2 ** min(nfails, self.post_max_fail_backoff) * self.post_period
            LOGGER.debug(f"Worker sleeping for {delay} s")
            await asyncio.sleep(delay)

            try:
                send_queue_data(
                    self.queue, self.post_url, compress=self.post_data_compress
                )
            except Exception as e:
                nfails += 1
                LOGGER.error(f"Could not send data (subsequent failure {nfails}): {e}")
            else:
                # Reset the number of subsequent failures.
                nfails = 0

            LOGGER.debug("Finished processing messages")

    def _load_queue(self):
        if self.backup_path.is_file():
            LOGGER.info(f"Using saved backup at {self.backup_path}")

            with self.backup_path.open("rb") as fobj:
                self.queue = pickle.load(fobj)

            LOGGER.debug(f"Loaded {len(self.queue)} saved item(s)")
        else:
            # No backup file available.
            self.queue = deque([])

    def _backup_queue(self):
        if not self.queue:
            # Empty queue.
            return

        # Create the path if not present.
        self.backup_path.parent.mkdir(exist_ok=True)

        LOGGER.info(
            f"Dumping queue with {len(self.queue)} item(s) to {self.backup_path}"
        )
        with self.backup_path.open("wb") as fobj:
            pickle.dump(self.queue, fobj)
