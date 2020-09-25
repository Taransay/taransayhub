import sys
import signal
from pathlib import Path
import pickle
from datetime import datetime
from collections import deque
import asyncio
from functools import partial
import logging
import serial_asyncio
from .data import ingest_data, send_queue_data

LOGGER = logging.getLogger(__name__)


class TaransayHub:
    def __init__(self, nodes, device_path, baud_rate, post_url, backup_dir):
        self.nodes = nodes
        self.device_path = Path(device_path)
        self.backup_dir = Path(backup_dir)
        self.baud_rate = int(baud_rate)
        self.post_url = str(post_url)
        self.queue = None

        self._load_queue()

    async def main(self, delay):
        LOGGER.info(f"Starting {self.__class__.__name__} with delay = {delay} s")

        # Set up signal handlers.
        loop = asyncio.get_running_loop()
        for signame in ("SIGINT", "SIGTERM"):
            loop.add_signal_handler(
                getattr(signal, signame), partial(self._handle_exit, signame, loop)
            )

        # Create the serial reader stream.
        reader, _ = await serial_asyncio.open_serial_connection(
            url=str(self.device_path), baudrate=self.baud_rate
        )
        LOGGER.info(f"Reader created at {self.device_path} @ {self.baud_rate} byte/s")

        # Create the received message handler.
        received = self._reciever(reader)
        # Create the queue worker.
        worked = self._worker(delay)

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

    async def _reciever(self, reader):
        while True:
            raw_msg = await reader.readline()
            msg = raw_msg.strip().decode()
            LOGGER.debug(f"Received message '{msg}'.")
            ingest_data(msg, self.queue, self.nodes)

    async def _worker(self, delay):
        while True:
            # Wait for a while.
            LOGGER.debug(f"Worker sleeping for {delay} s")
            await asyncio.sleep(delay)

            try:
                send_queue_data(self.queue, self.post_url)
            except Exception as e:
                LOGGER.error(f"Could not send data: {e}")

            LOGGER.debug("Finished processing messages")

    def _load_queue(self):
        try:
            # Get the latest backup.
            backup_path = next(iter(sorted(self.backup_dir.iterdir(), reverse=True)))
        except (FileNotFoundError, StopIteration):
            # No backup.
            self.queue = deque([])
        else:
            # Return the backed up queue.
            LOGGER.info(f"Using saved backup at {backup_path}")
            with backup_path.open("rb") as fobj:
                self.queue = pickle.load(fobj)
            LOGGER.debug(f"Loaded {len(self.queue)} saved item(s)")

    def _backup_queue(self):
        if not self.queue:
            # Empty queue.
            return

        self.backup_dir.mkdir(exist_ok=True)

        now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        queue_dump_path = self.backup_dir / f"{self.__class__.__name__}-dump-{now}"
        LOGGER.info(
            f"Dumping queue with {len(self.queue)} item(s) to {queue_dump_path}"
        )
        with queue_dump_path.open("wb") as fobj:
            pickle.dump(self.queue, fobj)
