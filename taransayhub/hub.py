"""Hub for data forwarding."""

import sys
import signal
import logging
from collections import defaultdict
from contextlib import AsyncExitStack
from pathlib import Path
from functools import partial
import asyncio
from enum import auto, Enum
import json
import yaml
import serial_asyncio
import paho.mqtt.publish as mqtt_publish
import paho.mqtt.subscribe as mqtt_subscribe
from asyncio_mqtt import Client
from . import devices
from .data import parse_payload, TaransayDataType

LOGGER = logging.getLogger(__name__)


class TaransayTopicType(Enum):
    STATE = auto()
    COMMAND = auto()
    CONFIG = auto()


class TaransayHub:
    """Taransay forwarder.

    This opens a serial connection to the attached Taransay Base board and asynchronously
    fills a queue with received data. Another asynchronous job periodically empties the
    queue and sends it to the configured remote server.
    """

    DEVICES = {
        "rgb": devices.RGBDevice,
        "rgbw": devices.RGBWDevice,
        "switch": devices.SwitchDevice,
        "th": devices.TemperatureHumidityDevice,
        "ct": devices.CurrentTransducerDevice,
    }

    def __init__(self, config_file):
        self.device_path = None
        self.baud_rate = None
        self.nodes = None
        self.discovery_prefix = None
        self.client_id = None
        self.request_acknowledge = None

        self._config_file = Path(config_file)
        self._load_config()

        # State stuff.
        self._client = None
        self._tasks = None
        self._devices = None  # node -> Device
        self._serial_writer = None

    def _load_config(self):
        LOGGER.debug(f"Loading configuration from {self._config_file}")

        with self._config_file.open("r") as fobj:
            config_data = yaml.safe_load(fobj)

        self.client_id = config_data["client_id"]
        self.request_acknowledge = config_data["request_acknowledge"]
        self.device_path = Path(config_data["device_path"])
        self.baud_rate = int(config_data["baud_rate"])
        self.discovery_prefix = config_data["discovery_prefix"]
        self.nodes = config_data["nodes"]

    async def main(self):
        LOGGER.info(f"Starting {self.__class__.__name__}")

        self._tasks = set()
        self._devices = {}

        # Create the serial reader stream.
        serial_reader, self._serial_writer = await serial_asyncio.open_serial_connection(
            url=str(self.device_path), baudrate=self.baud_rate
        )
        LOGGER.info(f"Serial reader created at {self.device_path} @ {self.baud_rate} byte/s")

        LOGGER.info("Configuring devices")
        self._configure_devices()

        # Connect to the MQTT broker
        self._client = Client("localhost", client_id=self.client_id)

        async with AsyncExitStack() as stack:
            await stack.enter_async_context(self._client)

            # Keep track of the asyncio tasks that we create, so that we can cancel them on exit.
            stack.push_async_callback(self._cancel, self._client)

            for device in self._devices.values():
                manager = device.mqtt_filter()
                messages = await stack.enter_async_context(manager)
                self._tasks.add(asyncio.create_task(device.handle_filtered_messages(messages)))

                # Note that we subscribe *after* starting the message loggers. Otherwise, we may
                # miss retained messages.
                await device.mqtt_subscribe()
                await device.mqtt_publish_discovery()

            # Messages that doesn't match a filter will get logged here
            #messages = await stack.enter_async_context(client.unfiltered_messages())
            #task = asyncio.create_task(self._log_messages(messages, "[unfiltered] {}"))
            #self._tasks.add(task)

            # Setup serial message handler.
            task = asyncio.create_task(self._serial_receiver(serial_reader))
            self._tasks.add(task)

            # Wait until the callables finish, or an exception is thrown.
            finished, pending = await asyncio.wait(self._tasks, return_when=asyncio.FIRST_EXCEPTION)

            # Report any thrown exceptions.
            for task in finished:
                if task.exception():
                    LOGGER.error(f"{task} got an exception: {task.exception()}")

        self._client = None
        LOGGER.info("Finished")

    async def _log_messages(self, messages, template):
        async for message in messages:
            # 🤔 Note that we assume that the message payload is an
            # UTF8-encoded string (hence the `bytes.decode` call).
            print(template.format(message.payload.decode()))

    async def _cancel(self, client):
        for task in self._tasks:
            if task.done():
                continue

            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass

        LOGGER.info("Unconfiguring devices")
        for device in self._devices.values():
            await device.mqtt_unconfigure(client)

    async def _serial_receiver(self, serial_reader):
        while True:
            raw_msg = await serial_reader.readline()

            try:
                msg = raw_msg.strip().decode()
            except UnicodeDecodeError as e:
                LOGGER.error(f"Error decoding message: {e}")
                LOGGER.debug(f"original message: {repr(raw_msg.strip())})")
            else:
                await self._handle_received_serial(msg)

    def _configure_devices(self):
        """Configure devices."""
        for node, config in self.nodes.items():
            devicetype = self.DEVICES[config.pop("device_type")]
            name = config.pop("name")

            device = devicetype(
                name=name,
                node=node,
                discovery_prefix=self.discovery_prefix,
                serial_write=partial(self._serial_write, node=node),
                publish=partial(self._mqtt_device_publish, node=node),
                subscribe=partial(self._mqtt_device_subscribe, node=node),
                unsubscribe=partial(self._mqtt_device_unsubscribe, node=node),
                filter=partial(self._mqtt_device_filter, node=node),
                **config
            )
            self._devices[node] = device

    async def _serial_write(self, message, node):
        tx = "TXA" if self.request_acknowledge else "TX"
        payload = f"{tx}:{node}:{message}\n"
        LOGGER.info(f"Sending payload: {payload[:-1]}")
        return self._serial_writer.write(payload.encode("ascii"))

    async def _mqtt_device_publish(self, *args, node, **kwargs):
        LOGGER.debug(f"MQTT device publish args={args} node={node} kwargs={kwargs}")
        await self._client.publish(*args, **kwargs)

    async def _mqtt_device_subscribe(self, *args, node, **kwargs):
        LOGGER.debug(f"MQTT device subscribe args={args} node={node} kwargs={kwargs}")
        await self._client.subscribe(*args, **kwargs)

    async def _mqtt_device_unsubscribe(self, *args, node, **kwargs):
        LOGGER.debug(f"MQTT device unsubscribe args={args} node={node} kwargs={kwargs}")
        await self._client.unsubscribe(*args, **kwargs)

    def _mqtt_device_filter(self, *args, node, **kwargs):
        LOGGER.debug(f"MQTT device filter args={args} node={node} kwargs={kwargs}")
        return self._client.filtered_messages(*args, **kwargs)

    async def _handle_received_serial(self, msg):
        msg_type, msg_data = parse_payload(msg)

        if msg_type is TaransayDataType.COMMENT:
            LOGGER.debug(f"Comment from base: {msg_data}")
        elif msg_type is TaransayDataType.PROMPT:
            LOGGER.debug(f"Prompt from base: {msg_data}")
        elif msg_type is TaransayDataType.ERROR:
            LOGGER.error(f"Error from base: {msg_data}")
        elif msg_type is TaransayDataType.UNRECOGNISED:
            LOGGER.warning(f"Unrecognised message from base: {msg_data}")
        else:
            ack = "acknowledged" if msg_data.get("rssi") else "not acknowledged"
            LOGGER.debug(f"Payload via base: {repr(msg_data)} ({ack})")

            node = int(msg_data["node"])
            device = self._devices.get(node)

            if device is None:
                LOGGER.error(f"Sending device {repr(node)} not configured")
            else:
                await device.handle_device_payload(msg_data)
