"""Hub for data forwarding."""

import sys
import signal
import logging
from collections import defaultdict
from pathlib import Path
from functools import partial
import asyncio
from enum import auto, Enum
import json
import yaml
import serial_asyncio
import paho.mqtt.publish as mqtt_publish
from .data import ingest_data

LOGGER = logging.getLogger(__name__)


class TaransayTopicType(Enum):
    STATE = auto()
    CONFIG = auto()


class TaransayHub:
    """Taransay forwarder.

    This opens a serial connection to the attached Taransay Base board and asynchronously
    fills a queue with received data. Another asynchronous job periodically empties the
    queue and sends it to the configured remote server.
    """

    def __init__(self, config_file):
        self.device_path = None
        self.baud_rate = None
        self.nodes = None
        self.discovery_prefix = None

        self._config_file = Path(config_file)
        self._load_config()
        self._configured_topics = defaultdict(list)

    def _load_config(self):
        LOGGER.debug(f"Loading configuration from {self._config_file}")

        with self._config_file.open("r") as fobj:
            config_data = yaml.safe_load(fobj)

        self.device_path = Path(config_data["device_path"])
        self.baud_rate = int(config_data["baud_rate"])
        self.discovery_prefix = config_data["discovery_prefix"]
        self.nodes = config_data["nodes"]

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

        LOGGER.info("Publishing autodiscover topics")
        self._publish_autodiscover()

        # Create the serial reader stream.
        reader, _ = await serial_asyncio.open_serial_connection(
            url=str(self.device_path), baudrate=self.baud_rate
        )
        LOGGER.info(f"Reader created at {self.device_path} @ {self.baud_rate} byte/s")

        # Create the received message handler.
        received = self._reciever(reader)

        # Wait until the callables finish, or an exception is thrown.
        finished, pending = await asyncio.wait(
            [received], return_when=asyncio.FIRST_EXCEPTION
        )

        # Report any thrown exceptions.
        for task in finished:
            if task.exception():
                LOGGER.error(f"{task} got an exception: {task.exception()}")

        LOGGER.info("Finished")

    def _handle_exit(self, signame, loop):
        """Handle a shutdown.

        https://docs.python.org/3.8/library/asyncio-eventloop.html#set-signal-handlers-for-sigint-and-sigterm
        """
        LOGGER.info(f"Received {signame} signal.")

        LOGGER.info("Unpublishing autodiscover topics")
        self._unpublish_autodiscover()

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

        LOGGER.info("Republishing autodiscover topics")
        self._publish_autodiscover()

    async def _reciever(self, reader):
        while True:
            raw_msg = await reader.readline()

            try:
                msg = raw_msg.strip().decode()
            except UnicodeDecodeError as e:
                LOGGER.error(f"Error decoding message: {e}")
            else:
                LOGGER.debug(f"Received message '{msg}'.")
                self._publish_recv_msg_to_mqtt(msg)

    def _publish_autodiscover(self):
        """Publish MQTT discovery topics for the nodes registered in the configuration."""
        def _spec_payload(
            device_unique_id,
            device_class,
            device_spec,
            channel_unique_id,
            channel_name,
            description,
            unit,
            expire_after
        ):
            payload = {
                "unique_id": channel_unique_id,
                "name": description,
                "device_class": device_class,
                # See https://developers.home-assistant.io/docs/device_registry_index/.
                "device": device_spec,
                "state_class": "measurement",
                "state_topic": self._state_topic(device_unique_id),
                "unit_of_measurement": unit,
                "value_template": f"{{{{ value_json.data.{channel_name} }}}}",
                "expire_after": expire_after,
            }

            msg = {
                "topic": self._config_topic(channel_unique_id),
                "payload": json.dumps(payload),
                "retain": True,
            }

            return msg

        for node, nodeconfig in self.nodes.items():
            msgs = []
            device_name = nodeconfig["name"]
            device_unique_id = self._device_unique_id(device_name)

            # See https://developers.home-assistant.io/docs/device_registry_index/.
            device_spec = {
                "identifiers": [f"taransay-{node}"],
                "manufacturer": "Sean Leavey",
                "model": nodeconfig["board_model"],
                "name": nodeconfig["description"],
                "sw_version": nodeconfig["firmware_version"],
            }

            # Set up channels in config.
            for channel_name, channel_data in nodeconfig["channels"].items():
                if not channel_data["enabled"]:
                    LOGGER.info(f"skipping ignored channel {repr(channel_name)} for node {node}")
                    continue

                channel_unique_id = self._channel_unique_id(device_name, channel_name)
                msg = _spec_payload(
                    device_unique_id=device_unique_id,
                    device_class=channel_data["class"],
                    device_spec=device_spec,
                    channel_unique_id=channel_unique_id,
                    channel_name=channel_name,
                    description=channel_data["description"],
                    unit=channel_data["unit"],
                    expire_after=nodeconfig["expire_after"]
                )
                msgs.append(msg)
                self._configured_topics[node].append(msg["topic"])

            # Set up RSSI.
            if nodeconfig["rssi"]:
                channel_name = "rssi"
                channel_unique_id = self._channel_unique_id(device_name, channel_name)
                msg = _spec_payload(
                    device_unique_id=device_unique_id,
                    device_class="signal_strength",
                    device_spec=device_spec,
                    channel_unique_id=channel_unique_id,
                    channel_name=channel_name,
                    description=nodeconfig["rssi_description"],
                    unit="dBm",
                    expire_after=nodeconfig["expire_after"]
                )
                msgs.append(msg)
                self._configured_topics[node].append(msg["topic"])

            self._do_publish_multiple_mqtt(msgs, node)

    def _unpublish_autodiscover(self):
        """Unpublish MQTT discovery topics for the nodes registered in the configuration."""
        while self._configured_topics:
            node, topics = self._configured_topics.popitem()
            msgs = [{"topic": topic, "payload": ""} for topic in topics]
            self._do_publish_multiple_mqtt(msgs, node)

    def _publish_recv_msg_to_mqtt(self, msg):
        data = ingest_data(msg, self.nodes)

        if data is None:
            LOGGER.info("Skipped invalid message.")
            return

        tick, parsed_data = data
        payload = {"datetime": str(tick), **parsed_data}
        unique_id = self._device_unique_id(parsed_data["device_name"])
        self._do_publish_single_mqtt(
            self._state_topic(unique_id),
            json.dumps(payload),
            parsed_data["node"],
        )

    def _do_publish_single_mqtt(self, topic, payload, node, retain=True, **kwargs):
        self._do_publish_multiple_mqtt(
            [{"topic": topic, "payload": payload, "retain": retain}], node, **kwargs
        )

    def _do_publish_multiple_mqtt(self, msgs, node, **kwargs):
        for msg in msgs:
            LOGGER.debug(f"publishing message {msg}")

        mqtt_publish.multiple(msgs, client_id=f"taransay-{node}", **kwargs)

    def _device_unique_id(self, device_name):
        return f"taransay-{device_name}"

    def _channel_unique_id(self, device_name, channel_name):
        return f"{self._device_unique_id(device_name)}-{channel_name}"

    def _state_topic(self, unique_id):
        return self._topic(unique_id, type_=TaransayTopicType.STATE)

    def _config_topic(self, unique_id):
        return self._topic(unique_id, type_=TaransayTopicType.CONFIG)

    def _topic(self, object_id, type_):
        pieces = [self.discovery_prefix, "sensor", object_id]

        if type_ is TaransayTopicType.STATE:
            last = "state"
        elif type_ is TaransayTopicType.CONFIG:
            last = "config"
        else:
            raise ValueError(f"unknown topic type {repr(type_)}")

        pieces.append(last)

        return "/".join(pieces)
