import abc
import asyncio
import json
import logging

LOGGER = logging.getLogger(__name__)


class Device:
    def __init__(
        self,
        name,
        node,
        discovery_prefix,
        description,
        board,
        board_version,
        firmware,
        firmware_version,
        serial_write,
        publish,
        subscribe,
        unsubscribe,
        filter
    ):
        self.name = name
        self.node = node
        self.discovery_prefix = discovery_prefix
        self.description = description
        self.board = board
        self.board_version = board_version
        self.firmware = firmware
        self.firmware_version = firmware_version
        self.serial_write = serial_write
        self.publish = publish
        self.subscribe = subscribe
        self.unsubscribe = unsubscribe
        self.filter = filter

    @property
    def device_spec(self):
        # See https://developers.home-assistant.io/docs/device_registry_index/.
        return {
            "identifiers": [f"taransay-{self.node}"],
            "manufacturer": "Sean Leavey",
            "model": self.board,
            "name": self.description,
            "sw_version": f"{self.firmware} {self.firmware_version}",
            #"hw_version": f"{self.board} {self.board_version}"
        }

    @property
    def unique_id(self):
        return f"taransay-{self.name}"

    @abc.abstractmethod
    def mqtt_filter(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def mqtt_subscribe(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def mqtt_publish_discovery(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def mqtt_unconfigure(self, client):
        raise NotImplementedError

    @abc.abstractmethod
    async def handle_filtered_messages(self, messages):
        raise NotImplementedError

    @abc.abstractmethod
    async def handle_device_payload(self, payload):
        raise NotImplementedError


class RGBDevice(Device):
    def mqtt_filter(self):
        return self.filter(self.command_topic)

    async def mqtt_subscribe(self):
        await self.subscribe(self.command_topic)

    @property
    def state_topic(self):
        pieces = [self.discovery_prefix, self.unique_id, "state"]
        return "/".join(pieces)

    @property
    def command_topic(self):
        pieces = [self.discovery_prefix, self.unique_id, "set"]
        return "/".join(pieces)

    @property
    def config_topic(self):
        pieces = [self.discovery_prefix, "light", self.unique_id, "config"]
        return "/".join(pieces)

    async def mqtt_publish_discovery(self):
        payload = json.dumps(
            {
                "unique_id": self.unique_id,
                "name": self.description,
                # See https://developers.home-assistant.io/docs/device_registry_index/.
                "device": self.device_spec,
                "schema": "json",
                "state_topic": self.state_topic,
                "command_topic": self.command_topic,
                "brightness": True,
                "color_mode": True,
                "supported_color_modes": ["rgb"]
            }
        )

        await self.publish(self.config_topic, payload=payload, retain=True)

    async def mqtt_unconfigure(self, client):
        await self.unsubscribe(self.command_topic)
        await self.publish(self.state_topic, payload="")
        await self.publish(self.config_topic, payload="")

    async def handle_filtered_messages(self, messages):
        async for message in messages:
            command_struct = json.loads(message.payload.decode())
            state = command_struct.get("state")

            LOGGER.debug(f"mqtt payload: {repr(command_struct)}")

            payload = f"STATE:{state.upper()}"

            if "color" in command_struct:
                color = command_struct["color"]
                payload = f"{payload}:RGB:{color['r']},{color['g']},{color['b']}"

            if "brightness" in command_struct:
                brightness = command_struct["brightness"]
                payload = f"{payload}:W:{brightness}"

            await self.serial_write(payload)

    async def handle_device_payload(self, payload):
        message = payload["msg"]
        pieces = message.split(":")
        payload = {}

        invalid_msg = f"Invalid payload for message received to {self}: {repr(message)}"

        if pieces[0] != "STATE" or len(pieces) < 2:
            LOGGER.error(invalid_msg)
            return

        payload["state"] = pieces[1]

        if len(pieces) > 2:
            if len(pieces) != 4 or pieces[2] != "RGBW":
                LOGGER.error(invalid_msg)
                return
            rgbw = pieces[3].split(",")
            payload["color"] = {
                "r": int(rgbw[0]),
                "g": int(rgbw[1]),
                "b": int(rgbw[2]),
            }
            payload["color_mode"] = "rgb"
            payload["brightness"] = int(rgbw[3])

        await self.publish(self.state_topic, payload=json.dumps(payload))


class RGBWDevice(Device):
    def mqtt_filter(self):
        return self.filter(self.command_topic)

    async def mqtt_subscribe(self):
        await self.subscribe(self.command_topic)

    @property
    def state_topic(self):
        pieces = [self.discovery_prefix, self.unique_id, "state"]
        return "/".join(pieces)

    @property
    def command_topic(self):
        pieces = [self.discovery_prefix, self.unique_id, "set"]
        return "/".join(pieces)

    @property
    def config_topic(self):
        pieces = [self.discovery_prefix, "light", self.unique_id, "config"]
        return "/".join(pieces)

    async def mqtt_publish_discovery(self):
        payload = json.dumps(
            {
                "unique_id": self.unique_id,
                "name": self.description,
                # See https://developers.home-assistant.io/docs/device_registry_index/.
                "device": self.device_spec,
                "schema": "json",
                "state_topic": self.state_topic,
                "command_topic": self.command_topic,
                "color_mode": True,
                "supported_color_modes": ["rgbw"],
            }
        )

        await self.publish(self.config_topic, payload=payload, retain=True)

    async def mqtt_unconfigure(self, client):
        await self.unsubscribe(self.command_topic)
        await self.publish(self.state_topic, payload="")
        await self.publish(self.config_topic, payload="")

    async def handle_filtered_messages(self, messages):
        async for message in messages:
            command_struct = json.loads(message.payload.decode())
            state = command_struct.get("state")

            LOGGER.debug(f"mqtt payload: {repr(command_struct)}")

            payload = f"STATE:{state.upper()}"

            if "color" in command_struct:
                color = command_struct["color"]
                payload = f"{payload}:RGBW:{color['r']},{color['g']},{color['b']},{color['w']}"

            await self.serial_write(payload)

    async def handle_device_payload(self, payload):
        message = payload["msg"]
        pieces = message.split(":")
        payload = {}

        invalid_msg = f"Invalid payload for message received to {self}: {repr(message)}"

        if pieces[0] != "STATE" or len(pieces) < 2:
            LOGGER.error(invalid_msg)
            return

        payload["state"] = pieces[1]

        if len(pieces) > 2:
            if pieces[2] == "RGBW":
                rgbw = pieces[3].split(",")

                if len(rgbw) != 4:
                    LOGGER.error(invalid_msg)
                    return

                payload["color"] = {
                    "r": int(rgbw[0]),
                    "g": int(rgbw[1]),
                    "b": int(rgbw[2]),
                    "w": int(rgbw[3])
                }
                payload["color_mode"] = "rgbw"
                payload["effect"] = "off"

        await self.publish(self.state_topic, payload=json.dumps(payload))


class SwitchDevice(Device):
    def mqtt_filter(self):
        return self.filter(self.command_topic)

    async def mqtt_subscribe(self):
        await self.subscribe(self.command_topic)

    @property
    def state_topic(self):
        pieces = [self.discovery_prefix, self.unique_id, "state"]
        return "/".join(pieces)

    @property
    def command_topic(self):
        pieces = [self.discovery_prefix, self.unique_id, "set"]
        return "/".join(pieces)

    @property
    def config_topic(self):
        pieces = [self.discovery_prefix, "switch", self.unique_id, "config"]
        return "/".join(pieces)

    async def mqtt_publish_discovery(self):
        payload = json.dumps(
            {
                "unique_id": self.unique_id,
                "name": self.description,
                # See https://developers.home-assistant.io/docs/device_registry_index/.
                "device": self.device_spec,
                "state_topic": self.state_topic,
                "command_topic": self.command_topic,
                "payload_on": "ON",
                "payload_off": "OFF",
                "state_on": "ON",
                "state_off": "OFF",
                "value_template": "{{ value_json.SW1 }}",
                "icon": "mdi:lightbulb"
            }
        )

        await self.publish(self.config_topic, payload=payload, retain=True)

    async def mqtt_unconfigure(self, client):
        await self.unsubscribe(self.command_topic)
        await self.publish(self.state_topic, payload="")
        await self.publish(self.config_topic, payload="")

    async def handle_filtered_messages(self, messages):
        async for message in messages:
            state = message.payload.decode()
            LOGGER.debug(f"mqtt payload: {repr(state)}")

            # STATE:<n>:<state>
            payload = f"STATE:1:{state}"

            await self.serial_write(payload)

    async def handle_device_payload(self, payload):
        message = payload["msg"]
        pieces = message.split(":")
        payload = {}

        invalid_msg = f"Invalid payload for message received to {self}: {repr(message)}"

        # Should be STATE:1:<ON/OFF>:2:<ON/OFF>
        if pieces[0] != "STATE" or len(pieces) != 5:
            LOGGER.error(invalid_msg)
            return

        payload["SW1"] = pieces[2]
        payload["SW2"] = pieces[4]

        await self.publish(self.state_topic, payload=json.dumps(payload))


class TemperatureHumidityDevice(Device):
    def __init__(self, *, channels, expire_after=300, rssi=False, rssi_description=None, **kwargs):
        self.channels = dict(channels)
        self.expire_after = int(expire_after)
        self.rssi = rssi
        self.rssi_description = rssi_description
        self.__config_topics = []
        super().__init__(**kwargs)

    def mqtt_filter(self):
        return self.filter(self.command_topic)

    async def mqtt_subscribe(self):
        await self.subscribe(self.command_topic)

    @property
    def state_topic(self):
        pieces = [self.discovery_prefix, self.unique_id, "state"]
        return "/".join(pieces)

    @property
    def command_topic(self):
        pieces = [self.discovery_prefix, self.unique_id, "set"]
        return "/".join(pieces)

    async def mqtt_publish_discovery(self):
        sensor_config = []
        topic_pieces = [self.discovery_prefix, "sensor"]

        channels = dict(self.channels)

        if self.rssi:
            # Configure an RSSI channel.
            channels["rssi"] = {
                "enabled": True,
                "description": self.rssi_description,
                "class": "signal_strength",
                "unit": "dBm",
            }

        for channel_name, channel_data in channels.items():
            if not channel_data.get("enabled"):
                LOGGER.info(f"skipping ignored channel {repr(channel_name)} for node {self.node}")
                continue

            channel_unique_id = f"{self.unique_id}-{channel_name}"
            pieces = topic_pieces + [channel_unique_id, "config"]
            config_topic = "/".join(pieces)
            self.__config_topics.append(config_topic)

            payload = json.dumps(
                {
                    "unique_id": channel_unique_id,
                    "name": channel_data["description"],
                    "device": self.device_spec,
                    "device_class": channel_data["class"],
                    "state_class": "measurement",
                    "state_topic": self.state_topic,
                    "unit_of_measurement": channel_data["unit"],
                    "value_template": f"{{{{ value_json.data.{channel_name} }}}}",
                    "expire_after": self.expire_after,
                }
            )

            await self.publish(config_topic, payload=payload, retain=True)

    async def mqtt_unconfigure(self, client):
        await self.unsubscribe(self.command_topic)
        await self.publish(self.state_topic, payload="")

        for config_topic in self.__config_topics:
            await self.publish(config_topic, payload="")

    async def handle_filtered_messages(self, messages):
        async for message in messages:
            state = message.payload.decode()
            LOGGER.debug(f"mqtt payload: {repr(state)}")
            # Do nothing.
            LOGGER.debug("(doing nothing)")

    async def handle_device_payload(self, payload):
        message = payload["msg"]
        rssi = payload.get("rssi")
        pieces = message.split(":")
        payload = {}

        invalid_msg = f"Invalid payload for message received to {self}: {repr(message)}"

        # Should be STATE:V:<VOLTAGE>:T1:<TEMPERATURE>:T2:<EXTERNAL_TEMPERATURE>:H:<HUMIDITY>
        if pieces[0] != "STATE" or len(pieces) != 9:
            LOGGER.error(invalid_msg)
            return

        def prepare(raw, channel):
            scale = float(self.channels[channel]["scale"])
            precision = self.channels[channel]["precision"]
            return f"{float(raw) * scale:.{precision}f}"

        payload["data"] = {
            "battery_voltage": prepare(pieces[2], "battery_voltage"),
            "temperature": prepare(pieces[4], "temperature"),
            "ext_temperature": prepare(pieces[6], "ext_temperature"),
            "humidity": prepare(pieces[8], "humidity"),
            "rssi": rssi
        }

        await self.publish(self.state_topic, payload=json.dumps(payload))


class CurrentTransducerDevice(Device):
    def __init__(self, *, channels, expire_after=60, rssi=False, rssi_description=None, **kwargs):
        self.channels = dict(channels)
        self.expire_after = int(expire_after)
        self.rssi = rssi
        self.rssi_description = rssi_description
        self.__config_topics = []
        super().__init__(**kwargs)

    def mqtt_filter(self):
        return self.filter(self.command_topic)

    async def mqtt_subscribe(self):
        await self.subscribe(self.command_topic)

    @property
    def state_topic(self):
        pieces = [self.discovery_prefix, self.unique_id, "state"]
        return "/".join(pieces)

    @property
    def command_topic(self):
        pieces = [self.discovery_prefix, self.unique_id, "set"]
        return "/".join(pieces)

    async def mqtt_publish_discovery(self):
        sensor_config = []
        topic_pieces = [self.discovery_prefix, "sensor"]

        channels = dict(self.channels)

        if self.rssi:
            # Configure an RSSI channel.
            channels["rssi"] = {
                "enabled": True,
                "description": self.rssi_description,
                "class": "signal_strength",
                "unit": "dBm",
            }

        for channel_name, channel_data in channels.items():
            if not channel_data.get("enabled"):
                LOGGER.info(f"skipping ignored channel {repr(channel_name)} for node {self.node}")
                continue

            channel_unique_id = f"{self.unique_id}-{channel_name}"
            pieces = topic_pieces + [channel_unique_id, "config"]
            config_topic = "/".join(pieces)
            self.__config_topics.append(config_topic)

            payload = json.dumps(
                {
                    "unique_id": channel_unique_id,
                    "name": channel_data["description"],
                    "device": self.device_spec,
                    "device_class": channel_data["class"],
                    "state_class": "measurement",
                    "state_topic": self.state_topic,
                    "unit_of_measurement": channel_data["unit"],
                    "value_template": f"{{{{ value_json.data.{channel_name} }}}}",
                    "expire_after": self.expire_after,
                }
            )

            await self.publish(config_topic, payload=payload, retain=True)

    async def mqtt_unconfigure(self, client):
        await self.unsubscribe(self.command_topic)
        await self.publish(self.state_topic, payload="")

        for config_topic in self.__config_topics:
            await self.publish(config_topic, payload="")

    async def handle_filtered_messages(self, messages):
        async for message in messages:
            state = message.payload.decode()
            LOGGER.debug(f"mqtt payload: {repr(state)}")
            # Do nothing.
            LOGGER.debug("(doing nothing)")

    async def handle_device_payload(self, payload):
        message = payload["msg"]
        rssi = payload.get("rssi")
        pieces = message.split(":")
        payload = {}

        invalid_msg = f"Invalid payload for message received to {self}: {repr(message)}"

        # Should be STATE:V:<VOLTAGE>:P:<POWER>:T1:<TEMPERATURE>:T2:<EXTERNAL_TEMPERATURE>:H:<HUMIDITY>
        if pieces[0] != "STATE" or len(pieces) != 11:
            LOGGER.error(invalid_msg)
            return

        def prepare(raw, channel):
            scale = float(self.channels[channel]["scale"])
            precision = self.channels[channel]["precision"]
            return f"{float(raw) * scale:.{precision}f}"

        payload["data"] = {
            "battery_voltage": prepare(pieces[2], "battery_voltage"),
            "power": prepare(pieces[4], "power"),
            "temperature": prepare(pieces[6], "temperature"),
            "ext_temperature": prepare(pieces[8], "ext_temperature"),
            "humidity": prepare(pieces[10], "humidity"),
            "rssi": rssi
        }

        await self.publish(self.state_topic, payload=json.dumps(payload))
