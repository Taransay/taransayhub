"""Data decoding and parsing."""

from datetime import datetime
import struct
import logging

LOGGER = logging.getLogger(__name__)


def parse_data(data, nodes):
    data = data.strip()

    if data.startswith("?"):
        raise ValueError("unreliable content")
    elif data.startswith(";"):
        # Data is a comment.
        comment = data.lstrip("; ")
        LOGGER.info(f"[device] {comment}")
        return

    pieces = data.split()

    if not pieces:
        return

    if pieces[0] != "OK":
        raise ValueError("unexpected packet format")

    pieces = pieces[1:]

    if not pieces:
        # The data was just an OK...
        return

    # Report and remove received signal strength indicator (RSSI).
    if pieces[-1].startswith("(") and pieces[-1].endswith(")"):
        rssi = pieces[-1].lstrip("(").rstrip(")")
        LOGGER.debug(f"RSSI: {rssi} dB")
        pieces = pieces[:-1]

    if not pieces:
        # The data was just an OK and RSSI value...
        return

    node = int(pieces[0])

    try:
        node_data = nodes[node]
    except KeyError:
        raise ValueError(
            f"unrecognised node: {node} (ensure you have properly configured new nodes)"
        )

    # Discard if anything non-numerical is found.
    try:
        raw_data = [int(piece) for piece in pieces[1:]]
    except ValueError as e:
        e.message = f"cannot handle non-float data ({e})"
        raise e

    channel_spec = node_data["channels"]
    datacodes = [channel["code"] for channel in channel_spec.values()]
    datasizes = [check_datacode(code) for code in datacodes]

    if len(raw_data) != sum(datasizes):
        raise ValueError(
            f"Received data length {len(raw_data)} is not valid for datacodes {datacodes}"
        )

    decoded_data = {"rssi": rssi}
    byte_position = 0

    for (channel, channel_spec), datacode in zip(channel_spec.items(), datacodes):
        # Determine the number of bytes to use for each value by its datacode.
        size = int(check_datacode(datacode))
        # Decode the data.
        value = decode(
            datacode, [int(v) for v in raw_data[byte_position : byte_position + size]]
        )
        byte_position += size
        value = round(value * channel_spec["scale"], channel_spec["precision"])

        if not channel_spec["enabled"]:
            LOGGER.debug(
                f"skipping disabled channel {repr(channel)} with value {repr(value)}"
            )
            continue

        decoded_data[channel] = value

    return {
        "node": node,
        "device_name": node_data["name"],
        "device_type": node_data["device_type"],
        "data": decoded_data,
    }


def check_datacode(datacode):
    try:
        return struct.calcsize(str(datacode))
    except struct.error:
        return False


def decode(datacode, frame):
    # Ensure little-endian and standard sizes used.
    e = "<"

    # Set the base data type to bytes.
    b = "B"

    # Get data size from data code.
    s = int(check_datacode(datacode))

    result = struct.unpack(e + datacode[0], struct.pack(e + b * s, *frame))

    return result[0]


def ingest_data(data, nodes):
    try:
        parsed_data = parse_data(data, nodes)
    except Exception as e:
        LOGGER.debug(f"Skipped invalid data packet '{data}' ({type(e)}): {e}")
    else:
        if parsed_data is None:
            # Non-data packet.
            LOGGER.debug(f"Skipped non-data packet '{data}'")
        else:
            return datetime.utcnow(), parsed_data
