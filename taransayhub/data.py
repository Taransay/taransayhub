"""Data decoding and parsing."""

from datetime import datetime
import struct
from collections import defaultdict
import logging
import requests

LOGGER = logging.getLogger(__name__)

# Ignored messages.
STATUS_MESSAGES = ["Resetting RF"]


def parse_data(data, nodes):
    data = data.strip()

    if data in STATUS_MESSAGES:
        return

    pieces = data.split()

    if not pieces:
        return

    if pieces[0] == "?":
        raise ValueError("unreliable content")
    elif pieces[0] != "OK":
        raise ValueError("unexpected packet format")

    pieces = pieces[1:]

    if not pieces:
        # The data was just an OK...
        return

    # Remove RSSI.
    if pieces[-1].startswith("(") and pieces[-1].endswith(")"):
        pieces = pieces[:-1]

    if not pieces:
        # The data was just an OK and RSSI value...
        return

    node = int(pieces[0])

    try:
        node_data = nodes[node]
    except KeyError:
        raise ValueError(f"unrecognised node: {node}")

    # Discard if anything non-numerical is found.
    try:
        raw_data = [int(piece) for piece in pieces[1:]]
    except ValueError as e:
        e.message = f"cannot handle non-float data ({e})"
        raise e

    datacodes = node_data["datacodes"]
    datasizes = [check_datacode(code) for code in datacodes]

    if len(raw_data) != sum(datasizes):
        raise ValueError(
            f"Received data length {len(raw_data)} is not valid for datacodes {datacodes}"
        )

    decoded_data = []
    byte_position = 0

    for datacode in datacodes:
        # Determine the number of bytes to use for each value by its datacode.
        size = int(check_datacode(datacode))
        # Decode the data.
        value = decode(
            datacode, [int(v) for v in raw_data[byte_position : byte_position + size]]
        )
        byte_position += size
        decoded_data.append(value)

    scales = node_data["scales"]
    scaled_data = [value * scale for value, scale in zip(decoded_data, scales)]
    rounded_data = [
        round(value, precision)
        for value, precision in zip(scaled_data, node_data["rounding"])
    ]

    return {
        "group": node_data["group"],
        "device": node_data["device"],
        "data": rounded_data,
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


def ingest_data(data, queue, nodes):
    try:
        parsed_data = parse_data(data, nodes)
    except Exception as e:
        LOGGER.debug(f"Skipped invalid data packet '{data}': {e}")
    else:
        if parsed_data is None:
            # Non-data packet.
            LOGGER.debug(f"Skipped non-data packet '{data}'")
        else:
            queue.append((datetime.now(), parsed_data))


def get_data_payload(items):
    payloads = defaultdict(lambda: defaultdict(list))

    for data_time, parsed_data in items:
        row = [str(data_time), parsed_data["data"]]
        payloads[parsed_data["group"]][parsed_data["device"]].append(row)

    return {"sent": str(datetime.now()), "data": payloads}


def send_queue_data(queue, url):
    LOGGER.debug(f"Posting payload to {url}")

    with QueueItemTransaction(queue) as items:
        LOGGER.debug(f"Sending request with {len(items)} item(s)")
        payload = get_data_payload(items)
        response = requests.post(url, json=payload)
        LOGGER.debug(f"Got response: {response}")


class QueueItemTransaction:
    def __init__(self, queue):
        self._queue = queue
        self._transaction_items = []

    def __enter__(self):
        while True:
            try:
                self._transaction_items.append(self._queue.popleft())
            except IndexError:
                # Queue is empty.
                break

        return self._transaction_items

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            # Nothing went wrong.
            return

        LOGGER.warning(f"Queue item transaction did not succeed; restoring items")

        # Restore the items.
        for item in reversed(self._transaction_items):
            self._queue.appendleft(item)

        LOGGER.info(f"Put {len(self._transaction_items)} item(s) back into queue")
