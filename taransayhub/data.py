"""Data decoding and parsing."""

import re
from enum import auto, Enum

data_pattern = re.compile(
    r"receive \[from (?P<node>\d+)\] (?P<msg>.*) \[RSSI (?P<rssi>-?\d+)\]( (?P<ack>\[ACK\]))?"
)


class TaransayDataType(Enum):
    COMMENT = auto()
    PROMPT = auto()
    ERROR = auto()
    DATA = auto()
    UNRECOGNISED = auto()


def parse_payload(payload):
    payload = payload.strip()

    if payload.startswith(";"):
        # Data is a comment.
        comment = payload.lstrip("; ")
        return TaransayDataType.COMMENT, comment
    elif payload.startswith(">"):
        # Data is a prompt.
        prompt = payload.lstrip("> ")
        return TaransayDataType.PROMPT, prompt
    elif payload.startswith("!"):
        # Data is a prompt.
        prompt = payload.lstrip("! ")
        return TaransayDataType.ERROR, prompt

    data = data_pattern.match(payload)

    if data is None:
        return TaransayDataType.UNRECOGNISED, payload

    return TaransayDataType.DATA, data.groupdict()
