import json
from pathlib import Path


class Parameters:
    FQDN = "FQDN"
    USER_SCHEMA_STR = json.dumps(
        json.loads(Path("json-shema.json").read_text(encoding="utf-8")),
        separators=(",", ":"),  # компактный, детерминированный вид
    )

class Messages:
    message1 = {
        "id": 1,
        "name": "First user",
        "favoriteNumber": 42,
        "favoriteColor": "blue",
    }

    message2 = {
        "id": 2,
        "name": "Second user",
        "favoriteNumber": 2,
        "favoriteColor": "red",
    }

    message3= {
        "id": 3,
        "name": "Third user",
        "favoriteNumber": 3,
        "favoriteColor": "blue",
    }

    message4 = {
        "id": 4,
        "name": "Four user",
        "favoriteNumber": 4,
        "favoriteColor": "green",
    }

    message5 = {
        "id": 5,
        "name": "Five user",
        "favoriteNumber": 5,
        "favoriteColor": "grey",
    }

    message6 = {
        "id": 6,
        "name": "Six user",
        "favoriteNumber": 6,
        "favoriteColor": "grey",
    }
