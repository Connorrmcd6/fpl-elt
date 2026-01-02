import json
from typing import Any, Dict, Iterable, List
import hashlib


def _iter_items(data: Any, id_field: str) -> Iterable[Dict]:
    if isinstance(data, dict) and "elements" in data:
        return data["elements"]
    if isinstance(data, dict) and "events" in data and id_field == "id":
        return data.get("events") or data.get("teams") or [data]
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def load_raw(data: Any, client: Any, table: str, id_field: str = "id") -> None:
    """
    Load FPL JSON objects into raw.<table>.
    Each row inserted: (id UInt64, raw_data String).
    Uses clickhouse-connect client's insert method.
    """
    items = list(_iter_items(data, id_field))
    if not items:
        return

    rows: List[tuple] = []
    for item in items:
        if not isinstance(item, dict):
            continue

        raw_json = json.dumps(item, separators=(",", ":"))

        if id_field in item:
            try:
                row_id = int(item[id_field])
            except (ValueError, TypeError):
                row_id = int(
                    hashlib.sha256(str(item[id_field]).encode()).hexdigest(), 16
                ) % (10**18)
        else:
            row_id = int(hashlib.sha256(raw_json.encode()).hexdigest(), 16) % (10**18)

        rows.append((row_id, raw_json))

    if not rows:
        return

    client.insert(f"raw.{table}", rows, column_names=["id", "raw_data"])
