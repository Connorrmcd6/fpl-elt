import os
import time
import fpl_client
import loader
import clickhouse_connect


def _get_client_with_retry(retries: int = 12, delay: float = 1.0):
    last_exc = None
    for _ in range(retries):
        try:
            return clickhouse_connect.get_client(
                host=os.getenv("CLICKHOUSE_HOST", "localhost"),
                port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
                username=os.getenv("CLICKHOUSE_USER", "default"),
                database=os.getenv("CLICKHOUSE_DATABASE", "raw"),
                secure=os.getenv("CLICKHOUSE_SECURE", "false").lower()
                in ("1", "true", "yes"),
            )
        except Exception as e:
            last_exc = e
            time.sleep(delay)
    raise last_exc


def main():
    client = fpl_client.FPLClient()

    bootstrap_keys = [
        "chips",
        "elements",
        "element_stats",
        "element_types",
        "events",
        "teams",
    ]
    bootstrap_data = client.get_bootstrap_components(bootstrap_keys)

    fixtures = client.get_fixtures()

    ch = _get_client_with_retry()

    # Load data using the results
    loader.load_raw(bootstrap_data.get("chips"), ch, "chips", id_field="id")
    loader.load_raw(bootstrap_data.get("elements"), ch, "players", id_field="id")
    loader.load_raw(
        bootstrap_data.get("element_stats"), ch, "player_stats", id_field="id"
    )
    loader.load_raw(
        bootstrap_data.get("element_types"), ch, "player_types", id_field="id"
    )
    loader.load_raw(bootstrap_data.get("events"), ch, "gameweeks", id_field="id")
    loader.load_raw(bootstrap_data.get("teams"), ch, "teams", id_field="id")
    loader.load_raw(fixtures, ch, "fixtures", id_field="id")


if __name__ == "__main__":
    main()
