#!/bin/bash
set -e

clickhouse-client --multiquery --user "${CLICKHOUSE_USER:-default}" <<'EOSQL'
CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.chips
(
    id UInt64,
    raw_data String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (id, loaded_at);

CREATE TABLE IF NOT EXISTS raw.players
(
    id UInt64,
    raw_data String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (id, loaded_at);

CREATE TABLE IF NOT EXISTS raw.player_stats
(
    id UInt64,
    raw_data String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (id, loaded_at);

CREATE TABLE IF NOT EXISTS raw.player_types
(
    id UInt64,
    raw_data String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (id, loaded_at);

CREATE TABLE IF NOT EXISTS raw.fixtures
(
    id UInt64,
    raw_data String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (id, loaded_at);

CREATE TABLE IF NOT EXISTS raw.gameweeks
(
    id UInt64,
    raw_data String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (id, loaded_at);

CREATE TABLE IF NOT EXISTS raw.teams
(
    id UInt64,
    raw_data String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (id, loaded_at);
EOSQL