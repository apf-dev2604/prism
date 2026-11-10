"""Startup validation for configs, adapters, and required runtime settings."""

import os
from typing import Any, Iterable, Optional

from .constants import REQUIRED_SOURCE_TABLE_KEYS


REQUIRED_CONFIG_ATTRS = (
    "BRAND_KEY",
    "BRAND",
    "PLATFORM",
    "WALLET_PLATFORM",
    "SOURCE_DB_NAME",
    "TARGET_DB_NAME",
    "SOURCE_SCHEMA",
    "TARGET_SCHEMA",
    "SOURCE_MODE",
    "SOURCE_TABLES",
    "PLAYER_TABLE",
    "GAME_TRANSACTION_TABLE",
    "WALLET_TRANSACTION_TABLE",
    "CHECKPOINT_TABLE",
    "BUSINESS_WINDOW_START_HOUR",
)

REQUIRED_ADAPTER_METHODS = (
    "as_dict",
    "source_date_expr",
    "source_created_value",
    "fetch_player_detail_map",
    "map_player",
    "map_game_transaction",
    "map_wallet",
)


def _missing_attrs(obj: Any, names: Iterable[str]) -> list[str]:
    return [name for name in names if not hasattr(obj, name) or getattr(obj, name) in (None, "")]


def validate_brand_config(config: Any) -> None:
    missing = _missing_attrs(config, REQUIRED_CONFIG_ATTRS)
    if missing:
        raise RuntimeError("Brand config is missing required setting(s): " + ", ".join(missing))

    missing_table_keys = [key for key in REQUIRED_SOURCE_TABLE_KEYS if key not in config.SOURCE_TABLES]
    if missing_table_keys:
        raise RuntimeError(
            f"Brand config {config.BRAND_KEY!r} SOURCE_TABLES missing key(s): "
            + ", ".join(missing_table_keys)
        )

    if config.SOURCE_MODE not in ("table_batch", "member_driven", "flat_table_batch"):
        raise RuntimeError(
            f"Unsupported SOURCE_MODE={config.SOURCE_MODE!r}; expected 'table_batch', 'member_driven', or 'flat_table_batch'."
        )


def validate_adapter(adapter: Any, source_mode: str) -> None:
    missing = _missing_attrs(adapter, REQUIRED_ADAPTER_METHODS)
    if source_mode == "member_driven":
        for name in ("fetch_member_game_rows", "fetch_member_wallet_rows"):
            if not hasattr(adapter, name):
                missing.append(name)
    if source_mode == "flat_table_batch":
        for name in ("fetch_player_rows", "fetch_game_rows", "fetch_wallet_rows"):
            if not hasattr(adapter, name):
                missing.append(name)
    if missing:
        raise RuntimeError("Brand adapter is missing required method(s): " + ", ".join(missing))


def validate_db_env(prefix: Optional[str] = None) -> None:
    """Validate the PostgreSQL environment variables used by helpers/db.py.

    Password values are never printed.
    """

    required = (
        "DB_SETTINGS_PIPELINE_DB_HOST",
        "DB_SETTINGS_PIPELINE_PIPELINE_USER",
        "ETL_DB_USER_PWD",
        "DB_SETTINGS_PIPELINE_DB_PORT",
    )

    missing = [
        name for name in required
        if not os.getenv(name)
    ]

    if missing:
        raise RuntimeError(
            "Missing required database environment variable(s): "
            + ", ".join(missing)
        )

    port_value = os.environ["DB_SETTINGS_PIPELINE_DB_PORT"]

    try:
        port = int(port_value)
    except ValueError:
        raise RuntimeError(
            "DB_SETTINGS_PIPELINE_DB_PORT must be a valid integer."
        ) from None

    if not 1 <= port <= 65535:
        raise RuntimeError(
            "DB_SETTINGS_PIPELINE_DB_PORT must be between 1 and 65535."
        )
