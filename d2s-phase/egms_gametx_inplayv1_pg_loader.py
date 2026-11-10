import argparse
# ADDED - Loader-to-DQ asynchronous handoff and SMTP notifications.
import subprocess
import smtplib
import stat
from email.message import EmailMessage
import csv
import gzip
import io
import json
import logging
import os
import sys
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Tuple, Dict, List

import psycopg2
from psycopg2 import sql

# SQL text moved into phase-local query module; original execution functions remain.
from query import egms_loader_queries as loader_queries

# Shared terminal/log presentation (SMTP import retained below).
sys.path.insert(0, os.environ["SHARED_MODS"])
from etl_report import configure_logging, print_summary, log_exception

# =============================================================================
# CHANGE VARIABLES HERE
# =============================================================================

DB_HOST = os.environ["DB_SETTINGS_PIPELINE_DB_HOST"]
DB_PORT = int(os.environ["DB_SETTINGS_PIPELINE_DB_PORT"])
ETL_USER = os.environ["DB_SETTINGS_PIPELINE_PIPELINE_USER"]
ETL_DB_USER_PWD = os.getenv("ETL_DB_USER_PWD", "")  # UPDATED - password from exported .env
DB_NAME = os.environ["DB_SETTINGS_DW_RAW_DB_NAME"]
DB_SCHEMA = os.environ["DB_SETTINGS_DW_RAW_SCHEMA_NAME"]
DB_TABLENAME = os.environ["DB_SETTINGS_DW_RAW_DB_TABLENAME"]
# Source extract location from the MySQL extractor.
# Expected file:
#   /home/allanf/scripts/artem/out/egms_games_txn_yyyymmdd/egms_games_txn_yyyymmdd.jsonl.gz
#SOURCE_BASE_DIR = Path("/home/allanf/scripts/artem/out")
SOURCE_BASE_DIR = Path(os.environ["DMP_DIR"])  # YYYYMM extracted directories
FILE_PREFIX = os.environ["DB_SETTINGS_INPLAY1_FILE_PREFIX"]

# Final PostgreSQL destination is intentionally NOT defaulted.
# When --final is used, you must provide --to-schema and --to-table.

# Historical/replay table. Used after a successful --final insert.
# This table must already exist. The loader will not create it.
HISTORY_SCHEMA = os.environ["DB_SETTINGS_DW_RAW_SCHEMA_NAME"]
HISTORY_TABLE = "temp_egms_games_txn_history"

# Transient load table. This table is truncated before every load.
#TEMP_SCHEMA = os.environ["DB_SETTINGS_DW_RAW_SCHEMA_NAME"]
TEMP_TABLE = os.environ["DB_SETTINGS_DW_RAW_TEMP_TABLE"]

# Local logs.
BASE_DIR = Path(os.environ["DIRECT2SILVER_PHASE"])  # Current project phase
LOG_DIR = Path(os.environ["BASE_LOG_DIR"])  # Configured log root

# Skip output directory.
# The loader writes skipped-existing-final records here.
LOAD_SKIP_OUT_DIR = BASE_DIR / "load_skip_out"

# COPY buffer flush size. This is memory control only, not a row limit.
COPY_FLUSH_ROWS = 10000
PROGRESS_EVERY_ROWS = int(os.environ["DB_SETTINGS_INPLAY1_PROGRESS_LOG_EVERY_ROWS"])

APP_NAME = "egms_games_txn_pg_loader"

# ADDED - Optional independent loader mode.
# False = After a successful --final load and closed PG connection, hand off to DQ.
# True  = Run the loader independently; skip only the DQ handoff.
#         PostgreSQL load validations and failure-only SMTP alerts remain active.
#SKIP_DQ = False
SKIP_DQ = os.environ.get("SKIP_DQ", "false").lower() == "true"

# =============================================================================
# HELPERS
# =============================================================================

def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def seconds_to_hhmmss(seconds: float) -> str:
    total = int(round(seconds))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def setup_logging(log_dir: Path, load_date: str = None) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{APP_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{load_date}.log"

    configure_logging(log_file)
    return log_file


def build_file_from_date(date_value: str) -> Path:
    if not (len(date_value) == 8 and date_value.isdigit()):
        raise ValueError("--date must be in yyyymmdd format. Example: 20241107")

    # UPDATED - Monthly YYYYMM directory with timestamped archive filenames.
    # The automated handoff uses --file (exact path); this is manual --date fallback only.
    month_dir = SOURCE_BASE_DIR / date_value[:6]
    candidates = sorted(month_dir.glob(f"{FILE_PREFIX}_{date_value}_exractdt_*.jsonl.gz"))
    if len(candidates) != 1:
        raise RuntimeError(
            f"Expected exactly one file for {date_value} in {month_dir}, "
            f"found {len(candidates)}. Use --file with the exact archive path."
        )
    return candidates[0]


def get_load_date_label(args, gz_file: Path) -> str:
    """Return yyyymmdd label for output files."""
    if args.date:
        if not (len(args.date) == 8 and args.date.isdigit()):
            raise ValueError("--date must be in yyyymmdd format. Example: 20241107")
        return args.date

    # Fallback for --file usage. Try to get yyyymmdd from filename/folder.
    name_parts = [gz_file.name, gz_file.parent.name]
    for name in name_parts:
        for part in name.replace(".", "_").replace("-", "_").split("_"):
            if len(part) == 8 and part.isdigit():
                return part

    return datetime.now().strftime("%Y%m%d")


def make_skip_output_paths(args, gz_file: Path) -> Dict[str, Path]:
    date_label = get_load_date_label(args, gz_file)
    LOAD_SKIP_OUT_DIR.mkdir(parents=True, exist_ok=True)
    return {
        "final_existing": LOAD_SKIP_OUT_DIR / f"{FILE_PREFIX}_{date_label}_skipped_existing_in_final.jsonl.gz",
    }


def pg_connect(args):
    return psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        user=args.pg_user,
        password=args.pg_password,
        dbname=args.pg_database,
        connect_timeout=15,
        sslmode="require"
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load EGMS JSONL.GZ extract into PostgreSQL transient table, optionally to a selected final table."
    )

    parser.add_argument(
        "--date",
        help="Extract date in yyyymmdd format. Example: 20241107. Used to locate the source file.",
    )
    parser.add_argument(
        "--file",
        help="Optional explicit path to egms_games_txn_yyyymmdd.jsonl.gz. Overrides --date path.",
    )

    parser.add_argument("--pg-host", default=DB_HOST)
    parser.add_argument("--pg-port", type=int, default=DB_PORT)
    parser.add_argument("--pg-user", default=ETL_USER)
    parser.add_argument("--pg-password", default=ETL_USER)
    parser.add_argument("--pg-database", default=DB_NAME)
    parser.add_argument("--temp-schema", default=DB_SCHEMA)
    parser.add_argument("--temp-table", default=TEMP_TABLE)

    parser.add_argument(
        "--to-final", "--final",
        dest="to_final",
        action="store_true",
        help="After temp load validation PASS, insert temp rows into the selected destination table.",
    )
    parser.add_argument(
        "--to-schema", "--target-schema", "--final-schema",
        dest="target_schema",
        default=None,
        help="Destination/final schema. Required when --final is supplied.",
    )
    parser.add_argument(
        "--to-table", "--target-table", "--final-table",
        dest="target_table",
        default=None,
        help="Destination/final table. Required when --final is supplied.",
    )

    parser.add_argument(
        "--mode",
        choices=["insert-only", "upsert"],
        default="insert-only",
        help="Only used with --final. insert-only skips duplicate ids; upsert updates changed rows. Default: insert-only.",
    )

    parser.add_argument(
        "--history-schema",
        default=HISTORY_SCHEMA,
        help="Historical/replay schema. Used after a successful --final insert. Table must already exist.",
    )
    parser.add_argument(
        "--history-table",
        default=HISTORY_TABLE,
        help="Historical/replay table. Used after a successful --final insert. Table must already exist.",
    )
    parser.add_argument(
        "--skip-history",
        action="store_true",
        help="With --final, skip copying temp rows to the historical/replay table.",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only connect, resolve/check table names, required columns, and privileges. Do not read file, truncate, copy, or insert.",
    )

    args = parser.parse_args()

    if not args.file and not args.date:
        parser.error("Provide --date yyyymmdd or --file /path/to/file.jsonl.gz")

    if args.to_final and (not args.target_schema or not args.target_table):
        parser.error("--final requires both --to-schema and --to-table")

    return args


def is_permission_denied_error(exc: Exception) -> bool:
    pgcode = getattr(exc, "pgcode", None)
    msg = str(exc).lower()
    return pgcode == "42501" or "permission denied" in msg or "insufficient privilege" in msg


def log_permission_context(args, stage_name: str, schema_name: str, table_name: str, exc: Exception) -> None:
    logging.error("Permission or privilege error detected.")
    logging.error("Failed stage : %s", stage_name)
    logging.error("Database     : %s", args.pg_database)
    logging.error("Schema       : %s", schema_name or "N/A")
    logging.error("Table        : %s", table_name or "N/A")
    logging.error("User         : %s", args.pg_user)
    logging.error("Host         : %s:%s", args.pg_host, args.pg_port)
    logging.error("Error        : %s", exc)


def print_permission_context(args, stage_name: str, schema_name: str, table_name: str, exc: Exception) -> None:
    print("permission_error         : YES")
    print(f"failed_stage             : {stage_name}")
    print(f"database                 : {args.pg_database}")
    print(f"schema                   : {schema_name or 'N/A'}")
    print(f"table_name               : {table_name or 'N/A'}")
    print(f"pg_user                  : {args.pg_user}")
    print(f"pg_host                  : {args.pg_host}:{args.pg_port}")
    print(f"permission_error_message : {exc}")


# =============================================================================
# VALIDATION QUERIES
# =============================================================================

def strip_pg_quotes(name: str) -> str:
    """Accept GameTx or "GameTx" from CLI and return the raw PostgreSQL name."""
    value = str(name).strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1].replace('""', '"')
    return value


def pg_qualified_display(schema_name: str, table_name: str) -> str:
    """Display PostgreSQL object names safely, preserving mixed-case table names."""
    def q(name: str) -> str:
        return '"' + str(name).replace('"', '""') + '"'

    return f"{q(schema_name)}.{q(table_name)}"


def resolve_table_name(conn, schema_name: str, table_name: str) -> Tuple[str, str, str]:
    """
    Resolve the real PostgreSQL table name before any load action.

    Order:
      1. Exact schema + exact table match.
      2. Exact schema + case-insensitive table match, only if unique.
      3. Case-insensitive schema + case-insensitive table match, only if unique.

    This uses pg_catalog metadata only. It does not scan user data.
    """
    requested_schema = strip_pg_quotes(schema_name)
    requested_table = strip_pg_quotes(table_name)

    with conn.cursor() as cur:
        # Exact match first. This correctly finds public."GameTx" when requested table is GameTx.
        cur.execute(
            loader_queries.SQL_01_RESOLVE_TABLE_NAME,
            (requested_schema, requested_table),
        )
        row = cur.fetchone()
        if row:
            return row[0], row[1], "exact"

        # Same schema, case-insensitive table match.
        cur.execute(
            loader_queries.SQL_02_RESOLVE_TABLE_NAME,
            (requested_schema, requested_table),
        )
        rows = cur.fetchall()
        if len(rows) == 1:
            return rows[0][0], rows[0][1], "case-insensitive table match"
        if len(rows) > 1:
            options = ", ".join(pg_qualified_display(r[0], r[1]) for r in rows)
            raise RuntimeError(
                f"Ambiguous table name {pg_qualified_display(requested_schema, requested_table)}. "
                f"Multiple case-insensitive matches found: {options}"
            )

        # Case-insensitive schema + table match.
        cur.execute(
            loader_queries.SQL_03_RESOLVE_TABLE_NAME,
            (requested_schema, requested_table),
        )
        rows = cur.fetchall()
        if len(rows) == 1:
            return rows[0][0], rows[0][1], "case-insensitive schema/table match"
        if len(rows) > 1:
            options = ", ".join(pg_qualified_display(r[0], r[1]) for r in rows)
            raise RuntimeError(
                f"Ambiguous table name {pg_qualified_display(requested_schema, requested_table)}. "
                f"Multiple case-insensitive schema/table matches found: {options}"
            )

        # Helpful small metadata lookup; still no user-table scan.
        cur.execute(
            loader_queries.SQL_04_RESOLVE_TABLE_NAME,
            (requested_schema,),
        )
        candidates = cur.fetchall()

    candidate_text = ", ".join(pg_qualified_display(r[0], r[1]) for r in candidates) or "none visible"
    raise RuntimeError(
        "Required table does not exist or is not visible to this user. "
        f"Requested: {pg_qualified_display(requested_schema, requested_table)}. "
        f"Visible tables in requested schema sample: {candidate_text}"
    )


def required_columns_for_purpose(purpose: str, mode: str) -> Dict[str, str]:
    if purpose == "temp":
        return {"id": "text-compatible", "data": "json/jsonb", "game_dt": "timestamp/timestamptz"}
    if purpose == "final":
        return {"id": "text-compatible", "data": "json/jsonb", "game_dt": "timestamp/timestamptz"}
    if purpose == "history":
        return {"id": "text-compatible", "data": "json/jsonb", "game_dt": "timestamp/timestamptz"}
    return {}


def assert_required_columns(conn, schema_name: str, table_name: str, purpose: str, mode: str) -> None:
    required = required_columns_for_purpose(purpose, mode)
    with conn.cursor() as cur:
        cur.execute(
            loader_queries.SQL_05_ASSERT_REQUIRED_COLUMNS,
            (schema_name, table_name),
        )
        cols = {name: dtype for name, dtype in cur.fetchall()}

    missing = [col for col in required if col not in cols]
    if missing:
        raise RuntimeError(
            f"Required column(s) missing on {pg_qualified_display(schema_name, table_name)}: {', '.join(missing)}"
        )


def assert_table_privileges(conn, pg_user: str, schema_name: str, table_name: str, required_privileges: List[str]) -> None:
    qname = pg_qualified_display(schema_name, table_name)
    with conn.cursor() as cur:
        failed = []
        for priv in required_privileges:
            cur.execute(loader_queries.SQL_06_ASSERT_TABLE_PRIVILEGES, (pg_user, qname, priv))
            if not bool(cur.fetchone()[0]):
                failed.append(priv)
    if failed:
        raise RuntimeError(
            f"Missing privilege(s) on {qname} for user {pg_user}: {', '.join(failed)}"
        )


def resolve_and_validate_table(conn, args, purpose: str, schema_attr: str, table_attr: str) -> Tuple[str, str]:
    requested_schema = getattr(args, schema_attr)
    requested_table = getattr(args, table_attr)
    resolved_schema, resolved_table, resolution = resolve_table_name(conn, requested_schema, requested_table)

    logging.info(
        "Resolved %s table: requested=%s resolved=%s resolution=%s",
        purpose,
        pg_qualified_display(strip_pg_quotes(requested_schema), strip_pg_quotes(requested_table)),
        pg_qualified_display(resolved_schema, resolved_table),
        resolution,
    )

    # Store resolved exact names back into args so all later SQL uses the correct quoted identifier.
    setattr(args, schema_attr, resolved_schema)
    setattr(args, table_attr, resolved_table)

    assert_required_columns(conn, resolved_schema, resolved_table, purpose, args.mode)

    if purpose == "temp":
        assert_table_privileges(conn, args.pg_user, resolved_schema, resolved_table, ["SELECT", "INSERT", "TRUNCATE"])
    elif purpose == "final":
        required = ["SELECT", "INSERT"]
        if args.mode == "upsert":
            required.append("UPDATE")
        assert_table_privileges(conn, args.pg_user, resolved_schema, resolved_table, required)
    elif purpose == "history":
        assert_table_privileges(conn, args.pg_user, resolved_schema, resolved_table, ["INSERT"])

    return resolved_schema, resolved_table


def truncate_temp(conn, args) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(loader_queries.SQL_07_TRUNCATE_TEMP).format(
                schema=sql.Identifier(args.temp_schema),
                table=sql.Identifier(args.temp_table),
            )
        )
    conn.commit()


def get_temp_count(conn, args) -> int:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(loader_queries.SQL_08_GET_TEMP_COUNT).format(
                schema=sql.Identifier(args.temp_schema),
                table=sql.Identifier(args.temp_table),
            )
        )
        return int(cur.fetchone()[0])


def validate_temp_values(conn, args) -> None:
    """Validate mapped source datetime and both derived fields in PostgreSQL."""
    q = sql.SQL(loader_queries.SQL_09_VALIDATE_TEMP_VALUES).format(
        schema=sql.Identifier(args.temp_schema),
        table=sql.Identifier(args.temp_table),
    )
    with conn.cursor() as cur:
        cur.execute(q)
        dates, payout, contribution = cur.fetchone()
    logging.info("Value checks: game_dt=%s payout=%s contribution=%s",
                 dates, payout, contribution)
    if dates or payout or contribution:
        raise RuntimeError(
            f"PostgreSQL value validation failed: "
            f"game_dt={dates}, payout={payout}, contribution={contribution}"
        )


def validate_final_values(conn, args) -> None:
    """Verify all three columns match TEMP for every extracted id."""
    q = sql.SQL(loader_queries.SQL_10_VALIDATE_FINAL_VALUES).format(
        ts=sql.Identifier(args.temp_schema),
        tt=sql.Identifier(args.temp_table),
        fs=sql.Identifier(args.target_schema),
        ft=sql.Identifier(args.target_table),
    )
    with conn.cursor() as cur:
        cur.execute(q)
        mismatches = cur.fetchone()[0]
    logging.info("Final vs temp value mismatches: %s", mismatches)
    if mismatches:
        raise RuntimeError(
            f"Final table differs from validated temp rows: {mismatches} record(s). "
            "Existing final IDs may be stale; use --mode upsert after review."
        )


def get_temp_duplicate_id_count(conn, args) -> int:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(loader_queries.SQL_11_GET_TEMP_DUPLICATE_ID_COUNT).format(
                schema=sql.Identifier(args.temp_schema),
                table=sql.Identifier(args.temp_table),
            )
        )
        return int(cur.fetchone()[0])


def get_existing_in_final_count(conn, args) -> int:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(loader_queries.SQL_12_GET_EXISTING_IN_FINAL_COUNT).format(
                temp_schema=sql.Identifier(args.temp_schema),
                temp_table=sql.Identifier(args.temp_table),
                target_schema=sql.Identifier(args.target_schema),
                target_table=sql.Identifier(args.target_table),
            )
        )
        return int(cur.fetchone()[0])


def write_existing_in_final_skips(conn, args, output_file: Path) -> int:
    """Write id, game_dt, and loaded_time for rows from temp that already exist in final."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    count = 0

    query = sql.SQL(loader_queries.SQL_13_WRITE_EXISTING_IN_FINAL_SKIPS).format(
        temp_schema=sql.Identifier(args.temp_schema),
        temp_table=sql.Identifier(args.temp_table),
        target_schema=sql.Identifier(args.target_schema),
        target_table=sql.Identifier(args.target_table),
    )

    # Named cursor streams result rows from PostgreSQL instead of holding all rows in memory.
    cursor_name = f"skip_existing_{int(time.time())}"
    #with conn.cursor(name=cursor_name) as cur, output_file.open("w", encoding="utf-8") as out:
    with conn.cursor(name=cursor_name) as cur, gzip.open(output_file, "wt", encoding="utf-8") as out:
        cur.itersize = 10000
        cur.execute(query)
        for row in cur:
            count += 1
            row_id, game_dt, loaded_time = row
            out.write(json.dumps({
                "id": row_id,
                "game_dt": game_dt,
                "loaded_time": loaded_time,
            }, ensure_ascii=False, default=json_default))
            out.write("\n")

    if count == 0:
        try:
            output_file.unlink()
        except FileNotFoundError:
            pass

    return count

# =============================================================================
# FILE READ + COPY
# =============================================================================

def stable_json_text(value: Any) -> str:
    """Serialize JSON without sorting keys so the approved payload sequence is retained."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


# Exact JSONB payload order retained from the original extractor output.
# This keeps new loaded rows consistent with the existing PostgreSQL JSONB payload shape.
PAYLOAD_COLUMNS = [
    "JW1",
    "JW2",
    "JW3",
    "JW4",
    "JW5",
    "PC1",
    "PC2",
    "PC3",
    "PC4",
    "PC5",
    "Outlet",
    "GameDate",
    "GameName",
    "SessionID",
    "TotalWins",
    "TotalStakes",
    "GameProvider",
    "PlayerAccount",
    "TransactionID",
    "JACKPOT_PAYOUT",
    "SEED_MONEY_WON",
    "UpdateDateTime",
    "JACKPOT_CONTRIBUTION",
    "PROGRESSIVE_CONTRIBUTION_PAID",
    "SEED_MONEY_JACKPOT_WON_OVER_1000",
]


# =============================================================================
# TRANSFORM STEP: JACKPOT DERIVED FIELDS
# =============================================================================
# Purpose:
#   The extractor now extracts raw MariaDB/MySQL values only.
#   This loader performs the small transformation needed to preserve the existing
#   PostgreSQL JSONB payload format.
#
# Derived fields added here before COPY into the temp table:
#   JACKPOT_PAYOUT       = JW1 + JW2 + JW3 + JW4 + JW5
#   JACKPOT_CONTRIBUTION = PC1 + PC2 + PC3 + PC4 + PC5
#
# Keep this section easy to find if future transform rules are added.

def to_decimal(value: Any) -> Decimal:
    """Preserve exact financial values; NULL means zero for the sum."""
    if value is None or value == "":
        return Decimal("0.000000")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Invalid jackpot component: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"Non-finite jackpot component: {value!r}")
    return result


def decimal_6(value: Decimal) -> str:
    return format(value, "f")  # Exact sum; do not round PC/JW components


def add_computed_fields_to_payload(payload: Dict[str, Any]) -> None:
    """
    Add the derived fields removed from the extraction step.

    This keeps the extractor as extract-only while preserving the existing PostgreSQL
    JSONB payload shape expected by the target table.
    """
    jackpot_payout = sum(to_decimal(payload.get(col)) for col in ["JW1", "JW2", "JW3", "JW4", "JW5"])
    jackpot_contribution = sum(to_decimal(payload.get(col)) for col in ["PC1", "PC2", "PC3", "PC4", "PC5"])

    payload["JACKPOT_PAYOUT"] = decimal_6(jackpot_payout)
    payload["JACKPOT_CONTRIBUTION"] = decimal_6(jackpot_contribution)


def transform_payload_for_postgres(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Preserve every original source key/value and add only:
      JACKPOT_PAYOUT = JW1+JW2+JW3+JW4+JW5
      JACKPOT_CONTRIBUTION = PC1+PC2+PC3+PC4+PC5

    Values remain strings exactly as supplied by the extractor.
    """
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object in the data field")

    derived = {"JACKPOT_PAYOUT", "JACKPOT_CONTRIBUTION"}
    original = dict(payload)
    if derived & original.keys():
        raise ValueError("Source JSON unexpectedly contains computed jackpot keys")

    missing = set(PAYLOAD_COLUMNS) - derived - original.keys()
    unexpected = original.keys() - (set(PAYLOAD_COLUMNS) - derived)
    if missing or unexpected:
        raise ValueError(
            f"Unexpected source payload shape: missing={sorted(missing)}, "
            f"extra={sorted(unexpected)}"
        )

    add_computed_fields_to_payload(payload)
    result = {col: payload[col] for col in PAYLOAD_COLUMNS}
    if any(result[k] != v for k, v in original.items()):
        raise ValueError("Original extracted JSON field changed during transformation")
    return result


def count_jsonl_rows(gz_file: Path) -> int:
    count = 0
    with gzip.open(str(gz_file), "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
                if count % PROGRESS_EVERY_ROWS == 0:
                    logging.info("Progress : file_line_count=%s", count)
    return count


def copy_jsonl_to_temp(conn, args, gz_file: Path) -> int:
    """
    Stream JSONL.GZ into PostgreSQL transient table using COPY.
    The source JSONL row must have:
        id      = TransactionID
        data    = JSON payload
        game_dt = GameDate
    """
    copied_count = 0
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

    copy_sql = sql.SQL(loader_queries.SQL_14_COPY_JSONL_TO_TEMP).format(
        schema=sql.Identifier(args.temp_schema),
        table=sql.Identifier(args.temp_table),
    )

    def flush_buffer(cur):
        nonlocal buffer
        buffer.seek(0)
        cur.copy_expert(copy_sql.as_string(conn), buffer)
        buffer.close()
        buffer = io.StringIO()
        return csv.writer(buffer, delimiter="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

    with conn.cursor() as cur:
        with gzip.open(str(gz_file), "rt", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                if not line.strip():
                    continue

                try:
                    obj = json.loads(line)
                    row_id = str(obj["id"])
                    data_obj = obj["data"]

                    # TRANSFORM STEP:
                    # Add derived jackpot fields and rebuild the JSON payload using
                    # the old approved PAYLOAD_COLUMNS order before COPY to PostgreSQL.
                    data_obj = transform_payload_for_postgres(data_obj)

                    game_dt = str(obj["game_dt"])
                    data_text = stable_json_text(data_obj)
                except Exception as exc:
                    raise ValueError(f"Invalid JSONL row at line {line_no}: {exc}")

                writer.writerow([row_id, data_text, game_dt])
                copied_count += 1

                if copied_count % COPY_FLUSH_ROWS == 0:
                    writer = flush_buffer(cur)

                if copied_count % PROGRESS_EVERY_ROWS == 0:
                    logging.info("Progress : copied_to_temp_count=%s", copied_count)

            if buffer.tell() > 0:
                flush_buffer(cur)

    conn.commit()
    return copied_count


# =============================================================================
# FINAL INSERT + HISTORY COPY
# =============================================================================

def insert_to_final(conn, args) -> Tuple[int, int, int]:
    """
    Returns:
        inserted_count, updated_count, skipped_count

    insert-only:
        duplicate id = do nothing
        missing id   = insert

    upsert:
        duplicate id with changed data/game_dt = update
    """
    with conn.cursor() as cur:
        if args.mode == "insert-only":
            cur.execute(
                sql.SQL(loader_queries.SQL_15_INSERT_TO_FINAL).format(
                    temp_schema=sql.Identifier(args.temp_schema),
                    temp_table=sql.Identifier(args.temp_table),
                    target_schema=sql.Identifier(args.target_schema),
                    target_table=sql.Identifier(args.target_table),
                )
            )
        else:
            cur.execute(
                sql.SQL(loader_queries.SQL_16_INSERT_TO_FINAL).format(
                    temp_schema=sql.Identifier(args.temp_schema),
                    temp_table=sql.Identifier(args.temp_table),
                    target_schema=sql.Identifier(args.target_schema),
                    target_table=sql.Identifier(args.target_table),
                )
            )

        result = cur.fetchone()

    # Commit only after final row-value validation succeeds in main().
    return int(result[0]), int(result[1]), int(result[2])


def copy_temp_to_history(conn, args) -> int:
    """
    Fast set-based, idempotent copy from temp to historical/replay table.
    The history table must already exist and must have at least:
        id TEXT with PRIMARY KEY or UNIQUE constraint
        data JSONB
        game_dt TIMESTAMPTZ

    Existing history IDs are preserved using ON CONFLICT (id) DO NOTHING.
    """
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(loader_queries.SQL_17_COPY_TEMP_TO_HISTORY).format(
                history_schema=sql.Identifier(args.history_schema),
                history_table=sql.Identifier(args.history_table),
                temp_schema=sql.Identifier(args.temp_schema),
                temp_table=sql.Identifier(args.temp_table),
            )
        )
        copied_rows = cur.fetchone()[0]
    conn.commit()
    return int(copied_rows)


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    args = parse_args()
    log_file = setup_logging(LOG_DIR, args.date)

    if args.file:
        gz_file = Path(args.file)
    else:
        gz_file = build_file_from_date(args.date)

    if not gz_file.exists():
        logging.error("Input file not found: %s", gz_file)
        return 1

    started_at = time.time()
    started_text = now_text()

    final_destination_table = pg_qualified_display(args.target_schema, args.target_table) if args.to_final else "not loaded; temp validation only"
    history_destination_table = pg_qualified_display(args.history_schema, args.history_table) if args.to_final and not args.skip_history else "not copied"
    current_stage = "initializing"
    current_schema = ""
    current_table = ""

    logging.info("Loader started")
    logging.info("Started at PHT : %s", started_text)
    logging.info("Remote PG      : %s:%s", args.pg_host, args.pg_port)
    logging.info("Database       : %s", args.pg_database)
    logging.info("PG user        : %s", args.pg_user)
    logging.info("Source file    : %s", gz_file)
    logging.info("Temp table     : %s.%s", args.temp_schema, args.temp_table)
    logging.info("Final table    : %s", final_destination_table)
    logging.info("History table  : %s", history_destination_table)
    logging.info("To final       : %s", args.to_final)
    logging.info("Mode           : %s", args.mode if args.to_final else "temp-only dry-run")
    logging.info("Log file       : %s", log_file)
    logging.info("Flush size     : %s", COPY_FLUSH_ROWS)
    logging.info("Progress every : %s rows", PROGRESS_EVERY_ROWS)
    logging.info("Check only     : %s", args.check_only)
    logging.info("Load skip out  : %s", LOAD_SKIP_OUT_DIR)

    conn = None
    file_count = 0
    copied_count = 0
    temp_count = 0
    duplicate_id_count = 0
    existing_in_final_count = 0
    expected_new_count = 0
    final_inserted_count = 0
    final_updated_count = 0
    final_skipped_count = 0
    final_duration = 0.0
    history_copied_count = 0
    history_duration = 0.0
    migration_status = "NOT READY"
    final_existing_skip_file = "not generated"

    skip_output_paths = make_skip_output_paths(args, gz_file)
    final_existing_skip_file = str(skip_output_paths["final_existing"])

    try:
        current_stage = "connect to PostgreSQL"
        conn = pg_connect(args)

        current_stage = "resolve and validate temp table"
        current_schema = args.temp_schema
        current_table = args.temp_table
        resolve_and_validate_table(conn, args, "temp", "temp_schema", "temp_table")

        if args.to_final:
            current_stage = "resolve and validate final table"
            current_schema = args.target_schema
            current_table = args.target_table
            resolve_and_validate_table(conn, args, "final", "target_schema", "target_table")

            if not args.skip_history:
                current_stage = "resolve and validate history table"
                current_schema = args.history_schema
                current_table = args.history_table
                resolve_and_validate_table(conn, args, "history", "history_schema", "history_table")

        final_destination_table = pg_qualified_display(args.target_schema, args.target_table) if args.to_final else "not loaded; temp validation only"
        history_destination_table = pg_qualified_display(args.history_schema, args.history_table) if args.to_final and not args.skip_history else "not copied"  # REPAIRED - pasted text lin

        if args.check_only:
            completed_at = time.time()
            ended_text = now_text()
            duration = completed_at - started_at
            print_summary("TABLE CHECK SUMMARY", [
                ("status", "PASS"),
                ("check_only", "YES"),
                ("started_at_pht", started_text),
                ("completed_at_pht", ended_text),
                ("duration_seconds", f"{duration:.2f}"),
                ("remote_pg", f"{args.pg_host}:{args.pg_port}"),
                ("database", args.pg_database),
                ("temp_table_resolved", pg_qualified_display(args.temp_schema, args.temp_table)),
                ("final_table_resolved", final_destination_table),
                ("history_table_resolved", history_destination_table),
                ("next_step", "run without --check-only to load temp/dry-run or add --final to migrate"),
            ])
            return 0

        current_stage = "count JSONL rows"
        current_schema = ""
        current_table = ""
        logging.info("Counting JSONL rows in file...")
        count_start = time.time()
        file_count = count_jsonl_rows(gz_file)
        count_duration = time.time() - count_start
        logging.info("File row count : %s", file_count)
        logging.info("File count duration seconds: %.2f", count_duration)

        current_stage = "truncate temp table"
        current_schema = args.temp_schema
        current_table = args.temp_table
        logging.info("Truncating transient table...")
        truncate_temp(conn, args)
        logging.info("Transient table truncated: %s.%s", args.temp_schema, args.temp_table)

        current_stage = "copy file to temp table"
        current_schema = args.temp_schema
        current_table = args.temp_table
        logging.info("Copying JSONL.GZ into transient table...")
        copy_start = time.time()
        copied_count = copy_jsonl_to_temp(conn, args, gz_file)
        copy_duration = time.time() - copy_start
        logging.info("Copied count from file: %s", copied_count)
        logging.info("Copy duration seconds: %.2f", copy_duration)

        current_stage = "validate temp table counts"
        current_schema = args.temp_schema
        current_table = args.temp_table
        temp_count = get_temp_count(conn, args)
        duplicate_id_count = get_temp_duplicate_id_count(conn, args)
        logging.info("Transient table row count: %s", temp_count)
        logging.info("Duplicate id count in transient table: %s", duplicate_id_count)

        if not (file_count == copied_count == temp_count):
            raise RuntimeError(
                f"FAIL: Count mismatch. file_count={file_count}, copied_count={copied_count}, temp_count={temp_count}. Final insert aborted."
            )

        if duplicate_id_count > 0:
            raise RuntimeError(
                f"FAIL: Duplicate id detected in transient table: duplicate_id_count={duplicate_id_count}. Final insert aborted."
            )

        current_stage = "validate temp datetime and jackpot values"
        validate_temp_values(conn, args)

        migration_status = "GOOD FOR MIGRATION"
        logging.info("PASS: file_count, copied_count, and transient table count match.")
        #logging.info("GOOD FOR MIGRATION: temp table is loaded and validated.")
        logging.info("[DATALAKE VALIDATION][PASS] Transient table loaded and validated; ready for Data Lake insertion.")

        if args.to_final:
            current_stage = "pre-check existing ids in final table"
            current_schema = args.target_schema
            current_table = args.target_table
            existing_in_final_count = write_existing_in_final_skips(
                conn, args, skip_output_paths["final_existing"]
            )
            expected_new_count = temp_count - existing_in_final_count
            #logging.info("Existing ids in final before insert: %s", existing_in_final_count)
            logging.info("[DATALAKE] Existing IDs in destination before insert: %s", existing_in_final_count)
            #logging.info("Expected new rows to insert: %s", expected_new_count)
            logging.info("[DATALAKE] Expected new rows to insert: %s", expected_new_count)
            if existing_in_final_count > 0:
                logging.info("Skipped-existing-final records written to: %s", skip_output_paths["final_existing"])

            current_stage = "insert/upsert to final table"
            #logging.info("--final supplied. Loading transient rows to final table...")
            logging.info("[DATALAKE INSERT] --final supplied. Loading validated transient rows into the persistent Data Lake table.")
            final_start = time.time()
            final_inserted_count, final_updated_count, final_skipped_count = insert_to_final(conn, args)
            current_stage = "verify final matches temp"
            validate_final_values(conn, args)
            conn.commit()
            final_duration = time.time() - final_start
            logging.info("Final load completed")
            logging.info("Final inserted count: %s", final_inserted_count)
            logging.info("Final updated count: %s", final_updated_count)
            logging.info("Final skipped count: %s", final_skipped_count)
            logging.info("Final load duration seconds: %.2f", final_duration)

            migration_status = f"LOADED IN {final_destination_table}"

            if not args.skip_history:
                current_stage = "copy temp rows to history table"
                current_schema = args.history_schema
                current_table = args.history_table
                logging.info("Copying all temp rows to history/replay table: %s.%s", args.history_schema, args.history_table)
                history_start = time.time()
                history_copied_count = copy_temp_to_history(conn, args)
                history_duration = time.time() - history_start
                logging.info("History copy completed. copied_rows=%s duration_seconds=%.2f", history_copied_count, history_duration)
        else:
            logging.info("--final not supplied. Dry-run stopped after transient table validation PASS.")

        completed_at = time.time()
        ended_text = now_text()
        duration = completed_at - started_at

        logging.info("Completed at PHT: %s", ended_text)
        logging.info("Duration        : %.3f minutes (%s)", duration / 60.0, seconds_to_hhmmss(duration))
        logging.info("Load completed successfully. temp_count=%s to_final=%s", temp_count, args.to_final)

        summary_fields = [
            ("status", "PASS"),
            ("migration_status", migration_status),
            ("started_at_pht", started_text),
            ("completed_at_pht", ended_text),
            ("duration_seconds", f"{duration:.2f}"),
            ("duration_minutes", f"{duration / 60.0:.3f}"),
            ("duration_hhmmss", seconds_to_hhmmss(duration)),
            ("remote_pg", f"{args.pg_host}:{args.pg_port}"),
            ("database", args.pg_database),
            ("source_file", gz_file),
            ("temp_table", f"{args.temp_schema}.{args.temp_table}"),
            ("final_table", final_destination_table),
            ("history_table", history_destination_table),
            ("file_jsonl_line_count", file_count),
            ("copied_to_temp_count", copied_count),
            ("temp_table_count", temp_count),
            ("duplicate_id_count", duplicate_id_count),
            ("to_final", args.to_final),
            ("mode", args.mode if args.to_final else 'temp-only dry-run'),
        ]
        if args.to_final:
            summary_fields.extend([
                ("existing_in_final_before", existing_in_final_count),
                ("expected_new_rows", expected_new_count),
                ("final_existing_skip_file", final_existing_skip_file if existing_in_final_count > 0 else 'none'),
                ("final_inserted_count", final_inserted_count),
                ("final_updated_count", final_updated_count),
                ("final_skipped_count", final_skipped_count),
                ("final_duration_seconds", f"{final_duration:.2f}"),
            ])
            if not args.skip_history:
                summary_fields.extend([
                    ("history_copied_count", history_copied_count),
                    ("history_duration_seconds", f"{history_duration:.2f}"),
                ])
        else:
            summary_fields.extend([
                ("final_load_status", "not loaded; temp validation only"),
                ("next_step", "rerun with --final --to-schema <schema> --to-table <table>"),
            ])
        print_summary("LOAD SUMMARY", summary_fields)
        return 0

    except Exception as exc:
        log_exception("Loader failed", exc, LOG_DIR)
        if is_permission_denied_error(exc):
            log_permission_context(args, current_stage, current_schema, current_table, exc)
        if conn:
            conn.rollback()

        completed_at = time.time()
        ended_text = now_text()
        duration = completed_at - started_at

        summary_fields = [
            ("status", "FAIL"),
            ("started_at_pht", started_text),
            ("completed_at_pht", ended_text),
            ("duration_seconds", f"{duration:.2f}"),
            ("duration_minutes", f"{duration / 60.0:.3f}"),
            ("duration_hhmmss", seconds_to_hhmmss(duration)),
            ("remote_pg", f"{args.pg_host}:{args.pg_port}"),
            ("database", args.pg_database),
            ("source_file", gz_file),
            ("temp_table", f"{args.temp_schema}.{args.temp_table}"),
            ("final_table", final_destination_table),
            ("history_table", history_destination_table),
            ("file_jsonl_line_count", file_count),
            ("copied_to_temp_count", copied_count),
            ("temp_table_count", temp_count),
            ("final_existing_skip_file", final_existing_skip_file if existing_in_final_count > 0 else 'none'),
            ("failed_stage", current_stage),
            ("error", exc),
        ]
        print_summary("LOAD SUMMARY", summary_fields)
        if is_permission_denied_error(exc):
            print_permission_context(args, current_stage, current_schema, current_table, exc)
        return 1

    finally:
        if conn:
            conn.close()




# =============================================================================
# ADDED - FAILURE-ONLY SMTP ALERTS (shared modules; .env is exported by Bash)
# =============================================================================
# Shared module already supplies the SMTP implementation.
sys.path.insert(0, os.environ["SHARED_MODS"])
from etl_alert import send_failure

# ADDED - DQ handoff configuration; does not modify any PG load SQL or validation.
DQ_BASH = BASE_DIR / "batch_daily_dqraw.sh"
DQ_HANDOFF_SECONDS = 10
DQ_HANDOFF_DIR = Path(os.environ["BASE_LOG_DIR"]) / "dq_handoff"


def send_loader_failure_email(reason: str) -> None:
    """Notify on loader FAIL / DQ startup FAIL; never email on PASS."""
    logging.error("PostgreSQL loader failure: %s", reason)
    send_failure(
        stage="MYSQL-DLAKE-PG-LOADER",
        entity="gameTransaction",
        window_from="unknown",
        window_to="unknown",
        exit_code=1,
    )


def handoff_dq_after_loader(args) -> None:
    """ADDED - Load PASSED and PostgreSQL connection has already closed.

    Launch Bash DQ in a new session; inspect its startup for 10 seconds; exit
    without waiting for DQ completion. DQ Python runs as another process.
    """
    if not DQ_BASH.is_file():
        raise FileNotFoundError(f"DQ Bash missing: {DQ_BASH}")
    # REFUSE group/world writable script to reduce replacement/hijacking risk.
    if DQ_BASH.stat().st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise PermissionError(f"DQ Bash must not be group/world writable: {DQ_BASH}")
    DQ_HANDOFF_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    marker = DQ_HANDOFF_DIR / f"dq_{now}.started"
    status = DQ_HANDOFF_DIR / f"dq_{now}.status"
    logfile = DQ_HANDOFF_DIR / f"dq_{now}.log"
    # DQ receives the exact file and the corresponding 06:00-06:00 window.
    day_label = get_load_date_label(args, Path(args.file) if args.file else build_file_from_date(args.date))
    dt = datetime.strptime(day_label, "%Y%m%d")
    from_dt = dt.strftime("%Y-%m-%d 06:00:00")
    from datetime import timedelta
    to_dt = (dt + timedelta(days=1)).strftime("%Y-%m-%d 06:00:00")
    source_file = str(Path(args.file).resolve()) if args.file else str(build_file_from_date(args.date).resolve())
    command = ["/usr/bin/bash", str(DQ_BASH), from_dt, to_dt, source_file, str(marker), str(status)]
    safe_env = os.environ.copy()  # Inherit shared PRISM TOML and .env exports
    safe_env["PATH"] = "/usr/local/bin:/usr/bin:/bin"
    with logfile.open("ab") as log:
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL,
                                 stdout=log, stderr=subprocess.STDOUT,
                                 cwd=str(BASE_DIR), env=safe_env,
                                 start_new_session=True, close_fds=True, shell=False)
    logging.info("DQ independent startup PID=%s; log=%s", child.pid, logfile)
    # ADDED - This validates startup, not the final DQ data quality outcome.
    time.sleep(DQ_HANDOFF_SECONDS)
    rc = child.poll()
    if rc is None and marker.exists():
        logging.info("DQ HANDOFF ACCEPTED: still running after %ss", DQ_HANDOFF_SECONDS)
        return
    if rc == 0 and status.exists() and status.read_text().startswith("PASS"):
        logging.info("DQ completed successfully within startup interval")
        return
    raise RuntimeError(f"DQ handoff failed/stopped within {DQ_HANDOFF_SECONDS}s; "
                       f"pid={child.pid} exit={rc} marker={marker.exists()} log={logfile}")


def orchestrated_main() -> int:
    """ADDED - Wrapper around ORIGINAL main(). Original data processing unchanged."""
    try:
        result = main()  # RETAIN - untouched PostgreSQL load logic and conn.close()
        if result != 0:
            send_loader_failure_email("PostgreSQL loader exited with FAIL. Review loader logs.")
            return result
        # Do not hand off on validation-only / staging-only executions.
        if "--check-only" in sys.argv or ("--final" not in sys.argv and "--to-final" not in sys.argv):
            return 0
        # UPDATED - Standalone mode: loader PASS without starting independent DQ.
        # This does not bypass staging checks, final validation, or the PG commit.
        if SKIP_DQ:
            logging.info("STANDALONE PG LOADER PASS: SKIP_DQ=True; DQ handoff skipped.")
            return 0
        # RETAIN - Normal event-driven mode: start DQ after PG connection closes.
        args = parse_args()
        handoff_dq_after_loader(args)
        return 0
    except Exception as exc:
        log_exception("Loader orchestration failure", exc, LOG_DIR)
        send_loader_failure_email(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(orchestrated_main())
