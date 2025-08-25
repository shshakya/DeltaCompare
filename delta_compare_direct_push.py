import argparse
import json
import logging
import os
import socket
import sys
import tempfile
import time as pytime
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dtime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import Json as PgJson  # noqa: F401
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSON as SAJson  # noqa: F401
from sqlalchemy.exc import DBAPIError

# Temporal handling: Python 3.9+
try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

from azure.eventhub import EventData, EventHubProducerClient

# ------------------------------------------------------------------------------
# Load configuration
# ------------------------------------------------------------------------------
with open("db_config.json") as config_file:
    config = json.load(config_file)

db1_engine = create_engine(
    config["db1"],
    pool_size=80,
    max_overflow=40,
    pool_timeout=120,
    pool_recycle=1800,
    pool_pre_ping=True,
    pool_use_lifo=True,
)
db2_engine = create_engine(
    config["db2"],
    pool_size=80,
    max_overflow=40,
    pool_timeout=120,
    pool_recycle=1800,
    pool_pre_ping=True,
    pool_use_lifo=True,
)

EVENT_HUB_CONNECTION_STR = config["event_hub_connection_str"]
EVENT_HUB_NAME = config["event_hub_name"]
fdw_servername = config["fdw_servername"]
fdw_user = config["fdw_user"]
fdw_db = config["fdw_db"]
fdw_password = config["fdw_password"]
fdw_host = config["fdw_host"]
fdw_port = config["fdw_port"]
max_threads = config["max_workers"]


# Convert human-readable timestamp to datetime
human_ts_str = config.get("static_timestamp", "2025-08-22T14:00:00Z")
dt = datetime.fromisoformat(human_ts_str.replace("Z", "+00:00"))

# Convert to milliseconds, microseconds, nanoseconds
STATIC_TS_MS = int(dt.timestamp() * 1000)
STATIC_TS_US = str(STATIC_TS_MS * 1000)
STATIC_TS_NS = str(STATIC_TS_MS * 1000000)


# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
log_filename = f"datadeltalog_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
)
logging.info(f"Script started on host: {socket.gethostname()}")
logging.getLogger("azure").setLevel(logging.WARNING)

# ------------------------------------------------------------------------------
# Errors
# ------------------------------------------------------------------------------
def _pg_message_only(err: Exception) -> str:
    """Return primary PostgreSQL message from SA/psycopg2 exceptions."""
    if isinstance(err, DBAPIError) and getattr(err, "orig", None) is not None:
        pg = err.orig
        primary = getattr(getattr(pg, "diag", None), "message_primary", None)
        return primary or getattr(pg, "pgerror", None) or str(pg)
    return str(err)


# ------------------------------------------------------------------------------
# One-time setup: FDW schema etc.
# ------------------------------------------------------------------------------
def setup_fdw_and_confirm(
    db2_engine,
    server,
    user,
    dbname,
    password,
    host,
    port,
    prompt="FDW setup complete. Continue with delta comparison? (yes/no): ",
) -> bool:
    """
    CALL public.setup_fdw_for_publications(:server,:user,:dbname,:pwd,:host,:port,:p_return)
    """
    try:
        with db2_engine.begin() as conn:
            logging.info("Setting up the schema for creating the delta...")
            result = conn.execute(
                text(
                    """
                    CALL public.setup_fdw_for_publications(
                        :server, :user, :dbname, :pwd, :host, :port, :p_return
                    )
                    """
                ),
                {
                    "server": server,
                    "user": user,
                    "dbname": dbname,
                    "pwd": password,
                    "host": host,
                    "port": port,
                    "p_return": 0,  # INOUT starts at 0
                },
            )
            try:
                p_return = result.scalar_one()
            except Exception:
                row = result.mappings().first()
                p_return = row["p_return"] if row and "p_return" in row else 0
            p_return = int(p_return or 0)
    except Exception as e:
        logging.error(f"FDW setup failed: {e}")
        sys.exit(1)

    if p_return == 1:
        logging.error("FDW setup failed. Aborting.")
        sys.exit(1)

    logging.info("FDW setup completed. Continuing...")

    try:
        resp = input(prompt).strip().lower()
    except KeyboardInterrupt:
        logging.info("Aborted by user.")
        return False

    return resp in ("y", "yes")


# ------------------------------------------------------------------------------
# Metadata helpers
# ------------------------------------------------------------------------------
def get_user_tables(engine, publications: List[str]) -> List[Tuple[str, str]]:
    """
    Return [(table_name, schema_name), ...] for tables in specified publications.
    If publications list is empty, return empty list (no fallback).
    """
    if not publications:
        logging.warning("No publication names provided in config. Skipping comparison.")
        return []

    placeholders = ", ".join([f"%s" for _ in publications])
    sql = f"""
        SELECT tablename AS full_table_name, schemaname
        FROM pg_catalog.pg_tables
        WHERE (schemaname, tablename) IN (
          SELECT schemaname, tablename
          FROM pg_publication_tables
          WHERE pubname IN ({placeholders})
        );
    """
    df = pd.read_sql(sql, engine, params=publications)
    return list(zip(df["full_table_name"], df["schemaname"]))



def get_primary_key_columns(schema: str, table: str) -> List[str]:
    sql = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary;
    """
    conn = db2_engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (f"{schema}.{table}",))
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def get_unique_key_columns(engine, schema: str, table: str) -> List[str]:
    """
    Prefer (in order):
      1) UNIQUE index that is the REPLICA IDENTITY
      2) UNIQUE index whose key columns are all NOT NULL
      3) Smallest UNIQUE index
    """
    sql = text(
        """
        WITH rel AS (
          SELECT c.oid AS relid
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = :schema AND c.relname = :table
        ),
        u AS (
          SELECT
            i.indexrelid,
            i.indnatts                                    AS nkeys,
            i.indisreplident                              AS is_replica_ident,
            COALESCE((SELECT NOT condeferrable
                      FROM pg_constraint
                      WHERE conindid = i.indexrelid
                        AND contype = 'u'), TRUE)         AS not_deferrable,
            bool_and(a.attnotnull)                        AS all_not_null,
            array_agg(a.attname ORDER BY k.ord)           AS cols
          FROM rel
          JOIN pg_index i        ON i.indrelid = rel.relid
          JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
          JOIN pg_attribute a    ON a.attrelid = rel.relid AND a.attnum = k.attnum
          WHERE i.indisunique
            AND NOT i.indisprimary
            AND i.indisvalid
            AND i.indpred IS NULL
            AND i.indexprs IS NULL
          GROUP BY i.indexrelid, i.indnatts, i.indisreplident
        )
        SELECT cols
        FROM u
        ORDER BY is_replica_ident DESC,
                 all_not_null    DESC,
                 nkeys           ASC,
                 indexrelid      ASC
        LIMIT 1;
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"schema": schema, "table": table}).first()
        return list(row[0]) if row and row[0] else []


def get_db_timezone(engine) -> str:
    """Return PostgreSQL 'TimeZone' setting (e.g., 'UTC', 'Asia/Kolkata')."""
    with engine.connect() as conn:
        tz = conn.execute(text("SHOW TimeZone")).scalar()
    return tz or "UTC"


def get_column_types(
    schema: str, table: str
) -> Tuple[Dict[str, str], Dict[str, Optional[int]]]:
    """
    Return ({col: typname}, {col: scale})

    scale is only populated for time/timetz/timestamp/timestamptz.
    For these types, a.atttypmod encodes scale as (scale + 4).
    A NULL/negative typmod means "unspecified precision" (treat as 6),
    matching Debezium which maps TIMESTAMP (no precision) to MicroTimestamp.
    """
    sql = """
        SELECT a.attname AS col,
               t.typname AS typ,
               CASE
                 WHEN t.typname IN ('time', 'timetz', 'timestamp', 'timestamptz')
                 THEN CASE WHEN a.atttypmod > 0 THEN a.atttypmod - 4 ELSE NULL END
                 ELSE NULL
               END AS scale
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        JOIN pg_type t ON t.oid = a.atttypid
        WHERE n.nspname = %s
          AND c.relname = %s
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum;
    """
    conn = db2_engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (schema, table))
            rows = cur.fetchall()
        types = {col: typ for col, typ, _ in rows}
        scales = {
            col: (scale if scale is not None else (6 if typ in ("time", "timetz", "timestamp", "timestamptz") else None))
            for col, typ, scale in rows
        }
        return types, scales
    finally:
        conn.close()


def get_comparable_columns(engine, schema: str, table: str, pk_cols: List[str]) -> List[str]:
    if not pk_cols:
        return []
    placeholders = ", ".join([f":pk_{i}" for i in range(len(pk_cols))])
    sql = text(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
          AND column_name NOT IN ({placeholders})
        """
    )
    params = {"schema": schema, "table": table}
    params.update({f"pk_{i}": col for i, col in enumerate(pk_cols)})
    with engine.connect() as conn:
        result = conn.execute(sql, params).mappings().all()
        return [row["column_name"] for row in result]


# ------------------------------------------------------------------------------
# Debezium-compatible temporal normalization (adaptive precision)
# ------------------------------------------------------------------------------
_EPOCH_DATE = date(1970, 1, 1)


def _days_since_epoch(d: date) -> int:
    return (d - _EPOCH_DATE).days


def _time_to_microseconds(t: dtime) -> int:
    return (t.hour * 3600 + t.minute * 60 + t.second) * 1_000_000 + t.microsecond


def _parse_date_str(s: str) -> date:
    return date.fromisoformat(s)


def _parse_time_str(s: str) -> dtime:
    return dtime.fromisoformat(s)


def _parse_time_tz_str(s: str) -> dtime:
    s = s.strip().replace("Z", "+00:00")
    return dtime.fromisoformat(s)


def _parse_ts_naive(s: str) -> datetime:
    s = "T".join(s.strip().split())
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
        aware = datetime.fromisoformat(s)
        return aware.replace(tzinfo=None)
    return datetime.fromisoformat(s)


def _parse_ts_tz(s: str) -> datetime:
    s = "T".join(s.strip().split()).replace("Z", "+00:00")
    return datetime.fromisoformat(s)


def _to_epoch_microseconds(dt_aware_utc: datetime) -> int:
    return int(dt_aware_utc.timestamp() * 1_000_000)


def _ensure_aware_utc_from_naive(dt_naive: datetime, assume_tz: str) -> datetime:
    tz = ZoneInfo(assume_tz)
    return dt_naive.replace(tzinfo=tz).astimezone(timezone.utc)


def _format_iso_zoned_timestamp_utc(dt_aw: datetime) -> str:
    dt_utc = dt_aw.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ") if dt_utc.microsecond else dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_iso_zoned_time(t_aw: dtime) -> str:
    if t_aw.tzinfo is None:
        return t_aw.strftime("%H:%M:%S.%f") if t_aw.microsecond else t_aw.strftime("%H:%M:%S")
    offset = t_aw.utcoffset() or timedelta(0)
    total_seconds = int(offset.total_seconds())
    sign = "+" if total_seconds >= 0 else "-"
    total_seconds = abs(total_seconds)
    hh, rem = divmod(total_seconds, 3600)
    mm, _ = divmod(rem, 60)
    frac = f".{t_aw.microsecond:06d}" if t_aw.microsecond else ""
    return f"{t_aw.hour:02d}:{t_aw.minute:02d}:{t_aw.second:02d}{frac}{sign}{hh:02d}:{mm:02d}"


def _as_dict(obj: Any) -> Optional[Dict[str, Any]]:
    """Ensure JSONB row is a dict (handles str/None)."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str):
        try:
            return json.loads(obj)
        except Exception:
            return None
    return None


def debezium_adaptive_normalize_record(
    record: Optional[Dict[str, Any]],
    col_types: Dict[str, str],
    col_scales: Dict[str, Optional[int]],
    *,
    assume_tz: str = "UTC",
) -> Optional[Dict[str, Any]]:
    """
    Apply Debezium adaptive mappings exactly:
      DATE -> days (INT32)
      TIME(1-3) -> ms (INT32), TIME(4-6) -> µs (INT64)
      TIMESTAMP(1-3) -> ms (INT64), TIMESTAMP(4-6)/unspecified -> µs (INT64)
      TIMESTAMPTZ -> ISO UTC string (Z)
      TIMETZ -> ISO time with offset
      NUMERIC/DECIMAL, INTERVAL -> strings
    """
    if not record:
        return record

    out: Dict[str, Any] = {}
    for col, val in record.items():
        typ = col_types.get(col, "").lower()
        scale = col_scales.get(col, None)

        # Default unspecified precision to 6 for time/timestamp families
        effective_scale = scale
        if typ in ("time", "timetz", "timestamp", "timestamptz") and effective_scale is None:
            effective_scale = 6

        if val is None:
            out[col] = None
            continue

        try:
            if typ == "date":
                d = _parse_date_str(val) if isinstance(val, str) else val
                out[col] = _days_since_epoch(d) if isinstance(d, date) else val
                continue

            if typ == "time":
                t = _parse_time_str(val) if isinstance(val, str) else val
                if not isinstance(t, dtime):
                    out[col] = val
                    continue
                micros = _time_to_microseconds(t)
                if effective_scale is not None and effective_scale <= 3:
                    out[col] = int(micros // 1000)  # milliseconds INT32
                else:
                    out[col] = micros  # microseconds INT64
                continue

            if typ == "timetz":
                t = _parse_time_tz_str(val) if isinstance(val, str) else val
                out[col] = _format_iso_zoned_time(t) if isinstance(t, dtime) else str(val)
                continue

            if typ == "timestamp":
                # interpret naive in DB/connector time zone
                if isinstance(val, str):
                    dt_naive = _parse_ts_naive(val)
                elif isinstance(val, datetime):
                    dt_naive = val if val.tzinfo is None else val.replace(tzinfo=None)
                else:
                    out[col] = val
                    continue
                dt_utc = _ensure_aware_utc_from_naive(dt_naive, assume_tz)
                micros = _to_epoch_microseconds(dt_utc)
                if effective_scale is not None and effective_scale <= 3:
                    out[col] = int(micros // 1000)  # milliseconds
                else:
                    out[col] = micros  # microseconds
                continue

            if typ == "timestamptz":
                if isinstance(val, str):
                    dt_aw = _parse_ts_tz(val)
                elif isinstance(val, datetime):
                    dt_aw = val if val.tzinfo is not None else val.replace(tzinfo=ZoneInfo(assume_tz))
                else:
                    out[col] = str(val)
                    continue
                out[col] = _format_iso_zoned_timestamp_utc(dt_aw)
                continue

            if typ in ("interval", "numeric", "decimal"):
                out[col] = str(val)
                continue

            # fallback passthrough
            out[col] = val

        except Exception:
            # Preserve original if we fail to parse or convert
            out[col] = val

    return out


# ------------------------------------------------------------------------------
# Payload builder (Debezium envelope)
# ------------------------------------------------------------------------------
def build_payload(
    table_name: str,
    before: Any,
    after: Any,
    op_type: str,
    schema: str,
    col_types: Dict[str, str],
    col_scales: Dict[str, Optional[int]],
    assume_tz: str = "UTC",
    snapshot_flag: Optional[str] = "false",
) -> Dict[str, Any]:
    before_dict = _as_dict(before)
    after_dict = _as_dict(after)

    norm_before = debezium_adaptive_normalize_record(before_dict, col_types, col_scales, assume_tz=assume_tz)
    norm_after = debezium_adaptive_normalize_record(after_dict, col_types, col_scales, assume_tz=assume_tz)

    source = {
        "ts_ms": STATIC_TS_MS,
        "ts_us": STATIC_TS_US,
        "ts_ns": STATIC_TS_NS,
        "version": "3.2.0.Final",
        "connector": "postgresql",
        "name": "PostgreSQL_server",
        "db": fdw_db,
        "schema": schema,
        "table": table_name,
    }
    if snapshot_flag is not None:
        # "true" | "last" | "false" (if you want to mimic Debezium snapshots)
        source["snapshot"] = snapshot_flag

    return {
     "payload": {
            "before": norm_before,
            "after": norm_after,
            "ts_ms": STATIC_TS_MS,
            "ts_us": STATIC_TS_US,
            "ts_ns": STATIC_TS_NS,
            "source": source,
            "op": op_type
             }
     }

# --------------------------------------------------------------------------
# SQL Builder Helpers: Chunked jsonb_build_object to avoid TooManyArguments
# --------------------------------------------------------------------------

def _chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def _jsonb_build_object_chunks(alias, cols, typemap):
    """
    Build a concatenated jsonb_build_object() expression for a list of columns,
    chunked to avoid PostgreSQL's 100-argument limit.
    """
    chunk_exprs = []
    for chunk in _chunks(cols, 50):
        pairs = []
        for c in chunk:
            typ = typemap.get(c, 'text')
            base = f'{alias}."{c}"'
            if typ == "xml":
                base = f"{base}::text"
            pairs.append(f"'{c}', to_jsonb({base})")
        chunk_exprs.append(f"jsonb_build_object({', '.join(pairs)})")
    return " || ".join(chunk_exprs) if chunk_exprs else "to_jsonb(NULL)"

# --------------------------------------------------------------------------
# Delta SQL builders (xml safely cast to text where needed)
# --------------------------------------------------------------------------

def build_fdw_delta_query_keys_only(
    schema_local: str, schema_remote: str, table_name: str, key_cols: list, col_types: dict
) -> str:
    def qident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    non_keys = [c for c in col_types.keys() if c not in key_cols]
    json_cols = key_cols + non_keys

    obj_local = _jsonb_build_object_chunks("l", json_cols, col_types)
    obj_remote = _jsonb_build_object_chunks("r", json_cols, col_types)

    pk_join = " AND ".join([f"l.{qident(c)} = r.{qident(c)}" for c in key_cols]) or "FALSE"
    is_null_remote = " AND ".join([f"r.{qident(c)} IS NULL" for c in key_cols]) or "FALSE"
    is_null_local = " AND ".join([f"l.{qident(c)} IS NULL" for c in key_cols]) or "FALSE"

    return f"""
        -- INSERTS (local only)
        SELECT 'c'::text AS op,
               NULL::jsonb AS before,
               {obj_local} AS after
        FROM {qident(schema_local)}.{qident(table_name)} l
        LEFT JOIN {qident(schema_remote)}.{qident(table_name)} r ON {pk_join}
        WHERE {is_null_remote}

        UNION ALL

        -- DELETES (remote only)
        SELECT 'd'::text AS op,
               {obj_remote} AS before,
               NULL::jsonb AS after
        FROM {qident(schema_remote)}.{qident(table_name)} r
        LEFT JOIN {qident(schema_local)}.{qident(table_name)} l ON {pk_join}
        WHERE {is_null_local}
    """

def build_fdw_delta_query_no_key(
    schema_local: str, schema_remote: str, table_name: str, col_types: dict
) -> str:
    def qident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    all_cols = list(col_types.keys())
    obj_local = _jsonb_build_object_chunks("l", all_cols, col_types)
    obj_remote = _jsonb_build_object_chunks("r", all_cols, col_types)

    return f"""
        -- INSERTS (local rows not in remote)
        SELECT 'c'::text AS op,
               NULL::jsonb AS before,
               {obj_local} AS after
        FROM {qident(schema_local)}.{qident(table_name)} l
        EXCEPT ALL
        SELECT 'c'::text AS op,
               NULL::jsonb AS before,
               {obj_remote} AS after
        FROM {qident(schema_remote)}.{qident(table_name)} r

        UNION ALL

        -- DELETES (remote rows not in local)
        SELECT 'd'::text AS op,
               {obj_remote} AS before,
               NULL::jsonb AS after
        FROM {qident(schema_remote)}.{qident(table_name)} r
        EXCEPT ALL
        SELECT 'd'::text AS op,
               {obj_local} AS before,
               NULL::jsonb AS after
        FROM {qident(schema_local)}.{qident(table_name)} l
    """

## helper function for handling xml columns.

def _is_distinct_expr(col: str, typemap: Dict[str, str]) -> str:
    """
    Returns an IS DISTINCT FROM expression for a column, with safe casts for types
    (notably xml) that don't have = or IS DISTINCT FROM operators.
    """
    # typemap contains pg_type.typname (lowercase) keyed by column name
    t = (typemap.get(col, "") or "").lower()
    qcol = '"' + col.replace('"', '""') + '"'
    if t == "xml":
        # Cast both sides to text for comparison
        return f"l.{qcol}::text IS DISTINCT FROM r.{qcol}::text"
    # Extend here if you later hit other types without = (e.g., custom domains)
    return f"l.{qcol} IS DISTINCT FROM r.{qcol}"

def build_fdw_delta_query(
    schema_local: str,
    schema_remote: str,
    table_name: str,
    pk_cols: list,
    compare_cols: list,
    col_types: dict,
) -> str:
    def qident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    json_cols = pk_cols + compare_cols
    obj_local = _jsonb_build_object_chunks("l", json_cols, col_types)
    obj_remote = _jsonb_build_object_chunks("r", json_cols, col_types)

    pk_join = " AND ".join([f"l.{qident(c)} = r.{qident(c)}" for c in pk_cols]) or "FALSE"
    is_diff_parts = [_is_distinct_expr(c, col_types) for c in compare_cols]
    is_diff = " OR ".join(is_diff_parts) if is_diff_parts else "FALSE"
    is_null_remote = " AND ".join([f"r.{qident(c)} IS NULL" for c in pk_cols]) or "FALSE"
    is_null_local = " AND ".join([f"l.{qident(c)} IS NULL" for c in pk_cols]) or "FALSE"

    return f"""
        SELECT op, before, after
        FROM (
          -- updated
          SELECT 'u' AS op, {obj_remote} AS before, {obj_local} AS after
          FROM {qident(schema_remote)}.{qident(table_name)} r
          JOIN {qident(schema_local)}.{qident(table_name)} l ON {pk_join}
          WHERE {is_diff}

          UNION ALL

          -- inserted
          SELECT 'c' AS op, NULL AS before, {obj_local} AS after
          FROM {qident(schema_local)}.{qident(table_name)} l
          LEFT JOIN {qident(schema_remote)}.{qident(table_name)} r ON {pk_join}
          WHERE {is_null_remote}

          UNION ALL

          -- deleted
          SELECT 'd' AS op, {obj_remote} AS before, NULL AS after
          FROM {qident(schema_remote)}.{qident(table_name)} r
          LEFT JOIN {qident(schema_local)}.{qident(table_name)} l ON {pk_join}
          WHERE {is_null_local}
        ) AS delta;
    """
# ------------------------------------------------------------------------------
# Event Hubs sender (batch-safe)
# ------------------------------------------------------------------------------
def send_to_eventhub(
    table: str,
    df: pd.DataFrame,
    schema: str,
    col_types: Dict[str, str],
    col_scales: Dict[str, Optional[int]],
    assume_tz: str = "UTC",
    snapshot_flag: Optional[str] = None,
) -> None:
    if df.empty:
        logging.info(f"No changes for {schema}.{table}")
        return

    producer = EventHubProducerClient.from_connection_string(
        EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )
    batch = producer.create_batch()
    items_in_batch = 0

    def _flush():
        nonlocal batch, items_in_batch
        if items_in_batch > 0:
            producer.send_batch(batch)
            batch = producer.create_batch()
            items_in_batch = 0

    for _, row in df.iterrows():
        payload_obj = build_payload(
            table_name=table,
            before=row.get("before"),
            after=row.get("after"),
            op_type=row.get("op"),
            schema=schema,
            col_types=col_types,
            col_scales=col_scales,
            assume_tz=assume_tz,
            snapshot_flag=snapshot_flag,
        )
        payload = json.dumps(payload_obj, ensure_ascii=False)
        # 🧪 Print payload for testing
        #print(f"\n--- Payload for table '{table}' ---\n{payload}\n")
        try:
            batch.add(EventData(payload))
            items_in_batch += 1
        except ValueError:
            # Batch full → flush & retry
            _flush()
            batch.add(EventData(payload))
            items_in_batch = 1

    _flush()
    producer.close()
    logging.info(f"{schema}.{table}: {len(df)} rows sent to Event Hub.")

# ------------------------------------------------------------------------------
# Orchestrator for a single table
# ------------------------------------------------------------------------------
def compare_table_fdw(table_name: str, schema_local: str, schema_remote: Optional[str] = None) -> str:
    try:
        if schema_remote is None:
            schema_remote = f"{schema_local}_fdw"

        pk_cols = get_primary_key_columns(schema_local, table_name)
        col_types, col_scales = get_column_types(schema_local, table_name)
        if not pk_cols:
            pk_cols = get_unique_key_columns(db2_engine, schema_local, table_name)

        if pk_cols:
            compare_cols = get_comparable_columns(db2_engine, schema_local, table_name, pk_cols)
            if not compare_cols:
                query = build_fdw_delta_query_keys_only(schema_local, schema_remote, table_name, pk_cols, col_types)
            else:
                query = build_fdw_delta_query(schema_local, schema_remote, table_name, pk_cols, compare_cols, col_types)
        else:
            query = build_fdw_delta_query_no_key(schema_local, schema_remote, table_name, col_types)

        logging.info(f"Working on table {schema_local}.{table_name} ...")
        delta_df = pd.read_sql(query, db2_engine)

        rows = int(delta_df.shape[0])
        if rows == 0:
            logging.info(f"{schema_local}.{table_name}: No differences (0 rows)")
            return f"{schema_local}.{table_name}: No differences"
        else:
            # assume_tz obtained globally in main
            send_to_eventhub(table_name, delta_df, schema_local, col_types, col_scales, assume_tz=GLOBAL_ASSUME_TZ)
            return f"{schema_local}.{table_name}: {len(delta_df)} rows sent"

    except DBAPIError as e:
        msg = _pg_message_only(e)
        logging.error("%s.%s failed: %s", schema_local, table_name, msg)
        return f"{schema_local}.{table_name}: Error - {msg}"
    except Exception as e:
        logging.exception("Unexpected error for %s.%s", schema_local, table_name)
        return f"{schema_local}.{table_name}: Error - {e}"


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    start_time = datetime.now()

    # 1) Setup FDW etc.
    proceed = setup_fdw_and_confirm(
        db2_engine=db2_engine,
        server=fdw_servername,
        user=fdw_user,
        dbname=fdw_db,
        password=fdw_password,
        host=fdw_host,
        port=fdw_port,
    )
    if not proceed:
        logging.info("User chose not to continue. Exiting.")
        sys.exit(0)

    # 2) Get tables
    try:
        publication_names = config.get("publication_names", [])
        tables = get_user_tables(db2_engine, publication_names)

        results: Dict[Tuple[str, str], str] = {}
    except Exception as e:
        logging.error(f"Failed to fetch tables: {e}")
        sys.exit(1)

    if not tables:
        logging.info("No tables found (pg_publication_tables returned 0). Exiting.")
        sys.exit(0)

    logging.info(f"Found {len(tables)} tables to compare.")

    # 3) Obtain DB timezone for interpreting TIMESTAMP (no tz) columns
    GLOBAL_ASSUME_TZ = get_db_timezone(db2_engine)
    logging.info(f"Using DB timezone for naive timestamps: {GLOBAL_ASSUME_TZ}")

    # 4) Process in parallel
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_table = {executor.submit(compare_table_fdw, tbl, schema): (schema, tbl) for tbl, schema in tables}
        for future in as_completed(future_to_table):
            key = future_to_table[future]
            try:
                results[key] = future.result()
            except Exception as e:
                results[key] = f"Failed: {e}"

    # 5) Summary
    logging.info("Completed data move to Event Hub. Generating final report...")
    end_time = datetime.now()
    elapsed = end_time - start_time

    logging.info("\n" + "=" * 90)
    logging.info("🧾 SUMMARY OF DELTA COMPARISON EXECUTION".center(90))
    logging.info("=" * 90)
    logging.info(f"Start Time  : {start_time}")
    logging.info(f"End Time    : {end_time}")
    logging.info(f"Elapsed Time: {elapsed}")
    logging.info("-" * 90)
    for table, summary in results.items():
        logging.info(f"{table} ➤ {summary}")
    logging.info("=" * 90 + "\n")
