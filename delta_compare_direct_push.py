import psycopg2
from sqlalchemy import create_engine, text
import pandas as pd
from psycopg2.extras import Json
from concurrent.futures import ThreadPoolExecutor, as_completed
import socket
from datetime import datetime
import json
from sqlalchemy.dialects.postgresql import JSON as Json
import os
import tempfile
import time
import logging
from datetime import datetime
import argparse
import sys
from azure.eventhub import EventData, EventHubProducerClient
from typing import Dict, List
from sqlalchemy.exc import DBAPIError

# Load configuration
with open('db_config.json') as config_file:
    config = json.load(config_file)

db1_engine = create_engine(config['db1'] , pool_size=20,        # >= threads (16) + small buffer
max_overflow=16,     # burst allowance ≈ threads
    pool_timeout=60,     # wait longer before raising
    pool_recycle=1800,   # drop stale conns periodically (secs)
    pool_pre_ping=True,  # validate conn before handing out
    pool_use_lifo=True,  # reduces head-of-line blocking
)
db2_engine = create_engine(config['db2'], pool_size=20,        # >= threads (16) + small buffer
max_overflow=16,     # burst allowance ≈ threads
    pool_timeout=60,     # wait longer before raising
    pool_recycle=1800,   # drop stale conns periodically (secs)
    pool_pre_ping=True,  # validate conn before handing out
    pool_use_lifo=True,  # reduces head-of-line blocking
)
EVENT_HUB_CONNECTION_STR = config['event_hub_connection_str']
EVENT_HUB_NAME = config['event_hub_name']
fdw_servername = config['fdw_servername']
fdw_user = config['fdw_user']
fdw_db = config['fdw_db']
fdw_password = config['fdw_password']
fdw_host = config['fdw_host']
fdw_port = config['fdw_port']
max_threads = config['max_workers']



# Logging setup
log_filename = f"datadeltalog_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])
logging.info(f"Script started on host: {socket.gethostname()}")

# Suppress verbose Azure SDK logs
logging.getLogger("azure").setLevel(logging.WARNING)

def _pg_message_only(err: Exception) -> str:
    # Handles SQLAlchemy-wrapped psycopg2 exceptions and plain exceptions
    if isinstance(err, DBAPIError) and getattr(err, "orig", None) is not None:
        pg = err.orig  # psycopg2 error
        primary = getattr(getattr(pg, "diag", None), "message_primary", None)
        return primary or getattr(pg, "pgerror", None) or str(pg)
    return str(err)
def setup_fdw_and_confirm(db2_engine, server, user, dbname, password, host, port,
                          prompt="FDW setup complete. Continue with delta comparison? (yes/no): "):
    """
    Calls: CALL public.setup_fdw_for_publications(:server,:user,:dbname,:pwd,:host,:port)
    on db2_engine, then asks the user to confirm continuation.
    Returns True if user answers yes/y, else False.
    """
    try:
        with db2_engine.begin() as conn:
            logging.info("Setting up the schema for creating the delta...")
            result = conn.execute(
                text("""
                    CALL public.setup_fdw_for_publications(
                        :server, :user, :dbname, :pwd, :host, :port , :p_return
                    )
                """),
                {
                    "server": server,
                    "user":   user,
                    "dbname": dbname,
                    "pwd":    password,
                    "host":   host,
                    "port":   port,
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



# 🔍 Get all user-defined tables from db1
def get_user_tables(engine):
    query = """
        SELECT tablename AS full_table_name,
        schemaname
        FROM pg_catalog.pg_tables
        WHERE (schemaname,tablename) in (select schemaname, tablename from pg_publication_tables);
    """
    df = pd.read_sql(query, engine)
    return list(zip(df['full_table_name'], df['schemaname']))

# 🔑 Get primary key columns safely
def get_primary_key_columns(schema, table):
    query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary;
    """
    conn = db2_engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, (f'{schema}.{table}',))

        #logging.info(f"Fetching PK for: {schema}.{table}")
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

def get_column_types(schema: str, table: str) -> Dict[str, str]:
    """
    Return a mapping {column_name: typname} for a given table.
    Example typnames: 'text', 'xml', 'jsonb', 'int4', '_int4' (arrays), etc.
    """
    type_sql = """
        SELECT a.attname AS col, t.typname AS typ
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
            cur.execute(type_sql, (schema, table))
            rows = cur.fetchall()
        # e.g., {'payload': 'jsonb', 'doc': 'xml', ...}
        return {col: typ for col, typ in rows}
    finally:
        cur.close()
        conn.close()
def get_unique_key_columns(engine, schema, table):
    """
    Prefer (in order):
      1) UNIQUE index that is the REPLICA IDENTITY
      2) UNIQUE index whose key columns are all NOT NULL
      3) Smallest UNIQUE index (fewest columns)
    Still excludes partial/expr/invalid and non-PK.
    """
    sql = text("""
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
            AND i.indpred IS NULL         -- not partial
            AND i.indexprs IS NULL        -- no expressions
          GROUP BY i.indexrelid, i.indnatts, i.indisreplident
        )
        SELECT cols
        FROM u
        -- prefer replica identity, then all NOT NULL, then narrowest, then oldest OID
        ORDER BY is_replica_ident DESC,
                 all_not_null    DESC,
                 nkeys           ASC,
                 indexrelid      ASC
        LIMIT 1;
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"schema": schema, "table": table}).first()
        return list(row[0]) if row and row[0] else []



def build_payload(table_name, before, after, op_type,schema):
    return {
        "payload": {
            "before": before,
            "after": after,
            "source": {
                "version": "3.2.0.Final",
                "connector": "postgresql",
                "name": "PostgreSQL_server",
                "db": fdw_db,
                "schema": schema,
                "table": table_name
            },
            "op": op_type
        }
    }

def send_to_eventhub(table, df,schema):
    if df.empty:
        logging.info(f"No changes for {table}")
        return
    #print(f"Initiating event hub connection for {schema}.{table}")
    producer = EventHubProducerClient.from_connection_string(EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME)
    batch = producer.create_batch()
    #print(f"connection initialized for {schema}.{table}")
    for _, row in df.iterrows():
        payload = json.dumps(build_payload(table, row.get('before'), row.get('after'), row.get('op'),schema))

        # 🧪 Print payload for testing
        #print(f"\n--- Payload for table '{table}' ---\n{payload}\n")

        try:
            batch.add(EventData(payload))
        except ValueError:
            producer.send_batch(batch)
            batch = producer.create_batch()
            batch.add(EventData(payload))

    if len(batch) > 0:
        producer.send_batch(batch)
    producer.close()
    logging.info(f"{schema}.{table}:{len(df)} rows sent to Event Hub.")

def get_comparable_columns(engine, schema, table, pk_cols):
    if not pk_cols:
        return []

    # Create placeholders like :pk_0, :pk_1, ...
    placeholders = ', '.join([f":pk_{i}" for i in range(len(pk_cols))])
    sql = text(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
          AND column_name NOT IN ({placeholders})
    """)

    # Build params dict
    params = {'schema': schema, 'table': table}
    params.update({f"pk_{i}": col for i, col in enumerate(pk_cols)})

    with engine.connect() as conn:
        result = conn.execute(sql, params).mappings().all()  # row is a dict now
        return [row['column_name'] for row in result]

def build_fdw_delta_query_keys_only(schema_local: str,
                                          schema_remote: str,
                                          table_name: str,
                                          key_cols: List[str],
                                          col_types: Dict[str, str]) -> str:
    def _qident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def _value_expr(alias: str, col: str, typname: str) -> str:
        base = f"{alias}.{_qident(col)}"
        # Cast XML to text for safe JSON and comparisons (if any)
        if typname == "xml":
            return f"{base}::text"
        return base

    def _values_block(alias: str, cols: List[str], typemap: Dict[str, str]) -> str:
        """
        Build: VALUES ('col', to_jsonb(alias.col_expr)), ... using type-aware value_expr.
        (text, jsonb) column types so jsonb_object_agg(k,v) works.
        """
        if not cols:
            return "VALUES ('__empty__', to_jsonb(NULL))"
        pairs = [f"('{c}', to_jsonb({_value_expr(alias, c, typemap.get(c, 'text'))}))" for c in cols]
        return "VALUES " + ", ".join(pairs)

    # JSON columns = keys + all non-key columns from the typemap (assumes same on both sides)
    non_keys = [c for c in col_types.keys() if c not in key_cols]
    json_cols = key_cols + non_keys

    values_local = _values_block("l", json_cols, col_types)
    values_remote = _values_block("r", json_cols, col_types)

    obj_local = f"(SELECT jsonb_object_agg(k, v) FROM ({values_local})  AS kv(k, v))"
    obj_remote = f"(SELECT jsonb_object_agg(k, v) FROM ({values_remote}) AS kv(k, v))"

    pk_join = " AND ".join([f"l.{_qident(c)} = r.{_qident(c)}" for c in key_cols]) or "FALSE"
    is_null_remote = " AND ".join([f"r.{_qident(c)} IS NULL" for c in key_cols]) or "FALSE"
    is_null_local = " AND ".join([f"l.{_qident(c)} IS NULL" for c in key_cols]) or "FALSE"

    return f"""
            -- INSERTS (local only)
            SELECT 'c'::text AS op,
                   NULL::jsonb AS before,
                   {obj_local} AS after
            FROM {_qident(schema_local)}.{_qident(table_name)} l
            LEFT JOIN {_qident(schema_remote)}.{_qident(table_name)} r ON {pk_join}
            WHERE {is_null_remote}

            UNION ALL

            -- DELETES (remote only)
            SELECT 'd'::text AS op,
                   {obj_remote} AS before,
                   NULL::jsonb AS after
            FROM {_qident(schema_remote)}.{_qident(table_name)} r
            LEFT JOIN {_qident(schema_local)}.{_qident(table_name)} l ON {pk_join}
            WHERE {is_null_local}
        """
    return query

def build_fdw_delta_query_no_key(schema_local: str,
                                       schema_remote: str,
                                       table_name: str,
                                       col_types: Dict[str, str]) -> str:

    def _qident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def _value_expr(alias: str, col: str, typname: str) -> str:
        base = f"{alias}.{_qident(col)}"
        # Cast XML to text for safe JSON and comparisons (if any)
        if typname == "xml":
            return f"{base}::text"
        return base

    def _values_block(alias: str, cols: List[str], typemap: Dict[str, str]) -> str:
        """
        Build: VALUES ('col', to_jsonb(alias.col_expr)), ... using type-aware value_expr.
        (text, jsonb) column types so jsonb_object_agg(k,v) works.
        """
        if not cols:
            return "VALUES ('__empty__', to_jsonb(NULL))"
        pairs = [f"('{c}', to_jsonb({_value_expr(alias, c, typemap.get(c, 'text'))}))" for c in cols]
        return "VALUES " + ", ".join(pairs)

    all_cols = list(col_types.keys())

    values_local = _values_block("l", all_cols, col_types)
    values_remote = _values_block("r", all_cols, col_types)

    obj_local = f"(SELECT jsonb_object_agg(k, v) FROM ({values_local})  AS kv(k, v))"
    obj_remote = f"(SELECT jsonb_object_agg(k, v) FROM ({values_remote}) AS kv(k, v))"

    return f"""
                -- INSERTS (local rows not in remote)
                SELECT 'c'::text AS op,
                       NULL::jsonb AS before,
                       {obj_local} AS after
                FROM {_qident(schema_local)}.{_qident(table_name)} l
                EXCEPT ALL
                SELECT 'c'::text AS op,
                       NULL::jsonb AS before,
                       {obj_remote} AS after
                FROM {_qident(schema_remote)}.{_qident(table_name)} r

                UNION ALL

                -- DELETES (remote rows not in local)
                SELECT 'd'::text AS op,
                       {obj_remote} AS before,
                       NULL::jsonb AS after
                FROM {_qident(schema_remote)}.{_qident(table_name)} r
                EXCEPT ALL
                SELECT 'd'::text AS op,
                       {obj_local} AS before,
                       NULL::jsonb AS after
                FROM {_qident(schema_local)}.{_qident(table_name)} l
            """
    return query

def build_fdw_delta_query(schema_local: str,schema_remote: str,table_name: str,pk_cols: List[str],compare_cols: List[str],col_types: Dict[str, str]) -> str:
    def qident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def value_expr(alias: str, col: str) -> str:
        """Type-aware value expression for comparisons/JSON."""
        base = f"{alias}.{qident(col)}"
        typ = col_types.get(col)
        if typ == "xml":
            return f"{base}::text"
        return base

    # --- JSON assembly via VALUES(...) -> jsonb_object_agg ---
    def values_block(alias: str, cols: List[str]) -> str:
        """
        Build: VALUES ('col1', to_jsonb(alias.col1)), ...
        Uses type-aware value_expr for proper casting (e.g., xml::text).
        """
        if not cols:
            return "VALUES ('__empty__', to_jsonb(NULL))"
        pairs = [f"('{c}', to_jsonb({value_expr(alias, c)}))" for c in cols]
        return "VALUES " + ", ".join(pairs)

    # include both PKs and compare_cols in JSON output (tweak if you want only compare_cols)
    json_cols = pk_cols + compare_cols

    values_local  = values_block("l", json_cols)
    values_remote = values_block("r", json_cols)

    obj_local  = f"(SELECT jsonb_object_agg(k, v) FROM ({values_local})  AS kv(k, v))"
    obj_remote = f"(SELECT jsonb_object_agg(k, v) FROM ({values_remote}) AS kv(k, v))"

    # --- joins & predicates ---
    pk_join = " AND ".join([f"l.{qident(c)} = r.{qident(c)}" for c in pk_cols]) or "FALSE"

    # Type-aware IS DISTINCT FROM for compare_cols
    is_diff_parts = [f"{value_expr('l', c)} IS DISTINCT FROM {value_expr('r', c)}" for c in compare_cols]
    is_diff = " OR ".join(is_diff_parts) if is_diff_parts else "FALSE"

    is_null_remote = " AND ".join([f"r.{qident(c)} IS NULL" for c in pk_cols]) or "FALSE"
    is_null_local  = " AND ".join([f"l.{qident(c)} IS NULL" for c in pk_cols]) or "FALSE"

    # --- final SQL ---
    return f"""
        SELECT
          op,
          before,
          after
        FROM (
          -- updated
          SELECT
            'u' AS op,
            {obj_remote} AS before,
            {obj_local}  AS after
          FROM {qident(schema_remote)}.{qident(table_name)} r
          JOIN {qident(schema_local)}.{qident(table_name)} l ON {pk_join}
          WHERE {is_diff}

          UNION ALL

          -- inserted (present in local, missing in remote)
          SELECT
            'c' AS op,
            NULL AS before,
            {obj_local} AS after
          FROM {qident(schema_local)}.{qident(table_name)} l
          LEFT JOIN {qident(schema_remote)}.{qident(table_name)} r ON {pk_join}
          WHERE {is_null_remote}

          UNION ALL

          -- deleted (present in remote, missing in local)
          SELECT
            'd' AS op,
            {obj_remote} AS before,
            NULL AS after
          FROM {qident(schema_remote)}.{qident(table_name)} r
          LEFT JOIN {qident(schema_local)}.{qident(table_name)} l ON {pk_join}
          WHERE {is_null_local}
        ) AS delta;
    """
    return query


def compare_table_fdw(table_name, schema_local, schema_remote=None):
    try:

        if schema_remote is None:
            schema_remote = f"{schema_local}_fdw"

        pk_cols = get_primary_key_columns(schema_local, table_name)
        col_types = get_column_types(schema_local, table_name)
        if not pk_cols:

            pk_cols = get_unique_key_columns(db2_engine, schema_local, table_name)

        if  pk_cols:
            compare_cols = get_comparable_columns(db2_engine, schema_local, table_name, pk_cols)
            if not compare_cols:
                # keys-only table -> c/d only on that key
                query = build_fdw_delta_query_keys_only(schema_local, schema_remote, table_name, pk_cols,col_types)
            else:
                # full u/c/d using the unique key / primary key
                query = build_fdw_delta_query(schema_local, schema_remote, table_name, pk_cols, compare_cols,col_types)
        else:
            # truly no key -> c/d only via full-row multiset diff
            query = build_fdw_delta_query_no_key(schema_local, schema_remote, table_name,col_types)

        logging.info(f"Working on table {schema_local}.{table_name}...")
        #logging.info(f"query on table {query}...")
        delta_df = pd.read_sql(query, db2_engine)

        rows = int(delta_df.shape[0])
        if rows == 0:
            logging.info(f"{schema_local}.{table_name}: No differences (0 rows)")
            return f"{schema_local}.{table_name}: No differences"
        else:
            send_to_eventhub(table_name, delta_df,schema_local)
            return f"{schema_local}.{table_name}: {len(delta_df)} rows sent"

    except DBAPIError as e:
        msg = _pg_message_only(e)
        logging.error("%s.%s failed: %s", schema_local, table_name, msg)
        return f"{schema_local}.{table_name}: Error - {msg}"
    except Exception as e:
        return f"{table_name}: Error - {e}"

# 🧵 Run comparisons in parallel
start_time = datetime.now()

# Call the proc and pause
proceed = setup_fdw_and_confirm(
    db2_engine=db2_engine,
    server=fdw_servername,
    user=fdw_user,
    dbname=fdw_db,
    password=fdw_password,
    host=fdw_host,
    port=fdw_port
)
if not proceed:
    logging.info("User chose not to continue. Exiting.")
    sys.exit(0)

#tables = get_user_tables(db2_engine)
#results = {}
try:
    tables = get_user_tables(db2_engine)
    results = {}
except Exception as e:
    logging.error(f"Failed to fetch tables: {e}")
    sys.exit(1)

# Stop if none
if not tables:  # handles [] from an empty DataFrame
    logging.info("No tables found (pg_publication_tables returned 0). Exiting.")
    sys.exit(0)
logging.info(f"Found {len(tables)} tables to compare.")

with ThreadPoolExecutor(max_workers=max_threads) as executor:
    future_to_table = {executor.submit(compare_table_fdw, tbl, schema): (schema, tbl) for tbl, schema in tables}
    for future in as_completed(future_to_table):
        table = future_to_table[future]
        try:
            results[table] = future.result()
        except Exception as e:
            results[table] = f" Failed: {e}"

logging.info("Completed data move to event hub. Generating final report...")
# Summary
logging.info("\n" + "="*90)
logging.info("🧾 SUMMARY OF DELTA COMPARISON EXECUTION".center(90))
logging.info("="*90)

# Execution Time Info
end_time = datetime.now()
elapsed = end_time - start_time
logging.info(f"Start Time  : {start_time}")
logging.info(f"End Time    : {end_time}")
logging.info(f"Elapsed Time: {elapsed}")
logging.info("-" * 90)

# Per-Table Results
for table, summary in results.items():
    logging.info(f"{table} ➤ {summary}")

logging.info("="*90 + "\n")
