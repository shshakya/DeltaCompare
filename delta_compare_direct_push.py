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

# Load configuration
with open('db_config.json') as config_file:
    config = json.load(config_file)

db1_engine = create_engine(config['db1'])
db2_engine = create_engine(config['db2'])
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

    producer = EventHubProducerClient.from_connection_string(EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME)
    batch = producer.create_batch()

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

def build_fdw_delta_query_keys_only(schema_local, schema_remote, table_name, key_cols):
    pk_join = " AND ".join([f"l.{c} = r.{c}" for c in key_cols])
    is_null_remote = " AND ".join([f"r.{c} IS NULL" for c in key_cols])
    is_null_local  = " AND ".join([f"l.{c} IS NULL" for c in key_cols])

    return f"""
        -- INSERTS (local only)
        SELECT 'c'::text AS op,
               NULL::jsonb AS before,
               to_jsonb(l) AS after
        FROM {schema_local}.{table_name} l
        LEFT JOIN {schema_remote}.{table_name} r ON {pk_join}
        WHERE {is_null_remote}

        UNION ALL

        -- DELETES (remote only)
        SELECT 'd'::text AS op,
               to_jsonb(r) AS before,
               NULL::jsonb AS after
        FROM {schema_remote}.{table_name} r
        LEFT JOIN {schema_local}.{table_name} l ON {pk_join}
        WHERE {is_null_local}
    """

def build_fdw_delta_query_no_key(schema_local, schema_remote, table_name):
    """
    No PK → full-row multiset difference. Emits only:
      'c' = present locally not remotely
      'd' = present remotely not locally
    Ensure NULLs are typed as jsonb to satisfy UNION type matching.
    """
    return f"""
        -- INSERTS (local rows not in remote)
        SELECT 'c'::text AS op,
               NULL::jsonb AS before,
               to_jsonb(l) AS after
        FROM {schema_local}.{table_name} l
        EXCEPT ALL
        SELECT 'c'::text AS op,
               NULL::jsonb AS before,
               to_jsonb(r) AS after
        FROM {schema_remote}.{table_name} r

        UNION ALL

        -- DELETES (remote rows not in local)
        SELECT 'd'::text AS op,
               to_jsonb(r) AS before,
               NULL::jsonb AS after
        FROM {schema_remote}.{table_name} r
        EXCEPT ALL
        SELECT 'd'::text AS op,
               to_jsonb(l) AS before,
               NULL::jsonb AS after
        FROM {schema_local}.{table_name} l
    """

def build_fdw_delta_query(schema_local, schema_remote, table_name, pk_cols, compare_cols):
    all_cols = pk_cols + compare_cols
    json_build_local = ", ".join([f"'{col}', l.{col}" for col in all_cols])
    json_build_remote = ", ".join([f"'{col}', r.{col}" for col in all_cols])
    pk_join = " AND ".join([f"l.{col} = r.{col}" for col in pk_cols])
    is_diff = " OR ".join([f"l.{col} IS DISTINCT FROM r.{col}" for col in compare_cols])
    is_null_remote = " AND ".join([f"r.{col} IS NULL" for col in pk_cols])
    is_null_local = " AND ".join([f"l.{col} IS NULL" for col in pk_cols])

    return f"""
            SELECT
              op,
              before,
              after
            FROM (
              -- updated
              SELECT
                'u' AS op,
                jsonb_build_object({json_build_remote}) AS before,
                jsonb_build_object({json_build_local}) AS after
              FROM {schema_remote}.{table_name} r
              JOIN {schema_local}.{table_name} l ON {pk_join}
              WHERE {is_diff}

              UNION ALL

              -- inserted
              SELECT
                'c' AS op,
                NULL AS before,
                jsonb_build_object({json_build_local}) AS after
              FROM {schema_local}.{table_name} l
              LEFT JOIN {schema_remote}.{table_name} r ON {pk_join}
              WHERE {is_null_remote}

              UNION ALL

              -- deleted
              SELECT
                'd' AS op,
                jsonb_build_object({json_build_remote}) AS before,
                NULL AS after
              FROM {schema_remote}.{table_name} r
              LEFT JOIN {schema_local}.{table_name} l ON {pk_join}
              WHERE {is_null_local}
            ) AS delta
        """
    return query


def compare_table_fdw(table_name, schema_local, schema_remote=None):
    try:

        if schema_remote is None:
            schema_remote = f"{schema_local}_fdw"

        pk_cols = get_primary_key_columns(schema_local, table_name)
        if not pk_cols:

            pk_cols = get_unique_key_columns(db2_engine, schema_local, table_name)

        if  pk_cols:
            compare_cols = get_comparable_columns(db2_engine, schema_local, table_name, pk_cols)
            if not compare_cols:
                # keys-only table -> c/d only on that key
                query = build_fdw_delta_query_keys_only(schema_local, schema_remote, table_name, pk_cols)
            else:
                # full u/c/d using the unique key / primary key
                query = build_fdw_delta_query(schema_local, schema_remote, table_name, pk_cols, compare_cols)
        else:
            # truly no key -> c/d only via full-row multiset diff
            query = build_fdw_delta_query_no_key(schema_local, schema_remote, table_name)

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
