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


# Parse command-line arguments
parser = argparse.ArgumentParser(description="FDW Delta Comparison Script")
parser.add_argument("--db1", required=True, help="SQLAlchemy connection string for PITR1")
parser.add_argument("--db2", required=True, help="SQLAlchemy connection string for PITR2")
parser.add_argument("--log", required=True, help="SQLAlchemy connection string for logging DB (PITR2)")
args = parser.parse_args()

# Connect engines
db1_engine = create_engine(args.db1)
db2_engine = create_engine(args.db2)
log_engine = create_engine(args.log)

# Create a timestamped log filename
log_filename = f"datadeltalog_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_filepath = os.path.join(os.getcwd(), log_filename)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()  # Optional: also print to console
    ]
)

# 🌐 Identify where the code is running
hostname = socket.gethostname()

logging.info(f"Script started on host: {hostname}")
# 🔍 Get all user-defined tables from db1
def get_user_tables(engine):
    query = """
        SELECT tablename AS full_table_name,
        schemaname
        FROM pg_catalog.pg_tables
        WHERE (schemaname,tablename) in (select schema_name,table_name from table_list);
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
    conn = db1_engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, (f'{schema}.{table}',))

        logging.info(f"Fetching PK for: {schema}.{table}")
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()
def build_payload(table_name, before, after, op_type):
    return {
        "payload": {
            "before": before,
            "after": after,
            "source": {
                "version": "3.2.0.Final",
                "connector": "postgresql",
                "name": "PostgreSQL_server",
                "ts_ms": 0,
                "ts_us": 0,
                "ts_ns": 0,
                "snapshot": True,
                "db": "postgres",
                "sequence": "",
                "schema": "public",
                "table": table_name,
                "txId": 0,
                "lsn": 0,
                "xmin": 0
            },
            "op": op_type,
            "ts_ms": 0,
            "ts_us": 0,
            "ts_ns": 0
        }
    }
# 🪵 Log delta rows into catch-all table
def log_delta(table, source, df, max_retries=3):
    if df.empty:

        logging.info(f"Skipping log for {table} - no rows to log for {source}")
        return
    op_map = {
        'inserted': 'c',
        'updated': 'u',
        'deleted': 'd'
    }
    op_type = op_map.get(source.replace('_in_db2', ''), 'u')  # default to 'u' if unknown
    # Step 1: Write to temp TSV file
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.tsv') as temp_file:
        for _, row in df.iterrows():
            before = row.get('before')
            after = row.get('after')
            op = row.get('op')
            payload_json = build_payload(table, before, after, op)
            line = f"{table}\t{source}\t{json.dumps(payload_json)}\n"
            temp_file.write(line)
        temp_file_path = temp_file.name

    try:
        # Step 2: COPY from file into staging table
        try:
            conn = log_engine.raw_connection()
            cursor = conn.cursor()
            with open(temp_file_path, 'r') as f:
                cursor.copy_from(f, 'staging_data_deltas', sep='\t')
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

        logging.info(f"Staged {len(df)} rows for {table}.")
    finally:
        os.remove(temp_file_path)
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


def compare_table_fdw(table_name, schema_local, schema_remote='public_fdw'):
    try:
        pk_cols = get_primary_key_columns(schema_local, table_name)
        if not pk_cols:
            return f"{table_name}: Skipped (No primary key)"

        compare_cols = get_comparable_columns(db1_engine, schema_local, table_name, pk_cols)
        if not compare_cols:
            return f"{table_name}:  No comparable columns"

        query = build_fdw_delta_query(schema_local, schema_remote, table_name, pk_cols, compare_cols)

        logging.info(f"Executing dynamic FDW query for table {table_name}...")
        delta_df = pd.read_sql(query, db2_engine)

        if delta_df.empty:
            return f"{table_name}: No differences"

        # Optional: log or return rows
        log_delta(table_name, 'fdw_diff', delta_df)

        return f"{table_name}: {len(delta_df)} rows differ"

    except Exception as e:
        return f"{table_name}: Error - {e}"

# 🧵 Run comparisons in parallel
start_time = datetime.now()
tables = get_user_tables(db1_engine)
results = {}
with ThreadPoolExecutor(max_workers=9) as executor:
    future_to_table = {executor.submit(compare_table_fdw, tbl, schema): (schema, tbl) for tbl, schema in tables}
    for future in as_completed(future_to_table):
        table = future_to_table[future]
        try:
            results[table] = future.result()
        except Exception as e:
            results[table] = f" Failed with error: {e}"

# 🛠️ Final consolidated insert into data_deltas with retry
max_retries = 3
for attempt in range(1, max_retries + 1):
    try:
        with log_engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO data_deltas (table_name, source, row_data)
                SELECT DISTINCT table_name, source, row_data
                FROM staging_data_deltas
                ON CONFLICT DO NOTHING;
            """))
            connection.execute(text("TRUNCATE TABLE staging_data_deltas;"))
        logging.info(" Final consolidated insert into data_deltas completed.")
        break  # success
    except Exception as e:
        if "deadlock detected" in str(e):
            logging.warning(f" Final insert deadlock on attempt {attempt}, retrying...")
            time.sleep(10 + attempt)
        else:
            logging.error(f" Final insert failed due to: {e}")
            raise
else:
    raise RuntimeError(" Final insert failed after multiple retries.")

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
