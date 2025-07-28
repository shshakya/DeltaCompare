import json
import pandas as pd
from sqlalchemy import create_engine, text
from azure.eventhub import EventData, EventHubProducerClient
import logging
import socket
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load configuration
with open('db_config.json') as config_file:
    config = json.load(config_file)

db1_engine = create_engine(config['db1'])
db2_engine = create_engine(config['db2'])
EVENT_HUB_CONNECTION_STR = config['event_hub_connection_str']
EVENT_HUB_NAME = config['event_hub_name']

# Logging setup
log_filename = f"datadeltalog_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])
logging.info(f"Script started on host: {socket.gethostname()}")

def build_payload(table_name, before, after, op_type):
    return {
        "payload": {
            "before": before,
            "after": after,
            "source": {
                "version": "3.2.0.Final",
                "connector": "postgresql",
                "name": "PostgreSQL_server",
                "db": "postgres",
                "schema": "public",
                "table": table_name
            },
            "op": op_type
        }
    }

def send_to_eventhub(table, df):
    if df.empty:
        logging.info(f"No changes for {table}")
        return

    producer = EventHubProducerClient.from_connection_string(EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME)
    batch = producer.create_batch()

    for _, row in df.iterrows():
        payload = json.dumps(build_payload(table, row.get('before'), row.get('after'), row.get('op')))
        try:
            batch.add(EventData(payload))
        except ValueError:
            producer.send_batch(batch)
            batch = producer.create_batch()
            batch.add(EventData(payload))

    if len(batch) > 0:
        producer.send_batch(batch)
    producer.close()
    logging.info(f"Sent {len(df)} rows for {table} to Event Hub.")

def get_user_tables(engine):
    query = """
        SELECT tablename AS full_table_name, schemaname
        FROM pg_catalog.pg_tables
        WHERE (schemaname,tablename) in (select schema_name,table_name from table_list);
    """
    df = pd.read_sql(query, engine)
    return list(zip(df['full_table_name'], df['schemaname']))

def get_primary_key_columns(schema, table):
    query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary;
    """
    conn = db1_engine.raw_connection()
    cur = conn.cursor()
    cur.execute(query, (f'{schema}.{table}',))
    pk_cols = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return pk_cols

def get_comparable_columns(engine, schema, table, pk_cols):
    placeholders = ', '.join([f":pk_{i}" for i in range(len(pk_cols))])
    sql = text(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        AND column_name NOT IN ({placeholders})
    """)
    params = {'schema': schema, 'table': table}
    params.update({f"pk_{i}": col for i, col in enumerate(pk_cols)})
    with engine.connect() as conn:
        result = conn.execute(sql, params).mappings().all()
        return [row['column_name'] for row in result]

def build_fdw_delta_query(schema_local, schema_remote, table_name, pk_cols, compare_cols):
    all_cols = pk_cols + compare_cols
    json_local = ", ".join([f"'{col}', l.{col}" for col in all_cols])
    json_remote = ", ".join([f"'{col}', r.{col}" for col in all_cols])
    pk_join = " AND ".join([f"l.{col} = r.{col}" for col in pk_cols])
    is_diff = " OR ".join([f"l.{col} IS DISTINCT FROM r.{col}" for col in compare_cols])
    is_null_remote = " AND ".join([f"r.{col} IS NULL" for col in pk_cols])
    is_null_local = " AND ".join([f"l.{col} IS NULL" for col in pk_cols])

    return f"""
        SELECT op, before, after FROM (
            SELECT 'u' AS op,
                   jsonb_build_object({json_remote}) AS before,
                   jsonb_build_object({json_local}) AS after
            FROM {schema_remote}.{table_name} r
            JOIN {schema_local}.{table_name} l ON {pk_join}
            WHERE {is_diff}
            UNION ALL
            SELECT 'c' AS op, NULL AS before,
                   jsonb_build_object({json_local}) AS after
            FROM {schema_local}.{table_name} l
            LEFT JOIN {schema_remote}.{table_name} r ON {pk_join}
            WHERE {is_null_remote}
            UNION ALL
            SELECT 'd' AS op,
                   jsonb_build_object({json_remote}) AS before,
                   NULL AS after
            FROM {schema_remote}.{table_name} r
            LEFT JOIN {schema_local}.{table_name} l ON {pk_join}
            WHERE {is_null_local}
        ) AS delta
    """

def compare_table_fdw(table_name, schema_local, schema_remote='public_fdw'):
    try:
        pk_cols = get_primary_key_columns(schema_local, table_name)
        if not pk_cols:
            return f"{table_name}: Skipped (No PK)"
        compare_cols = get_comparable_columns(db1_engine, schema_local, table_name, pk_cols)
        if not compare_cols:
            return f"{table_name}: Skipped (No comparable columns)"
        query = build_fdw_delta_query(schema_local, schema_remote, table_name, pk_cols, compare_cols)
        delta_df = pd.read_sql(query, db2_engine)
        send_to_eventhub(table_name, delta_df)
        return f"{table_name}: {len(delta_df)} rows sent"
    except Exception as e:
        return f"{table_name}: Error - {e}"

# Run comparisons
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
            results[table] = f"Failed: {e}"

# Summary
end_time = datetime.now()
logging.info("="*90)
logging.info("🧾 SUMMARY OF DELTA COMPARISON EXECUTION".center(90))
logging.info("="*90)
logging.info(f"Start Time  : {start_time}")
logging.info(f"End Time    : {end_time}")
logging.info(f"Elapsed Time: {end_time - start_time}")
for table, summary in results.items():
    logging.info(f"{table} ➤ {summary}")
logging.info("="*90 + "\n")
