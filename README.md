# DeltaCompare

## Overview

**DeltaCompare** generates a **change feed** (inserts, updates, deletes) between two PostgreSQL points in time—**PITR1** (older) and **PITR2** (newer)—and streams the result to **Azure Event Hubs** in a **Debezium-compatible** envelope.

Typical use case: a logical replication slot gets lost and later recreated. We compare data between the two PITRs to backfill the missed events.

---

## How It Works

1. Create a **PITR** at the time the replication slot was **lost** (**PITR1**).  
2. Create another **PITR** at the time the slot was **restored** (**PITR2**).  
3. On **PITR2**, the script:
   - Performs a **one-time FDW setup** (to connect to PITR1) using the stored procedure `public.setup_fdw_for_publications(...)`.
   - Discovers tables to compare from `pg_publication_tables` (via `publication_names` in config).
   - Builds **delta SQL** per table (keyed & keyless cases, XML-safe comparisons, chunked JSONB).
   - Streams **inserts**, **updates**, and **deletes** to **Azure Event Hub**:
     - **Insert**: Row exists in **PITR2** but not in **PITR1**
     - **Delete**: Row exists in **PITR1** but not in **PITR2**
     - **Update**: Row exists in both but differs in one or more non-key columns

---

## What’s New (Aug 2025)

- ✅ Automated FDW setup via `CALL public.setup_fdw_for_publications(...)` with a confirmation prompt
- ✅ Publication-driven table discovery (`publication_names` in `db_config.json`)
- ✅ Debezium-compatible payloads with `ts_ms`, `ts_us`, `ts_ns`, and adaptive timestamp handling
- ✅ Timezone-aware normalization (uses DB `TimeZone` for `timestamp` without time zone)
- ✅ XML-safe comparisons (casts to `text` where needed)
- ✅ Chunked `jsonb_build_object` to avoid Postgres’ 100-argument limit
- ✅ Parallel execution with configurable `max_workers`
- ✅ Direct push to Azure Event Hubs with batching
- ✅ Improved logging and final summary report

---

## Prerequisites

- **PostgreSQL** (Azure Database for PostgreSQL Flexible Server or self-managed)
  - Ability to create extensions, foreign servers, user mappings, and import foreign schemas
  - Network reachability from **PITR2** to **PITR1**
- **Azure Event Hubs** namespace and event hub
- **VM** (Linux recommended) in the **same region** as your databases (e.g., **Central India**) for low latency
- **Python 3.9+**

---

## Database Setup (Automated)

The script will execute the following stored procedure on **PITR2**:

```sql
CALL public.setup_fdw_for_publications(
  :server, :user, :dbname, :pwd, :host, :port, :p_return
);
```

**Important:**  
Before running the script, deploy the helper SQL file `sql/setup_fdw_for_publications.sql` on **PITR2**.  
This file creates the stored procedure `public.setup_fdw_for_publications` which automates:
- Creating the FDW extension
- Creating the foreign server and user mapping
- Importing foreign schemas for all tables in the specified publications

---

## VM Setup (to run the script)

1. Create a VM in the **same region** as your PostgreSQL servers (e.g., **Central India**).
2. Install Git:
   ```bash
   sudo apt update
   sudo apt install -y git
   ```
3. Clone the repository:
   ```bash
   git clone https://github.com/shshakya/DeltaCompare.git
   cd DeltaCompare
   ```
4. Install Python and dependencies:
   ```bash
   sudo apt install -y python3 python3-pip
   pip3 install -r requirements.txt
   ```
5. Ensure the VM can reach **both** PostgreSQL instances (PITR1 and PITR2).  
   - Open firewalls.
   - Add private endpoints or allowlisted IPs as needed.
   - For Azure DB, ensure `sslmode=require` in your connection strings.
6. Create and populate `db_config.json`.

---

## Configuration

Create `db_config.json` in the repo root:

```json
{
  "db1": "postgresql+psycopg2://<user>:<password>@<pitr1-host>:5432/<db>?sslmode=require",
  "db2": "postgresql+psycopg2://<user>:<password>@<pitr2-host>:5432/<db>?sslmode=require",

  "event_hub_connection_str": "Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<name>;SharedAccessKey=<key>",
  "event_hub_name": "<event-hub-name>",

  "fdw_servername": "server1_fdw",
  "fdw_user": "<pitr1-user>",
  "fdw_db": "<db>",
  "fdw_password": "<pitr1-password>",
  "fdw_host": "<pitr1-host>",
  "fdw_port": "5432",

  "publication_names": ["pub_orders", "pub_customers"],

  "max_workers": 12,

  "static_timestamp": "2025-08-22T14:00:00Z"
}
```

---

## Run

```bash
python3 delta_compare_direct_push.py
```

- The script:
  1. Performs FDW setup via stored procedure (and asks to continue).
  2. Fetches tables from `pg_publication_tables` for the publications you listed.
  3. Detects DB `TimeZone` to interpret naive `timestamp` values correctly.
  4. Runs comparisons in parallel (threads = `max_workers`).
  5. Pushes deltas to Event Hub (batched).
  6. Writes logs to `datadeltalog_YYYYMMDD_HHMMSS.log`.

---

## Output Format (Event Hub Payload)

Each record is a Debezium-like envelope:

```json
{
  "payload": {
    "before": { /* normalized row for 'd' or 'u' */ },
    "after":  { /* normalized row for 'c' or 'u' */ },
    "ts_ms":  1724335200000,
    "ts_us":  "1724335200000000",
    "ts_ns":  "1724335200000000000",
    "source": {
      "ts_ms": 1724335200000,
      "ts_us": "1724335200000000",
      "ts_ns": "1724335200000000000",
      "version": "3.2.0.Final",
      "connector": "postgresql",
      "name": "PostgreSQL_server",
      "db": "<db>",
      "schema": "<schema>",
      "table": "<table>"
    },
    "op": "c" | "u" | "d"
  }
}
```

---

## Performance & Sizing

- **Threads**: Start with `max_workers = 8–16`.
- **VM**: 16 vCPUs / 64 GB RAM (Azure `D16s_v5` or `E16ds_v5`).
- **Network**: Same region, 10 Gbps NIC.
- **FDW options**: `use_remote_estimate=true`, `fetch_size=batch_size=10000`.
- **Stats**: `ANALYZE` FDW schemas after import.

---

## Troubleshooting

- **FDW setup failed**: Ensure `public.setup_fdw_for_publications` exists on PITR2 and privileges are correct.
- **Permission errors**: Use admin role or equivalent.
- **Memory pressure**: Lower `max_workers` or split large tables.
- **Event Hub throttling**: Increase partitions / throughput units.

---

## Checklist

- [ ] PITR1 & PITR2 created  
- [ ] VM to PostgreSQL & EventHub connectivity verified  
- [ ] `setup_fdw_for_publications.sql` deployed on PITR2  
- [ ] `db_config.json` configured  
- [ ] Dependencies installed  
- [ ] Script executed successfully  

---
```
