# DeltaCompare

# Solution

1. Create a PITR (**PITR1**) at the time when the replication slot is lost  
2. Create another PITR (**PITR2**) at the time when the replication slot is added back  
3. Use **Postgres FDW** and **Python scripts** to get the delta between **PITR2** & **PITR1**  
4. The script will identify **inserts**, **deletes**, and **updates** in the interval and store them in a **delta table** in a format that **Event Hub / Landing Table** can consume  

---

# Setup

## PITR2

1. Enable **Postgres FDW extension**  
2. Execute the following scripts:

```sql
-- Create schema
CREATE SCHEMA public_fdw;

-- Enable FDW extension
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create foreign server
CREATE SERVER server1_fdw FOREIGN DATA WRAPPER postgres_fdw 
OPTIONS (host '<pitr1>.postgres.database.azure.com', port '5432', dbname '<dbname>');

-- Create user mapping
CREATE USER MAPPING FOR CURRENT_USER SERVER server1_fdw 
OPTIONS (user '<username>', password '<password>');

-- Import foreign schema (repeat for all PITR1 schemas)
IMPORT FOREIGN SCHEMA <schema> FROM SERVER server1_fdw INTO public_fdw;
```

3. Performance improvements:

```sql
-- Enable remote estimate
ALTER SERVER server1_fdw OPTIONS (ADD use_remote_estimate 'true');

-- For every table in public_fdw schema
ALTER FOREIGN TABLE public_fdw.<table> OPTIONS (add fetch_size '10000');
ALTER FOREIGN TABLE public_fdw.<table> OPTIONS (add batch_size '10000');
ANALYZE public_fdw.<table>;
```

4. Create additional tables:

```sql
-- Temporary staging table
CREATE UNLOGGED TABLE IF NOT EXISTS staging_data_deltas (
  table_name text,
  source text,
  row_data jsonb
);

-- Final delta table
CREATE TABLE public.data_deltas (
  table_name text NULL,
  source text NULL,
  row_data jsonb NULL,
  recorded_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uniq_delta UNIQUE (table_name, source, row_data)
);
```

---

## PITR1

Create a table to list schema and table names:

```sql
CREATE TABLE table_list (
  schema_name text,
  table_name text
);
```

---

# Script

**<GitHub repository>**

1. Create a VM in the same region (**India Central**)  
2. Install dependencies:

```bash
sudo apt update
sudo apt install git -y
sudo apt install python3 --version
sudo apt install python3-psycopg2-binary
sudo apt install python3-pandas
sudo apt install python3-sqlalchemy
```

3. Ensure the VM can connect to both PostgreSQL servers  
4. Move code from GitHub repo to VM  
5. Execute the script:

```bash
python3 data_delta.py \
  --db1 "postgresql://<username>:<Password>@<pitr1>:5432/db1" \
  --db2 "postgresql://<username>:<Password>@<pitr2>:5432/db1" \
  --log "postgresql://<username>:<Password>@<pitr2>:5432/db1"
```

### 🔐 Note on Password Encoding

If your password contains special characters, use URL encoding:

| Character | Encode As |
|-----------|------------|
| `@`       | `%40`      |
| `$`       | `%24`      |
| `/`       | `%2F`      |
| `:`       | `%3A`      |

Use Python’s `urllib.parse.quote_plus()` or an online encoder.

---

6. The script generates a log file named `datadeltalog_*` in the execution folder  
7. Once completed, delta records will be available in the `data_deltas` table  
