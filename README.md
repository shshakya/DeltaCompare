# DeltaCompare

# Solution

1. Create a PITR (**PITR1**) at the time when the replication slot is lost  
2. Create another PITR (**PITR2**) at the time when the replication slot is added back  
3. Use **Postgres FDW** and **Python scripts** to get the delta between **PITR2** & **PITR1**  
4. The script will identify **inserts**, **deletes**, and **updates** in the interval and store them in a **delta table** in a format that **Event Hub / Landing Table** can consume  
   - Insert: row exists in PITR2 but not in PITR1
   - Delete: row exists in PITR1 but not in PTR2
   - Update: row exists in both but has differences

---

# Setup

## PITR2

1. Enable **Postgres FDW extension**
   - Add POSTGRES_FDW to `azure.extensions` server parameter
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

-- OR

DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN SELECT DISTINCT schemaname FROM pg_publication_tables
  LOOP
    EXECUTE format('IMPORT FOREIGN SCHEMA %I FROM SERVER server1_fdw INTO public_fdw;', r.schemaname);
  END LOOP;
END $$;

```

3. Performance improvements:

```sql
-- Enable remote estimate
ALTER SERVER server1_fdw OPTIONS (ADD use_remote_estimate 'true');

-- For every table in public_fdw schema
ALTER FOREIGN TABLE public_fdw.<table> OPTIONS (add fetch_size '10000');
ALTER FOREIGN TABLE public_fdw.<table> OPTIONS (add batch_size '10000');

--OR

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public_fdw' AND c.relkind = 'f'
    LOOP
        EXECUTE format(
            'ALTER FOREIGN TABLE public_fdw.%I OPTIONS (add fetch_size ''10000'');',
            r.table_name
        );
    END LOOP;
END $$;

-- Do the analyze for all the tables in public_fdw schema
ANALYZE public_fdw.<table>;

-- OR Use vacuumdb client to speedup this task.

```


---

# Setup VM to run the delta script

1. Create a VM in the same region (**India Central**)
2. Install Git
```basg
sudo apt install git -y
```
3. Clone repository
4. Install dependencies:

```bash
cd DeltaCompare

# Update package list
sudo apt update

# Install Python 3 (already included in most systems, but this ensures it's there)
sudo apt install python3 -y

# Install pip (Python package manager)
sudo apt install python3-pip -y

pip3 install -r requirements.txt

```

5. Ensure the VM can connect to both PostgreSQL servers  
6. Set PostgreSQL username/passwords and eventhub connections strings in `db_config.json` file
7. Execute the script:

```bash
python3 delta_compare_direct_push.py
```

8. The script generates a log file named `datadeltalog_*` in the execution folder  
9. Once completed, delta records will be available in the `event_hub`  
