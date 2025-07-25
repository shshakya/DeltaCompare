#!/bin/bash

# -------------------------------
# PostgreSQL FDW Setup Script
# -------------------------------

# Configuration
PITR2_HOST="your_pitr2_host"
PITR1_HOST="your_pitr1_host"
USERNAME="your_username"
PASSWORD="your_password"
DBNAME="your_dbname"
FDW_SCHEMA="public_fdw"
LOCAL_SCHEMA="public_fdw"
REMOTE_SCHEMA="your_remote_schema"
FDW_SERVER="server1_fdw"
PUBLICATIONS=("pub1" "pub2" "pub3" "pub4" "pub5")

# Step 1: Create schema and enable FDW extension
psql -h $PITR2_HOST -U $USERNAME -d $DBNAME <<EOF
CREATE SCHEMA IF NOT EXISTS $FDW_SCHEMA;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
EOF

# Step 2: Create foreign server and user mapping
psql -h $PITR2_HOST -U $USERNAME -d $DBNAME <<EOF
CREATE SERVER IF NOT EXISTS $FDW_SERVER FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host '$PITR1_HOST', port '5432', dbname '$DBNAME');

CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER $FDW_SERVER
OPTIONS (user '$USERNAME', password '$PASSWORD');

ALTER SERVER $FDW_SERVER OPTIONS (ADD use_remote_estimate 'true');
EOF

# Step 3: Loop through each publication and import tables
for PUB in "${PUBLICATIONS[@]}"; do
  echo "Processing publication: $PUB"

  # Get table list from publication
  TABLE_LIST=$(psql -h $PITR1_HOST -U $USERNAME -d $DBNAME -Atc \
    "SELECT tablename FROM pg_publication_tables WHERE pubname = '$PUB';")

  # Format table list for LIMIT TO clause
  LIMIT_TO=$(echo $TABLE_LIST | tr ' ' ',' | sed 's/,/, /g')

  # Import foreign schema with LIMIT TO
  psql -h $PITR2_HOST -U $USERNAME -d $DBNAME <<EOF
IMPORT FOREIGN SCHEMA $REMOTE_SCHEMA
LIMIT TO ($LIMIT_TO)
FROM SERVER $FDW_SERVER
INTO $FDW_SCHEMA;
EOF

  # Step 4: Performance tuning in parallel
  for table in $TABLE_LIST; do
    (
      echo "Tuning table: $table"
      psql -h $PITR2_HOST -U $USERNAME -d $DBNAME <<EOT
      ALTER FOREIGN TABLE $LOCAL_SCHEMA.$table OPTIONS (add fetch_size '10000');
      ALTER FOREIGN TABLE $LOCAL_SCHEMA.$table OPTIONS (add batch_size '10000');
      ANALYZE $LOCAL_SCHEMA.$table;
EOT
    ) > logs/${PUB}_${table}_tune.log 2>&1 &
  done
done

# Wait for all background jobs to finish
wait

# Step 5: Create staging and delta tables
psql -h $PITR2_HOST -U $USERNAME -d $DBNAME <<EOF
CREATE UNLOGGED TABLE IF NOT EXISTS staging_data_deltas (
  table_name text,
  source text,
  row_data jsonb
);

CREATE TABLE IF NOT EXISTS public.data_deltas (
  table_name text NULL,
  source text NULL,
  row_data jsonb NULL,
  recorded_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uniq_delta UNIQUE (table_name, source, row_data)
);
EOF

# Step 6: Python environment setup
sudo apt update
sudo apt install -y python3 python3-pip
pip3 install psycopg2-binary pandas sqlalchemy

echo "Setup complete. You can now run data_delta.py"
