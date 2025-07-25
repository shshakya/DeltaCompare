#!/bin/bash

# Setup script for DeltaCompare project

echo "🔧 Starting setup for DeltaCompare..."

# Step 1: Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Step 2: Upgrade pip
echo "⬆️ Upgrading pip..."
pip install --upgrade pip

# Step 3: Install required packages
echo "📚 Installing dependencies..."
pip install psycopg2-binary sqlalchemy pandas

# Step 4: Create .env file with placeholders
echo "📝 Creating .env file..."
cat <<EOT >> .env
DB1=postgresql://user:password@localhost:5432/pitr1
DB2=postgresql://user:password@localhost:5432/pitr2
LOG_DB=postgresql://user:password@localhost:5432/logdb
EOT

# Step 5: Validate database connectivity
echo "🔍 Validating database connections..."
python3 <<EOF
import os
from sqlalchemy import create_engine

db1 = os.getenv("DB1", "postgresql://user:password@localhost:5432/pitr1")
db2 = os.getenv("DB2", "postgresql://user:password@localhost:5432/pitr2")
log_db = os.getenv("LOG_DB", "postgresql://user:password@localhost:5432/logdb")

def test_connection(conn_str, name):
    try:
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        print(f"✅ Connection to {name} successful.")
    except Exception as e:
        print(f"❌ Connection to {name} failed: {e}")

test_connection(db1, "DB1")
test_connection(db2, "DB2")
test_connection(log_db, "LOG_DB")
EOF

echo "✅ Setup complete. You can now run the comparison script."
