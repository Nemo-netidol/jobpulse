import sqlite3
from pathlib import Path

DB_PATH = Path("/opt/airflow/data/jobpulse.db")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# inspect jobs table
cursor.execute("SELECT COUNT(*) FROM jobs;")
print("Job count:", cursor.fetchone()[0])

# Print data example in DB
cursor.execute("SELECT id, company, title FROM jobs LIMIT 5;")
for row in cursor.fetchall():
    print(row)

conn.close()
