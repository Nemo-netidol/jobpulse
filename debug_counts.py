from dags.include.vector_db import VectorDatabase
from dags.include.database import Database
import os

CHROMA_DIR = "data/chroma_db"
DB_PATH = "data/jobpulse.db"

vdb = VectorDatabase(CHROMA_DIR)
db = Database(DB_PATH)

print(f"ChromaDB count: {vdb.get_data_count()}")
print(f"SQLite total jobs: {db.get_stats()['total_jobs']}")
print(f"SQLite embedded jobs: {db.get_stats()['embedded_jobs']}")
