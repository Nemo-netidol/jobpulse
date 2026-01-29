from .database import Database
from .vector_db import VectorDatabase
from typing import Dict

class EmbeddingService:
    """
    Service for sync sqlite -> vector database
    """
    def __init__(self, db, vector_db):
        self.sql_db = db
        self.vector_db = vector_db
        print("✅ EmbeddingService ready")
    
    def sync_embeddings(self, batch_size: int=20, delay=1.0) -> Dict:
        jobs = self.sql_db.get_jobs_without_embedding(batch_size)

        print(f"Found {len(jobs)} jobs to embed")

        success_count = 0
        failed_count = 0
        for i, job in enumerate(jobs, 1):
            try:
                if self.vector_db.add_job(job['id'], job):
                    print("add job successfully")
                    self.sql_db.mark_as_embedded(job['id'])
                    success_count += 1
                    if success_count % 5 == 0:
                        print(f"Processed {i}/{len(jobs)} jobs...")
                else:
                    failed_count += 1
            except Exception as e:
                print(f"❌ Error with job {job['id']}: {e}")
                failed_count += 1
                continue

        print("\n" + "=" * 60)
        print("Sync Complete!")
        print("=" * 60)
        print(f"✅ Success: {success_count}/{len(jobs)}")
        print(f"❌ Failed: {failed_count}/{len(jobs)}")

        stats = self.sql_db.get_stats()
        vector_stats = self.vector_db.get_stats()

        print(f"\n📊 Database Status:")
        print(f"   SQLite total: {stats['total_jobs']}")
        print(f"   With embeddings: {stats['embedded_jobs']}")
        print(f"   Pending: {stats['pending_embeddings']}")
        print(f"   ChromaDB total: {vector_stats['total_embeddings']}")