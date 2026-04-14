import os
from google.cloud import bigquery
import logging
from fastembed import TextEmbedding
import json
from typing import List


logger = logging.getLogger(__name__)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
key_path = os.path.join(project_root, 'gcp-key.json')

class BigQueryService:
    def __init__(self, hook):
        self.client = hook.get_client()

    def get_unembedded_jobs(self, save_local=False):
        try:
            logger.info("Querying BigQuery for unembedded jobs...")
            # Use a clean, single-line string for the SQL to avoid parsing issues
            sql = "SELECT * FROM `jobpulse-492611.jobpulse.jobs` WHERE has_embedded = False"
            df = self.client.query(sql).to_dataframe()

            if save_local:
                cache_path = os.path.join(project_root, "jobs_cache.jsonl")
                df.to_json(cache_path, orient='records', lines=True, date_format='iso')
                logger.info(f"Saved {len(df)} jobs to {cache_path}")

            return df.to_dict('records') # List of dictionary
        except Exception as e:
            logger.error(f"Error in BigQueryService: {e}")
            raise

    def get_category_count(self):
        logger.info("Querying job category...")
        sql = "SELECT category, COUNT(*) as count FROM `jobpulse-492611.jobpulse.jobs` GROUP BY category"
        df = self.client.query(sql).to_dataframe()
        return df

    def get_category_stats(self) -> List[dict]:
        """Fetch category counts for ECharts visualization."""
        logger.info("Fetching category stats for visualization...")
        sql = """
            SELECT category as name, COUNT(*) as value 
            FROM `jobpulse-492611.jobpulse.jobs` 
            WHERE category IS NOT NULL
            GROUP BY category
            ORDER BY value DESC
        """
        df = self.client.query(sql).to_dataframe()
        return df.to_dict('records')



if __name__ == "__main__":
    # class StandaloneBQHook:
    #     def __init__(self, key_path):
    #         self.client = bigquery.Client.from_service_account_json(key_path)
        
    #     def get_pandas_df(self, sql):
    #         return self.client.query(sql).to_dataframe()
    
    # mock_hook = StandaloneBQHook(key_path)
    # service = BigQueryService(mock_hook)
    # print("--- Testing BigQueryService Standalone ---")
    # jobs = service.get_unembedded_jobs(save_local=True)
    # print(f"Fetched {len(jobs)} jobs")
    
    with open("jobs_cache.jsonl", "r") as f:
        # Use a list comprehension to parse each line as a dictionary
        jobs: List[dict] = [json.loads(line) for line in f]

    # Embedding model
    embedding_model = TextEmbedding()

    jobs_to_embed = [f"""
                {job.get("title", "")}
                {job.get("company", "")}
                {job.get("description", "")}
                {job.get("location", "")}
                {job.get("posted_date", "")}
                {job.get("url", "")}
                {job.get("category", "")}
                    """.strip() for job in jobs]
    
    embeddings_generator = embedding_model.embed(jobs_to_embed)
    embedding_list = list(embeddings_generator)
    print(len(embedding_list[0]))
    
