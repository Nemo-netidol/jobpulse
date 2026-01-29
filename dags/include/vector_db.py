from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEndpointEmbeddings
import os
from typing import List, Dict
from langchain_core.documents import Document

class VectorDatabase:
    def __init__(self,  persist_directory: str = "./data/chroma_db"):
        self.model_id = "BAAI/bge-small-en-v1.5"

        self.embd = HuggingFaceEndpointEmbeddings(
            model=self.model_id,
            huggingfacehub_api_token=os.getenv('HF_TOKEN'))
        
        self.vectorstore = Chroma(
            collection_name="jobs", 
            embedding_function=self.embd, 
            persist_directory=persist_directory)

    def create_embeddings(self, text: str) -> List[float]:
        """Create embeddings using HuggingFace API. Returns 1D list of floats."""
        
        # HF returns shape [1, tokens, embedding_dim] - mean pool across tokens
        embeddings = self.embd.feature_extraction(text, model=self.model_id)
        return embeddings
    
    def add_job(self, job_id, job_data: dict) -> bool:
        """Add job to vector database"""
        try:
            text = f"""
                    {job_data.get("title", "")}
                    {job_data.get("company", "")}
                    {job_data.get("description", "")}
                    {job_data.get("location", "")}
                    {job_data.get("posted_date", "")}
                    """.strip()
            
            document = Document(
                page_content=text,
                metadata={
                    'job_id': job_id,
                    'title': job_data.get('title', '')[:100],
                    'company': job_data.get('company', '')[:100],
                    'location': job_data.get('location', '')[:100],
                    'post_date': job_data.get('post_date', '')
                }
            )

            # Add to vector database
            self.vectorstore.add_documents([document], ids=[job_id])

            return True
        except Exception as e:
            print(f"Error adding job {job_id}: {e}")
            return False
        
    def search(self, query: str, n_results: int=10) -> List[Dict]:
        try:
            # Use same embedding model for query to match stored embeddings
            results = self.vectorstore.similarity_search(
                query, 
                k=n_results
            )
            return results
        except Exception as e:
            print(f"Search error: {e}")
            return []
        
    def as_retriever(self, k: int=5):
        """Return LangChain retriever for use in chains"""
        return self.vectorstore.as_retriever(search_kwargs={"k": k})
        
    def get_stats(self):
        count = self.vectorstore._collection.count()
        return {
            "total_embeddings": count,
            "collection_name": "jobs"
        }
