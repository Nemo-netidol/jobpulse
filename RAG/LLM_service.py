import os
from langchain_core.prompts import PromptTemplate
from huggingface_hub import InferenceClient
from dotenv import load_dotenv
from .RetrievalStrategy import RetrievalStrategy

load_dotenv()

MAX_CHARS_PER_DOC = 3000

def format_docs(docs):
    formatted = []
    for doc in docs:
        title    = doc.metadata.get('title', 'N/A')
        company  = doc.metadata.get('company', 'N/A')
        location = doc.metadata.get('location', 'N/A')
        url = doc.metadata.get('url', 'N/A')
        # Truncate long descriptions so the total prompt stays within token limits
        content  = doc.page_content[:MAX_CHARS_PER_DOC]
        if len(doc.page_content) > MAX_CHARS_PER_DOC:
            content += "..."
        formatted.append(f"{title} at {company}, {location}: {content}. Source: {url}")
    return "\n\n".join(formatted)

class LLMService:
    def __init__(self, vector_db, strategy: RetrievalStrategy):
        self.model_id = "meta-llama/Llama-3.1-8B-Instruct"
        self.vector_db = vector_db
        self.client = InferenceClient(
            api_key=os.environ["HF_TOKEN"],
        )
        self.strategy = strategy
        self.retriever = vector_db.as_retriever(k=5)
        
        self.prompt = PromptTemplate.from_template("""You are a job search assistant.

Use the following job postings to answer the user's question.
Your task is to assist user to find jobs from the question they ask and use context to find the jobs. If there is no relevant context or job given to you, you can say that "at the moment, there is no job that you are looking for. Please wait for the job update in the future", or something like that. When you generating answer, don't forget to attach url source, location, discription, and salary of the job.

{context}

---

Answer the question based on the above context: {question}
""")       

    def set_strategy(self, strategy: RetrievalStrategy):
        """Setter function for retrievel strategy"""
        self.strategy = strategy

    def query(self, question: str):
        try:
            docs = self.strategy.retrieve(question, self.retriever)
            # retriever = self.vector_db.as_retriever(k=5)
            # docs = retriever.invoke(question)
            # print("Docs:", docs)
            context = format_docs(docs=docs)
            print("Context:", context)
            prompt = self.prompt.format(context=context, question=question)

            completion = self.client.chat.completions.create(
                model="meta-llama/Llama-3.1-8B-Instruct",
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=1024,
            )

            return completion.choices[0].message.content
        except Exception as e:
            print(f"Query error: {e}")
            return f"Error processing query: {str(e)}"
        
    def get_data_count(self):
        return self.vector_db.get_data_count()
    
    def debug_retriever(self, question: str):
        """Debug method to check what the retriever returns"""
        try:
            retriever = self.vector_db.as_retriever(k=5)
            docs = retriever.invoke(question)
            print(f"Retriever returned {len(docs)} documents")
            for i, doc in enumerate(docs):
                print(f"  Doc {i}: {doc.page_content[:100]}...")
            return docs
        except Exception as e:
            print(f"Retriever debug error: {e}")
            return []
    
    def debug_chain(self, question: str):
        """Debug each step of the RAG chain separately"""
        print("=" * 50)
        print("STEP 1: Testing Retriever")
        print("=" * 50)
        try:
            retriever = self.vector_db.as_retriever(k=5)
            docs = retriever.invoke(question)
            print(f"✓ Retriever returned {len(docs)} documents")
        except Exception as e:
            print(f"✗ Retriever failed: {e}")
            return
        
        print("\n" + "=" * 50)
        print("STEP 2: Testing format_docs")
        print("=" * 50)
        try:
            context = format_docs(docs)
            print(f"✓ Context formatted ({len(context)} chars):")
            print(context[:300] + "..." if len(context) > 300 else context)
        except Exception as e:
            print(f"✗ format_docs failed: {e}")
            return
        
        print("\n" + "=" * 50)
        print("STEP 3: Testing Prompt")
        print("=" * 50)
        try:
            prompt_value = self.prompt.invoke({"context": context, "question": question})
            print(f"✓ Prompt created:")
            print(str(prompt_value)[:500] + "..." if len(str(prompt_value)) > 500 else prompt_value)
        except Exception as e:
            print(f"✗ Prompt failed: {e}")
            return
        
        print("\n" + "=" * 50)
        print("STEP 4: Testing LLM")
        print("=" * 50)
        
        # Check HF_TOKEN
        hf_token = os.getenv("HF_TOKEN")
        if not hf_token:
            print("✗ HF_TOKEN environment variable is NOT set!")
            return None
        print(f"✓ HF_TOKEN is set (starts with: {hf_token[:10]}...)")
        print(f"  Using model: {self.model_id}")
        
        try:
            llm_response = self.llm.invoke(prompt_value)
            print(f"✓ LLM response type: {type(llm_response)}")
            print(f"✓ LLM response: {llm_response}")
            return llm_response
        except Exception as e:
            print(f"✗ LLM failed: {type(e).__name__}: {e}")
            print("\nPossible fixes:")
            print("  1. Check if HF_TOKEN is valid")
            print("  2. Accept model terms at: https://huggingface.co/meta-llama/Llama-3.2-1B")
            print("  3. Try a different model (e.g., 'mistralai/Mistral-7B-Instruct-v0.2')")
            return None
    


    





