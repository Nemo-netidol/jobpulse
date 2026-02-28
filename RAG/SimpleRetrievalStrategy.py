from .RetrievalStrategy import RetrievalStrategy

class SimpleRetrievalStrategy(RetrievalStrategy):
    def retrieve(self, query: str, retriever, k: int = 5):
        return  retriever.invoke(query)

    def get_name(self):
        return "Simple Vector Similarity"