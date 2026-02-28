from abc import ABC, abstractmethod

class RetrievalStrategy(ABC):
    @abstractmethod
    def retrieve(self, query: str, retriever, k: int = 5):
        pass

    @abstractmethod
    def get_name(self):
        pass