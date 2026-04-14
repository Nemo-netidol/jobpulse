from abc import ABC, abstractmethod
from services.vector_db import AbstractVectorDB
class RetrievalStrategy(ABC):
    @abstractmethod
    def retrieve(self, query: str, AbstractVectorDB, k: int = 5):
        pass

    @abstractmethod
    def get_name(self):
        pass