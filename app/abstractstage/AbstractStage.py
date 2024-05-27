from __future__ import annotations
from abc import ABC, abstractmethod

class AbstractStage(ABC):

   

    # @abstractmethod
    def create_full(self):
        pass


    @abstractmethod
    def create_incremental(self):
        pass

    # @abstractmethod
    def create_scd(self):
        pass

    
