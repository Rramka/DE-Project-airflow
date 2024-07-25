from __future__ import annotations
from abc import ABC, abstractmethod


class AbstractLink(ABC):

    @abstractmethod
    def some_function(self, stage, table):
        pass
