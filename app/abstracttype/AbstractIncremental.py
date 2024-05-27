from __future__ import annotations
from abc import ABC, abstractmethod


class AbstractIncremental(ABC):

    @abstractmethod
    def some_function(self):
        pass

    