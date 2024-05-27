from abstractstage.AbstractStage import AbstractStage

from abstracttype.AbstractFull import AbstractFull
from abstracttype.AbstractIncremental import AbstractIncremental
from abstracttype.AbastactSDCD import AbstractSCD

class StagingToDV(AbstractStage):

    def create_full(self) -> AbstractFull:
        return StagingToDVFull()

    def create_incremental(self) -> AbstractIncremental:
        return StagingToDVIncrementall()

    def create_SCD(self) -> AbstractSCD:
        return StagingToDVSCD()

class StagingToDVFull(AbstractFull):
    
    def some_function(self):
        return "StagingToDVFull"

class StagingToDVIncrementall(AbstractFull):
    
    def some_function(self):
        return "StagingToDVIncrementall"

class StagingToDVSCD(AbstractFull):
    
    def some_function(self):
        return "StagingToDVSCD"