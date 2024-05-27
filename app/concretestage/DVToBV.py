from abstractstage.AbstractStage import AbstractStage
from abstracttype.AbstractIncremental import AbstractIncremental


class DVToBV(AbstractStage):

    def create_incremental(self) -> AbstractIncremental:
        return DVToBvIncremental()

class DVToBvIncremental(AbstractIncremental):
    
    def some_function(self):
        return "DVToBvIncremental"