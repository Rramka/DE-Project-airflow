from abstractstage.AbstractStage import AbstractStage
from abstracttype.AbstractIncremental import AbstractIncremental
from abstracttype.AbstractFull import AbstractFull

from myFramework.utils.utils import getDF, fillPosgres, get_data_from_conf_table, last_run_date_update


class DVToBV(AbstractStage):

    def create_incremental(self) -> AbstractIncremental:
        return DVToBvIncremental()

    def create_full(self) -> AbstractFull:
        return DVToBvFull()

class DVToBvIncremental(AbstractIncremental):
    
    def some_function(self):
        return "DVToBvIncremental"

class DVToBvFull(AbstractFull):

    def some_function(self, table, stage):
        df = get_data_from_conf_table(f"{table}", f"{stage}")
        sourcedbname = df['sourcedbname'].values[0]
        Query = df['query'].values[0]
        InsertionType = df['insertiontype'].values[0]
        DestDBName = df['destdbname'].values[0]
        DestSchema = df['destschema'].values[0]
        DestTableName = df['desttablename'].values[0]

        sourceDF = getDF(sourcedbname, p_query=Query)
        # print('sourceDF', sourceDF)

        fillPosgres(sourceDF,DestDBName,DestSchema,DestTableName, InsertionType)
        last_run_date_update(DestDBName, DestSchema, DestTableName)

