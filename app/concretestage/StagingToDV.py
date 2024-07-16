from abstractstage.AbstractStage import AbstractStage

from abstracttype.AbstractFull import AbstractFull
from abstracttype.AbstractIncremental import AbstractIncremental
from abstracttype.AbastactSDCD import AbstractSCD
from myFramework.utils.utils import getDF, fillPosgres, get_data_from_conf_table, last_run_date_update

class StagingToDV(AbstractStage):

    def create_full(self) -> AbstractFull:
        return StagingToDVFull()

    def create_incremental(self) -> AbstractIncremental:
        return StagingToDVIncrementall()

    def create_SCD(self) -> AbstractSCD:
        return StagingToDVSCD()

class StagingToDVFull(AbstractFull):
    
    def some_function(self, table, stage) -> None:

            df = get_data_from_conf_table(f"{table}", f"{stage}")
            # print(df['sourcedbname'], df['sourcetablename'], df['sourceschema'])
            sourcedbname = df['sourcedbname'].values[0]
            sourcetablename = df['sourcetablename'].values[0]
            sourceschema = df['sourceschema'].values[0]

            DestDBName = df['destdbname'].values[0]
            DestSchema = df['destschema'].values[0]
            DestTableName = df['desttablename'].values[0]
            InsertionType = df['insertiontype'].values[0]

            # print(f"\n {sourcedbname} \n {sourcetablename} \n {sourceschema}")

            sourceDF = getDF(sourcedbname,sourcetablename , sourceschema)
            # print('sourceDF     :' ,sourceDF)

            fillPosgres(sourceDF, DestDBName, DestSchema, DestTableName, InsertionType)
            last_run_date_update(DestDBName, DestSchema, DestTableName)

class StagingToDVIncrementall(AbstractFull):
    
    def some_function(self):
        return "StagingToDVIncrementall"

class StagingToDVSCD(AbstractFull):
    
    def some_function(self):
        return "StagingToDVSCD"
