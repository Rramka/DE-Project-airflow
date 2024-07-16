from abstractstage.AbstractStage import AbstractStage
from abstracttype.AbstractFull import AbstractFull
from abstracttype.AbstractIncremental import AbstractIncremental
from myFramework.utils.readYaml import ReadYaml
from myFramework.utils.utils import getDF, fillPosgres, get_data_from_conf_table, last_run_date_update


class SqlToStaging(AbstractStage):

    def create_full(self) -> AbstractFull:
        return SQLToStagingFull()
    
    def create_incremental(self) -> AbstractIncremental:
        return SQLToStagingIncremental()
    

    
class SQLToStagingFull(AbstractFull):


    def some_function(self, table, stage) -> None:
        
        
        df = get_data_from_conf_table(f"{table}", f"{stage}")
        print(df)
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

        fillPosgres(sourceDF, DestDBName, DestSchema, DestTableName, InsertionType)
        last_run_date_update(DestDBName, DestSchema, DestTableName)

class SQLToStagingIncremental(AbstractIncremental):

    def some_function(self, table, stage) ->None:
        df = get_data_from_conf_table(f"{table}", f"{stage}")
        print(df)
        # print(df['sourcedbname'], df['sourcetablename'], df['sourceschema'])
        sourcedbname = df['sourcedbname'].values[0]
        sourcetablename = df['sourcetablename'].values[0]
        sourceschema = df['sourceschema'].values[0]

        DestDBName = df['destdbname'].values[0]
        DestSchema = df['destschema'].values[0]
        DestTableName = df['desttablename'].values[0]
        InsertionType = df['insertiontype'].values[0]
        FilterColumn = df['filtercolumn'].values[0]

        # print(f"\n {sourcedbname} \n {sourcetablename} \n {sourceschema}")
        try:
            sourceDF = getDF(sourcedbname,sourcetablename , sourceschema, FilterColumn, '2000-01-01', '2024-08-01')
        except:
            return("not data at the period")
        if sourceDF is not None:
            fillPosgres(sourceDF, DestDBName, DestSchema, DestTableName, InsertionType)
            last_run_date_update(DestDBName, DestSchema, DestTableName)

