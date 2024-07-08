from abstractstage.AbstractStage import AbstractStage
from abstracttype.AbstractFull import AbstractFull
from abstracttype.AbstractIncremental import AbstractIncremental
from myFramework.utils.readYaml import ReadYaml
from myFramework.utils.utils import getDF, fillPosgres, get_data_from_conf_table


class SqlToStaging(AbstractStage):

    def create_full(self) -> AbstractFull:
        return SQLToStagingFull()
    
    def create_incremental(self) -> AbstractIncremental:
        return SQLToStagingIncremental()
    

    
class SQLToStagingFull(AbstractFull):


    def some_function(self, table, stage) -> None:
        
        # yaml data
        # yaml = ReadYaml(f"/app/conf/{stage}/{tabletype}.yaml", f'{schema}.{table}')
        # # get data from source
        # sourceDF = getDF(yaml.getSourceDBName(), yaml.getTSourceTableName(),yaml.getSourceSchema())
        # # insert data another table
        # fillPosgres(sourceDF,f'{yaml.getDestDBName()}',f'{yaml.getDestSchema()}',yaml.getDestTbaleName(), yaml.getInsertionType())
        # print(stage, table)
        # print(f"{table}", f"{stage}")
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

        print(f"\n {sourcedbname} \n {sourcetablename} \n {sourceschema}")

        sourceDF = getDF(sourcedbname,sourcetablename , sourceschema)

        fillPosgres(sourceDF, DestDBName, DestSchema, DestTableName, InsertionType)

class SQLToStagingIncremental(AbstractIncremental):

    def some_function(self, stage, tabletype,schema, table, fromDate, toDate) ->None:
         # yaml data
        yaml = ReadYaml(f"/app/conf/{stage}/{tabletype}.yaml", f'{schema}.{table}')
        # get data from source
        sourceDF = getDF(yaml.getSourceDBName(), yaml.getTSourceTableName(),yaml.getSourceSchema(),fromDate, toDate)
        # insert data another table
        fillPosgres(sourceDF,f'{yaml.getDestDBName()}',f'{yaml.getDestSchema()}',yaml.getDestTbaleName(), yaml.getInsertionType())
