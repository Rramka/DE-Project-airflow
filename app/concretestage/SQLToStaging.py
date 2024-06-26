from abstractstage.AbstractStage import AbstractStage
from abstracttype.AbstractFull import AbstractFull
from abstracttype.AbstractIncremental import AbstractIncremental
from myFramework.utils.readYaml import ReadYaml
from myFramework.utils.utils import getDF, fillPosgres

class SqlToStaging(AbstractStage):

    def create_full(self) -> AbstractFull:
        return SQLToStagingFull()
    
    def create_incremental(self) -> AbstractIncremental:
        return SQLToStagingIncremental()
    

    
class SQLToStagingFull(AbstractFull):


    def some_function(self, stage, tabletype,schema, table) -> None:
        
        # yaml data
        yaml = ReadYaml(f"/app/conf/{stage}/{tabletype}.yaml", f'{schema}.{table}')
        # get data from source
        sourceDF = getDF(yaml.getSourceDBName(), yaml.getTSourceTableName(),yaml.getSourceSchema())
        # insert data another table
        fillPosgres(sourceDF,f'{yaml.getDestDBName()}',f'{yaml.getDestSchema()}',yaml.getDestTbaleName(), yaml.getInsertionType())
    
    
class SQLToStagingIncremental(AbstractIncremental):

    def some_function(self, stage, tabletype,schema, table, fromDate, toDate) ->None:
         # yaml data
        yaml = ReadYaml(f"/app/conf/{stage}/{tabletype}.yaml", f'{schema}.{table}')
        # get data from source
        sourceDF = getDF(yaml.getSourceDBName(), yaml.getTSourceTableName(),yaml.getSourceSchema(),fromDate, toDate)
        # insert data another table
        fillPosgres(sourceDF,f'{yaml.getDestDBName()}',f'{yaml.getDestSchema()}',yaml.getDestTbaleName(), yaml.getInsertionType())
    