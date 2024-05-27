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


    def some_function(self, stage, tabletype,schema, table):
        
        # yaml data
        yaml = ReadYaml(f"/app/conf/{stage}/{tabletype}.yaml", f'{schema}.{table}')
        # print(f"\n{yaml.getSourceDBName()} \n, {yaml.getTSourceTableName()} \n,{yaml.getSourceSchema()}")
        # get data from source
        sourceDF = getDF(yaml.getSourceDBName(), yaml.getTSourceTableName(),yaml.getSourceSchema())
        # sourceDF = getDF("postgres", "staff","public")
        # print( type(sourceDF))
        print(sourceDF)

        # cawera
        # fillPosgres(sourceDF,f'{yaml.getDestDBName()}',f'{yaml.getDestSchema()}',yaml.getDestTbaleName(), yaml.getInsertionType())

        return "SQLToStagingFull"
    
    
class SQLToStagingIncremental(AbstractIncremental):

    def some_function(self):
        return "SQLToStagingIncremental"