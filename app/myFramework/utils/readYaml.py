import yaml
import os


class ReadYaml:
    def __init__(self, path,key):
        self.path = path
        self.key = key
        
        
    
    def getYaml(self):
        script_directory = os.path.dirname(os.path.abspath(__file__))
        yaml_file_path = os.path.join(script_directory, self.path)
        with open(yaml_file_path, 'r') as file:
            yaml_data = yaml.safe_load(file)
        # print(yaml_data[self.key]['Query'])
        return yaml_data[self.key]


    def getTSourceTableName( self):
        return self.getYaml()['SourceTableName']
    
    def getSourceDBName(self ):
        return self.getYaml()['SourceDBName']
    
    def getSourceSchema(self ):
        return self.getYaml()['SourceSchema']
    
    def getTableType(self ):
        return self.getYaml()['TableType']
    
    def getDestTbaleName(self ):
        return self.getYaml()['DestTableName']
    
    def getDestDBName(self ):
        return self.getYaml()['DestDBName']
    
    def getDestSchema(self ):
        return self.getYaml()['DestSchema']
    
    def getfilterColumn(self ):
        return self.getYaml()['FilterColumn']
    
    def getCode(self):
        return self.getYaml()['Code']
    
    def getSurogateKey(self):
        return self.getYaml()['SurogateKey']

    def getNaturalKey(self):
        return self.getYaml()['NaturalKey']

    def getInsertionType(self):
        return self.getYaml()['InsertionType']

    def getRenameColumn(self):
        return self.getYaml()['RenameColumn']
    
    def getQuery(self):
        return self.getYaml()['Query']
    
    
    
