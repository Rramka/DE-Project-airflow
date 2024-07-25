from abstractstage.AbstractStage import AbstractStage

from abstracttype.AbstractFull import AbstractFull
from abstracttype.AbstractIncremental import AbstractIncremental
from abstracttype.AbastactSCD import AbstractSCD
from abstracttype.AbstractLink import AbstractLink
from myFramework.utils.utils import getDF, fillPosgres, get_data_from_conf_table, last_run_date_update, generateSurogateKey, GenerateNaturalKey, scd

class StagingToDV(AbstractStage):

    def create_full(self) -> AbstractFull:
        return StagingToDVFull()

    def create_incremental(self) -> AbstractIncremental:
        return StagingToDVIncrementall()

    def create_SCD(self) -> AbstractSCD:
        return StagingToDVSCD()

    def create_link(self) -> AbstractLink:
        return StagingToDVLink()

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
            SurogateKey = df['surogatekey'].values[0]
            Naturalkey = df['naturalkey'].values[0]
            Code = df['code'].values[0]


            # print(f"\n {sourcedbname} \n {sourcetablename} \n {sourceschema}")

            sourceDF = getDF(sourcedbname,sourcetablename , sourceschema)
            # print('sourceDF     :' ,sourceDF)

            dest_col_list = list(getDF(DestDBName, DestTableName,DestSchema).columns)
            # print('dest_col_list', dest_col_list)
            generatenaturalDF = GenerateNaturalKey(sourceDF, Naturalkey)
            # print('generatenaturalDF', generatenaturalDF)
            genaretedDF = generateSurogateKey(generatenaturalDF,Code, list(SurogateKey.split(" ")) ,dest_col_list)
            # print('genaretedDF', genaretedDF)

            fillPosgres(genaretedDF, DestDBName, DestSchema, DestTableName, InsertionType)
            last_run_date_update(DestDBName, DestSchema, DestTableName)

class StagingToDVIncrementall(AbstractIncremental):
    def some_function(self, table, stage) ->None:
        df = get_data_from_conf_table(f"{table}", f"{stage}")
        # print(df['sourcedbname'], df['sourcetablename'], df['sourceschema'])
        sourcedbname = df['sourcedbname'].values[0]
        sourcetablename = df['sourcetablename'].values[0]
        sourceschema = df['sourceschema'].values[0]

        DestDBName = df['destdbname'].values[0]
        DestSchema = df['destschema'].values[0]
        DestTableName = df['desttablename'].values[0]
        InsertionType = df['insertiontype'].values[0]
        FilterColumn = df['filtercolumn'].values[0]
        SurogateKey = df['surogatekey'].values[0]
        Naturalkey = df['naturalkey'].values[0]
        Code = df['code'].values[0]

        # print(f"\n {sourcedbname} \n {sourcetablename} \n {sourceschema}")
        # try:
        sourceDF = getDF(sourcedbname,sourcetablename,sourceschema)
            # .drop("insertion_date",  axis=1)
        # print('SOURCE DFFF', sourceDF)
        dest_col_list = list(getDF(DestDBName, DestTableName,DestSchema).columns)
        # print('dest_col_list', dest_col_list)
        generatenaturalDF = GenerateNaturalKey(sourceDF, Naturalkey)
        # print('generatenaturalDF', generatenaturalDF)
        genaretedDF = generateSurogateKey(generatenaturalDF,Code, list(SurogateKey.split(" ")) ,dest_col_list)
        # print('genaretedDF', genaretedDF)
        # except:
        #     return("not data at the period")
        # if sourceDF is not None:
        #     pass
        fillPosgres(genaretedDF,DestDBName,DestSchema,DestTableName, InsertionType)
        last_run_date_update(DestDBName, DestSchema, DestTableName)


class StagingToDVLink(AbstractLink):
    def some_function(self, table, stage) ->None:
        df = get_data_from_conf_table(f"{table}", f"{stage}")
        # print(df['sourcedbname'], df['sourcetablename'], df['sourceschema'])
        sourcedbname = df['sourcedbname'].values[0]
        sourcetablename = df['sourcetablename'].values[0]
        sourceschema = df['sourceschema'].values[0]

        DestDBName = df['destdbname'].values[0]
        DestSchema = df['destschema'].values[0]
        DestTableName = df['desttablename'].values[0]
        InsertionType = df['insertiontype'].values[0]
        FilterColumn = df['filtercolumn'].values[0]
        SurogateKey = df['surogatekey'].values[0]
        print('SurogateKey', SurogateKey)
        Code = df['code'].values[0]

        sourceDF = getDF(sourcedbname, sourcetablename,sourceschema)
            # .drop("insertion_date",  axis=1)
        dest_col_list = list(getDF(DestDBName, DestTableName,DestSchema).columns)
        genaretedDF = generateSurogateKey(sourceDF, Code, list(SurogateKey.split(" ")),dest_col_list)


        fillPosgres(genaretedDF,DestDBName,DestSchema,DestTableName,InsertionType)
        last_run_date_update(DestDBName, DestSchema, DestTableName)



class StagingToDVSCD(AbstractSCD):
    def some_function(self, table, stage) ->None:
        df = get_data_from_conf_table(f"{table}", f"{stage}")
        # print(df['sourcedbname'], df['sourcetablename'], df['sourceschema'])
        sourcedbname = df['sourcedbname'].values[0]
        sourcetablename = df['sourcetablename'].values[0]
        sourceschema = df['sourceschema'].values[0]

        DestDBName = df['destdbname'].values[0]
        DestSchema = df['destschema'].values[0]
        DestTableName = df['desttablename'].values[0]
        InsertionType = df['insertiontype'].values[0]
        FilterColumn = df['filtercolumn'].values[0]
        SurogateKey = df['surogatekey'].values[0]
        Naturalkey = df['naturalkey'].values[0]
        Code = df['code'].values[0]

        sourceDF = getDF(sourcedbname, sourcetablename,sourceschema,FilterColumn, "2000-01-01", "2024-02-28").drop("last_update",  axis=1)
        targetDF = getDF(DestDBName, DestTableName,DestSchema)
        SCD_DF = scd(sourceDF,targetDF, list(SurogateKey.split(" ")), Naturalkey)
        dest_col_list = list(getDF(DestDBName, DestTableName,DestSchema).columns)
        genaretedDF = generateSurogateKey(SCD_DF, Code, list(SurogateKey.split(" ")),dest_col_list)
        # print(genaretedDF)
        fillPosgres(genaretedDF,DestDBName,DestSchema,DestTableName, InsertionType)
