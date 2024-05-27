# from myFramework.utils.utils import fillPosgres
# from myFramework.utils.utils import getDF
# from sqltostaging.initial.toStaging import ToStaging as toStaging_initial
# from sqltostaging.full.toStaging import ToStaging as toStaging_full
# from sqltostaging.incremental.toStaging import ToStaging as toStaging_incremental
# # from managment.cleanstaging.cleanStaging import CleanStaging 
# from myFramework.utils.readYaml import ReadYaml
# # from stagingtodv.SCDType1.toDV import ToDV
# from stagingtodv.SCDType2.toDV import ToDV
# from dvtobv.ToBV import ToBV
# from datetime import datetime


from concretestage.SQLToStaging import SqlToStaging
from concretestage.StagingToDV import StagingToDV
from concretestage.DVToBV import DVToBV



def execute( stage, type,schema, table) -> None:

   print(f"\n stage: {stage} \n type: {type} \n schema: {schema} \n table:{table}")

   SqlToStaging().create_full().some_function(stage, type, schema, table)


    
# if __name__ == "__main__":

    # execute(SqlToStaging().create_full().some_function() )

    # execute(SqlToStaging().create_incremental().some_function())

    # execute(StagingToDV().create_full().some_function())
    # execute(StagingToDV().create_incremental().some_function())
    # execute(StagingToDV().create_SCD().some_function())

    # execute(DVToBV().create_incremental().some_function())

    
    
    # yaml_path, schema_name, table_name
    # testread = ReadYaml(table.yaml_data)
    # print(f"{testread.getYaml()}")
    # table = Tables(testread.path, testread.key)
    # sourceDF = getDF(table.getSourceDBName(), table.getTSourceTableName(), table.getSourceSchema())
    # fillPosgres(sourceDF, f'{test.getDestDBName()}', f'{test.getDestSchema()}', test.getDestTbaleName())
# return table_name




# ready = ReadYaml("/Users/ramazkapanadze/Desktop/DEProjectAirflow/app/conf/toStaging/full.yaml")
# # print(ready.getYaml())

# tab = Tables("public.store")
# print(tab.getTSourceTableName())








# ---------------------------BV----------------


# testread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/toBV/film.yaml", 'dvdrental.film')
# test = ToBV(testread.path, testread.key)
# sourcDF = utils.getDF(test.getSourceDBName(), test.getQuery())
# utils.fillPosgres(sourcDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName(), test.getInsertionType())


# print(sourcDF)



# ---------------------------DV----------------


# SCDType2

# testread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/toDV/dvdrental/SCDType2.yaml", 'dvdrental.address')
# test = ToDV(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema(),test.getfilterColumn(), "2003-05-26", "2014-02-28").drop("last_update",  axis=1)
# targetDF = utils.getDF(test.getDestDBName(), test.getDestTbaleName(),test.getDestSchema())
# SCD_DF = utils.scd(sourceDF,targetDF, list(test.getSurogateKey().split(" ")), test.getNaturalKey())
# dest_col_list = list(utils.getDF(test.getDestDBName(), test.getDestTbaleName(),test.getDestSchema()).columns)
# genaretedDF = utils.generateSurogateKey(SCD_DF, test.getCode(), list(test.getSurogateKey().split(" ")),dest_col_list)
# # print(genaretedDF)
# utils.fillPosgres(genaretedDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName(), test.getInsertionType())

# ,test.getfilterColumn(), "2013-05-26", "2005-05-26"


# Full - SCDType1
# create ReadYaml object

# testread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/toDV/dvdrental/SCDType1.yaml", 'dvdrental.city')
# test = ToDV(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema())
# dest_col_list = list(utils.getDF(test.getDestDBName(), test.getDestTbaleName(),test.getDestSchema()).columns)
# generatenaturalDF = utils.GenerateNaturalKey(sourceDF, test.getNaturalKey())
# genaretedDF = utils.generateSurogateKey(generatenaturalDF,test.getCode(), list(test.getSurogateKey().split(" ")), dest_col_list)
# # print(sourceDF)
# utils.fillPosgres(genaretedDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName(), test.getInsertionType())



#link
# testread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/toDV/dvdrental/link.yaml", 'dvdrental.film_actor')
# test = ToDV(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema(), ).drop("insertion_date",  axis=1)
# dest_col_list = list(utils.getDF(test.getDestDBName(), test.getDestTbaleName(),test.getDestSchema()).columns)
# genaretedDF = utils.generateSurogateKey(sourceDF, test.getCode(), list(test.getSurogateKey().split(" ")),dest_col_list)
# utils.fillPosgres(genaretedDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName(),test.getInsertionType())

# ,"2005-05-26", "2020-05-26"test.getfilterColumn()     for incremental loading


# incremental
# create ReadYaml object

# testread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/toDV/dvdrental/incremental.yaml", 'dvdrental.payment')
# test = ToDV(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema()).drop("insertion_date",  axis=1)
# dest_col_list = list(utils.getDF(test.getDestDBName(), test.getDestTbaleName(),test.getDestSchema()).columns)
# generatenaturalDF = utils.GenerateNaturalKey(sourceDF, test.getNaturalKey())
# genaretedDF = utils.generateSurogateKey(generatenaturalDF,test.getCode(), list(test.getSurogateKey().split(" ")) ,dest_col_list)
# utils.fillPosgres(genaretedDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName(), test.getInsertionType())

# ,test.getfilterColumn(), "2006-02-14", "2006-02-15"    -- for incremental




                        # S T A G I N G
# # initial
# test = toStaging_initial('dvdrental','public')
# tbname_Df = utils.getTbaleList(test.getSourceDBName,test.getSourceSchema)
# for tbname in tbname_Df['tablename']:    
#     utils.fillPosgres(utils.getDF( tbname ,test.getSourceSchema),'DBStaging','dvdrental', tbname)
  
   
# full load
# testread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/tostaging/dvdrental/full.yaml", 'public.store')
# test = toStaging_full(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema())
# utils.fillPosgres(sourceDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName())


# incremental 
# testread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/tostaging/dvdrental/incremental.yaml", 'public.customer')
# test = toStaging_incremental(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema(),test.getfilterColumn(), "2000-02-10", "2024-02-16")
# newsourceDF = utils.addInsertionDate(sourceDF)
# # print(newsourceDF)
# utils.fillPosgres(newsourceDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName())

# clean staging
# cleantestread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/tostaging/dvdrental/full.yaml", 'public.category')
# t = CleanStaging()
# t.cleanStaging('DBStaging', 'dvdrental', 'category', 2023, 12, 28)
