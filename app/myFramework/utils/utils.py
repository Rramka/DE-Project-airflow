import myFramework.source.posgresql.connect as conn
import pandas as pd
# from multipledispatch import dispatch
from datetime import datetime
from pandas import  DataFrame




def getTbaleList(dbname, schema):
        return pd.read_sql(f"select tablename  from pg_catalog.pg_tables where schemaname = '{schema}'", conn.getConnection(dbname))

def posgreExecute(dbName, query):
        engine = conn.getConnection(dbName)
        engine.execute(query)

def getDF(source_dbname, tablename=None,schema=None,filterColumn=None, dateFrom=None, dateTo=None,p_query=None):
    if p_query is None:

        if filterColumn is None and dateFrom is None and dateTo is None:
            query = f"select T.* from {source_dbname}.{schema}.{tablename} T"
            # print('query', query)
        else:
            query = f"select T.* from {source_dbname}.{schema}.{tablename} T where {filterColumn} >= '{dateFrom}' and {filterColumn} < '{dateTo}' "
            # print('query', query)
        # print(query)
    else:
        query = p_query

    cur = conn.getCursor(source_dbname)
    cur.execute(query)
    # colnames = [desc[0] for desc in cur.description]
    df = DataFrame(cur.fetchall())
    df.columns = [desc[0] for desc in cur.description]
    # return colnames
    # cur.close()
    return DataFrame(df)

def fillPosgres( df, dst_dbname, schema, tablename, insertiontype):
        df.to_sql(tablename, conn.getEngine(dst_dbname)
                , schema=f"{schema}", if_exists=insertiontype, index=False)

def last_run_date_update(dst_dbname, schema, tablename):
    query = f"update etlconf.Etl_Process_Mapping T set last_run_date = current_timestamp" \
            f" where destdbname='{dst_dbname}' and destschema='{schema}' and desttablename='{tablename}';"
    print(query)
    cur = conn.getCursor('postgres')
    cur.execute(query)
    cur.execute("commit;")

def addInsertionDate(df: pd.DataFrame ):
      return df.assign(insertion_date = lambda x: datetime.now())

def generateSurogateKey(df, code, SurogatekeyList, dest_col_list):
      newdf = pd.DataFrame(df)
      for key in SurogatekeyList:
        # print(key)
        Surogatekey = newdf[key]+int(str(1000000) + str(code))
        newdf = newdf.assign(tmpkey = pd.Series(Surogatekey).values).drop(f'{key}', axis=1)
        newdf.rename(columns={'tmpkey':f'gen_{key}'}, inplace=True)
      newdf = newdf.reindex(dest_col_list, axis=1)
      return newdf

def GenerateNaturalKey(df, Naturalkey):
    newdf = pd.DataFrame(df)
    NaturalValue = newdf[Naturalkey]
    newdf = newdf.assign(tmpkey = pd.Series(NaturalValue).values)
    newdf.rename(columns={'tmpkey':f'source_{Naturalkey}'}, inplace=True)
    return newdf

def scd(source_df, target_df, cols_to_gen, naturalkey, cols_to_track: list=None):

    for col in cols_to_gen:
         target_df.rename(columns={f'gen_{col}':col}, inplace=True)

    if not cols_to_track:
        cols = list(source_df.columns)
        cols.remove(naturalkey)
        cols_to_track = list(cols)

    source_df[f'source_{naturalkey}'] =  source_df[naturalkey]
    # print(source_df)


#  Check if target data is empty (initial load)
    if target_df.empty:
        # Add "start_date" column with current date for initial load
        source_df["start_ts"] = pd.to_datetime("today")
        source_df["end_ts"] = pd.NA
        # Copy source data to target data
        target_df = source_df
        target_df[f'source_{naturalkey}'] = source_df[naturalkey]

    else:
        # define new records in source df and add start date 
        new_df = source_df[~source_df[naturalkey].isin(target_df[f'source_{naturalkey}'])]
        new_df["start_ts"] = pd.to_datetime("today")
        
        # print('new_df')
        # print(new_df)

        # define existing record in source df 
        old_df = source_df[source_df[naturalkey].isin(target_df[f'source_{naturalkey}'])]

        # print('old df')
        # print(old_df)

        # define updated rows ins target df
        merge_df = old_df.merge(target_df, on=cols_to_track, how='outer', indicator=True)
        # print(merge_df)
        diff_in_old_df_df = merge_df[merge_df['_merge']=='left_only']                
        diff_in_old_df_df["start_ts"] = pd.to_datetime("today")
        data_diff_in_old_df = diff_in_old_df_df.drop(columns=['_merge', f'source_{naturalkey}_y',f'{naturalkey}_y'])
        data_diff_in_old_df.rename(columns={f'source_{naturalkey}_x':f'source_{naturalkey}'}, inplace=True)
        data_diff_in_old_df.rename(columns={f'{naturalkey}_x':f'{naturalkey}'}, inplace=True)

        # print('data_diff_in_old_df')
        # print(data_diff_in_old_df)


        # Update existing records in target data is source df exist change records
        for index, row in data_diff_in_old_df.iterrows():
            target_df.loc[target_df[f'source_{naturalkey}'] == row[naturalkey], "end_ts"] = pd.to_datetime("today")
        
        # Concatenate updated and new data to target data
        target_df = pd.concat([target_df,data_diff_in_old_df,new_df ], ignore_index = True)
        
        # print("last target_df")
        # print(target_df)

    # Print the final target DataFrame
    return target_df

def get_data_from_conf_table(table, stage):
    # print(stage)
    if stage != 'dvtobv':
        # print('iiiiiiiifffffff')
        query = f"select *  from etlconf.Etl_Process_Mapping where SourceTableName = '{table}' and Stage = '{stage}'"
    else:
        # print('else')
        query = f"select *  from etlconf.Etl_Process_Mapping where DestTableName = '{table}' and Stage = '{stage}'"
    # print('query', query)
    cur = conn.getCursor("postgres")
    cur.execute(query)
    # colnames = [desc[0] for desc in cur.description]
    df = DataFrame(cur.fetchall())
    #print columns name
    df.columns = [desc[0] for desc in cur.description]
    cur.close()
    # return colnames
    return DataFrame(df)

