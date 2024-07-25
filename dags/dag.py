from airflow import DAG
from airflow.decorators import task,dag
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import psycopg2
from pandas import  DataFrame

from datetime import datetime

from main import execute

dags = ["sqltostaging", "stagingtodv", "dvtobv"]



SQLToStagingTasks = ["city", "inventory", "payment", "country", "film_actor", "category",
                     "film", "film_category", "customer", "language", "actor",
                     "address", "staff", "rental", "store"]

bvtasks = ["film"]



def get_data_from_conf_table(table, stage):
    if stage != 'dvtobv':
        query = f"select *  from etlconf.Etl_Process_Mapping where SourceTableName = '{table}' and Stage = '{stage}'"
    else:
        query = f"select *  from etlconf.Etl_Process_Mapping where DestTableName = '{table}' and Stage = '{stage}'"
    cur = psycopg2.connect(database = "postgres",
                            user = "postgres",
                            host= "postgres_db",
                            password = "postgres",
                            port = 9999).cursor()
    cur.execute(query)
    # colnames = [desc[0] for desc in cur.description]
    df = DataFrame(cur.fetchall())
    #print columns name
    df.columns = [desc[0] for desc in cur.description]
    cur.close()

    # return colnames
    return DataFrame(df)

def process_table(table, stage):
    tabletype = get_data_from_conf_table(table,stage)['tabletype'].values[0]
    # print(f"{dag_id} :dag_id")
    # print(f"table: {table}")
    # if table_stage == "sqltostaging" and table_type == "full":
    return  execute(table, stage, tabletype)


def create_task(stage):
    if stage == "dvtobv":
        for task_id in bvtasks:
            tables_task = PythonOperator(task_id = task_id,
                                     python_callable=process_table ,
                                     op_kwargs={"table" : task_id, "stage" : stage}
                                    #  dag=dag
                                )
    else:
        for task_id in SQLToStagingTasks:
            tables_task = PythonOperator(task_id = task_id,
                                     python_callable=process_table , 
                                     op_kwargs={"table" : task_id, "stage" : stage}
                                    #  dag=dag
                                )
    tables_task


def create_dag(stage,start_date,schedule,description, tags,catchup):

    with DAG(dag_id=stage,start_date=start_date,schedule=schedule,description=description,tags=tags, catchup=catchup ):

        create_task(stage)

for dag_id in dags:
    create_dag(dag_id, datetime(2024,3,20),"@daily","main dag",["main"],False)
