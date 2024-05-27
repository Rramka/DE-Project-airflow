from airflow import DAG
from airflow.decorators import task,dag
from airflow.operators.python import PythonOperator

from datetime import datetime

from main import execute

dags = ["sqltostaging", "staigingtodv", "dvtobv"]



tasks = ["city", "inventory", "payment", "country", "film_actor", "category",
         "film", "film_category", "customer", "language", "actor",
         "address", "staff", "rental", "store"]

def process_table(table_stage, table_type="full",  schema="public", table="staff"):

    print( dag_id)

    if table_stage == "sqltostaging" and table_type == "full":
        return  execute(table_stage, table_type, schema, table)
    # print(f"processing table ")



def create_task(dag_id):

    for task_id in tasks:
        tables_task = PythonOperator(task_id = task_id,  
                                     python_callable=process_table , 
                                     op_args={dag_id:1}
                                )
# "/app/conf/toStaging/full.yaml" : 1, "dvdrental" : 2, task_id : 3, dag_id:4
        tables_task


def create_dag(dag_id, start_date,schedule,description, tags,catchup):

    with DAG(dag_id=dag_id,start_date=start_date,schedule=schedule,description=description,tags=tags, catchup=catchup ):

        create_task(dag_id)

for dag_id in dags:
    create_dag(dag_id, datetime(2024,3,20),"@daily","test dag",["test"],False)