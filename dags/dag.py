from airflow import DAG
from airflow.decorators import task,dag
from airflow.operators.python import PythonOperator

from datetime import datetime

from main import execute

dags = ["sqltostaging", "staigingtodv", "dvtobv"]



SQLToStagingTasks = ["city", "inventory", "payment", "country", "film_actor", "category",
                     "film", "film_category", "customer", "language", "actor",
                     "address", "staff", "rental", "store"]

def process_table(table, stage):

    print(f"{dag_id} :dag_id")
    print(f"table: {table}")
    # if table_stage == "sqltostaging" and table_type == "full":
    return  execute(table, stage)


def create_task(stage):

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
