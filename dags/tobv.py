from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, ExternalPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd

# from scripts.main import get_df

# from main import def_from_main

# with DAG("dv-to-bv", start_date=datetime(2024,3,10), schedule="@daily", 
#          description="DV To BV", tags=["Data Engineering","BV"], catchup=False) as dag:

 
#     # bash_dag = BashOperator(
#     #     task_id = "bash_dag",
#     #     bash_command='python $AIRFLOW_HOME/scripts/main.py'
#     # )

#     python_dag = PythonOperator(
#         task_id = "python_dag",
#         python_callable = def_from_main
#     )
#     # pass
