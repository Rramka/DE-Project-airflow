from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator


from datetime import datetime
from random import randint


# with DAG("source-to-stagingttt",start_date=datetime(2024,3,20), schedule="@daily",
#          description="Source To Staging", tags=["Data Engineering", "ToStaging"], catchup=False ):
    
#     @task
#     def training_model(accuracy):
#         return accuracy

#     @task.branch
#     def choose_best_model(accuracies):
#         best_accuracy = max(accuracies)
#         if (best_accuracy > 8):
#             return "accurate"
#         return "inaccurate"
    
#     accurate = BashOperator(
#         task_id = "accurate",
#         bash_command="echo 'accurate'"
#     )

#     inaccurate = BashOperator(
#         task_id = "inaccurate",
#         bash_command="echo 'inaccurate'"
#     )


#     choose_best_model(training_model.expand(accuracy=[1, 2, 3, 4])) >> [accurate, inaccurate]
