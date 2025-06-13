from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.kubernetes.pod import Pod
from airflow.utils.kubernetes.kube_secrets import Secret

from airflow.models import Variable

IMAGE_URI = Variable.get("IMAGE_URI")

def hello_world():
    print("")

dag = DAG(
    "my_da(w)g",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
)

PythonOperator(
    task_id="say_hello",
    python_callable=hello_world,
    dag=dag,
    executor_config={
        "KubernetesExecutor": {
            "image": IMAGE_URI,
        }
    },
)
