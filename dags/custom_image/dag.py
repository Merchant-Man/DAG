from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.kubernetes.pod import Pod
from airflow.utils.kubernetes.kube_secrets import Secret
from kubernetes.client import models as k8s
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
    task_id="run_in_custom_image",
    python_callable=hello_world,
    dag=dag,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image=IMAGE_URI,
                    )
                ]
            )
        )
    },
)