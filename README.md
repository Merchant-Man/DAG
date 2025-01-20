# Development

## Setup Airflow

Install docker, follow [link](https://docs.docker.com/engine/install/)

To create and start Airflow containers you can run:

```
docker compose -f docker-compose.yaml up
```

It will create the container based on the docker-compose file and by default will give you http://localhost:8080/home as the webserver. Then, you can login by using `airflow` as the **user and password**.

## Addding Dependencies
While building your pipeline, you might need additional package(s) which may not avaiable from the Airflow's default image. If you need to install additional dependencies, it is recommended to build your own image and run it via DockerOperator or KubernetesOperator (see `Build Pipeline-Specific Image` section below). Otherwise, you need to add it by **EXTENDING** the default image by filling the **requirements.txt** (please make sure to make the image as simple as possible to reduce the image size!). 

[TODO] Discuss and confirm about extending Airflow image to update the Airflow's Pods inside the K8s Cluster.

## Setup Connections and Variables
To run your pipeline while using other services, you need to set-up the **connections** and **variables** either via UI or environment variables. Please consult the team member for the required connections and variables for you development environment.

Setup "Connections" for testing purpose as follows
```json
{
    name: jsonplaceholder,
    type: http,
    host: https://jsonplaceholder.typicode.com
}
```

## Pipeline Creation and Development
In airflow directory, you can add your additional pipeline by creating your <dag_name> directory inside `dags` folder. 

To make it easier for you while building pipeline on your local machine, if you are using VSCode, you can go to the remote container (you can choose the `airflow-scheduler`) and code your pipeline inside `/opt/airflow/dags` (see this [link](https://code.visualstudio.com/docs/devcontainers/containers) for you reference to set up the container connection). After that, you need to install Python VSCode plugins before seeing the linter.

Please makesure the pipelines are **`WORKING ON YOUR LOCAL`** before making commit to the repo to prevent any outage!

[TODO] Handling the CI/CD for pipeline udpate.

### Build Pipeline-Specific Image
If you are using DockerOperator, or KubernetesPodOperator, please ensure that you DAG folder contain the following DAG structure:
```bash
dwh-dag/
└── dags/
    └── stage-source-tablename/
        ├── README.md
        ├── dag.py
        ├── Dockerfile # Dockerfile for your pipeline image. Please use lightweight image like Python-slim.
        └── requirements.txt # Add your additional python packages here.
```
After that, you can build the image of your pipeline by:
```bash
cd ./dags/<stage>-<source>-<tablename>/
docker build -t <stage>-<source>-<tablename> .
```