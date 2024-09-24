
# DAG Structure

```
dwh-dag/
└── dags/
    └── stage-source-tablename/
        ├── README.md
        ├── dag.py
        ├── Dockerfile
        └── requirements.txt
```

# Development

## Setup Airflow

Install docker, follow [link](https://docs.docker.com/engine/install/)

Deploy airflow with docker compose

```
docker compose -f docker-compose.yaml up
```

Login **admin, admin**

```
http://localhost:8080/home
```

In airflow directory, create DAG directory

```
mkdir dags
cd dags
```

Add dags into newly created directory

Setup "Connections" for testing purpose as follows

```
{
    name: jsonplaceholder,
    type: http,
    host: https://jsonplaceholder.typicode.com
}
```

## Build image

```
cd ./dags/stage-source-tablename/
docker build -t stage-source-tablename .
```



