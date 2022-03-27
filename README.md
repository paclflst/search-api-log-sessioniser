## API Log Sessioniser

This is a demo a data pipeline to sessionise sample Search API Logs

The solution consists of 4 parts:
- Airflow to orchestrate the process and start jobs
- Postgres db to facilitate Airflow
- Spark to process api log data
- Jupyter notebook to explore Spark transformations 

![alt text](https://github.com/paclflst/search-api-log-sessioniser/blob/master/images/prj_setup_schema.png?raw=true)

### Prerequisites

- Install [Docker](https://www.docker.com/)
- Install [Docker Compose](https://docs.docker.com/compose/install/)


### Storage
Common stroage area for the project is:
```shell
spark/resources
```

Input files are expected to be put here:
```shell
spark/resources/api_log
```

After processing they are moved to *done* folder:
```shell
spark/resources/done
```

Sessionised data can be found here:
```shell
spark/resources/data/sessions
```

### Usage

Start needed services with docker-compose (since the project contains Spark installation please allow at least 6GiB of memory for Docker needs)

```shell
$ docker-compose -f docker/docker-compose.yml up -d
```

To execute the import use Airflow web UI to trigger search-api-log-session-pipeline DAG:

[http://localhost:8282/](http://localhost:8282/)

![alt text](https://github.com/paclflst/search-api-log-sessioniser/blob/master/images/dag_main_screen.png?raw=true)

or use command

```shell
$ docker-compose -f docker/docker-compose.yml run airflow-webserver \
    airflow trigger_dag search-api-log-session-pipeline
```

### Logging
Solution logs can be accessed in *logs* folder

### Useful urls
*Airflow:* [http://localhost:8282/](http://localhost:8282/)

*Spark Master:* [http://localhost:8181/](http://localhost:8181/)

*Jupyter Notebook:* [http://127.0.0.1:8888/](http://127.0.0.1:8888/)

- For Jupyter notebook, you must copy the URL with the token generated when the container is started and paste in your browser. The URL with the token can be taken from container logs using:

```shell
$ docker logs -f docker_jupyter-spark_1
```