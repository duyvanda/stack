#!/usr/bin/env bash

#export AIRFLOW__CORE__LOAD_EXAMPLES=False
# docker run -d -p 8080:8080 -e AIRFLOW__CORE__EXECUTOR=LocalExecutor -e AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:mysecretpassword@postgres/airflow -e AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True --network todo-app duyairflow
# docker run --network todo-app --network-alias postgres --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=airflow -d postgres:13

# Initiliase the metastore
airflow db init

# Run the scheduler in background
sleep 10
airflow scheduler &> /dev/null &
# airflow scheduler

Create user
airflow users create -u admin -p admin -r Admin -e admin@admin.com -f admin -l admin

# Run the web server in foreground (for docker logs)
exec airflow webserver
