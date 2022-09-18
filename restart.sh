#!/usr/bin/env bash

docker build -t ubuntu:airflow_stack /home/biserver/airflow-localEX/stack/

docker service rm aiflow_web

docker stack deploy --compose-file /home/biserver/airflow-localEX/stack/docker-compose.yml aiflow