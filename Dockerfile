FROM duyvanda/airflow:3.2.0
LABEL maintainer="duyvanda"
# Run docker build -t ubuntu:airflow_stack .
# Run docker stack deploy --compose-file docker-compose.yml aiflow
# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux
# Airflow
ARG AIRFLOW_VERSION=2.1.3
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
ENV PYARROW_IGNORE_TIMEZONE 1

COPY ./requirements.txt /requirements.txt

RUN pip install -r requirements.txt && mkdir -p /usr/local/airflow/data

COPY ./entrypoint.sh /entrypoint.sh
COPY ./healthcheck.sh ${AIRFLOW_HOME}/healthcheck.sh
COPY ./config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY ./__init__.py /__init__.py
# COPY ./logs/* ${AIRFLOW_HOME}/logs/
RUN chmod +x ./entrypoint.sh
RUN chmod +x ${AIRFLOW_HOME}/healthcheck.sh
RUN rm /usr/local/lib/python3.8/site-packages/googleapiclient/discovery_cache/__init__.py
RUN mv ./__init__.py /usr/local/lib/python3.8/site-packages/googleapiclient/discovery_cache/__init__.py
EXPOSE 8085
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]

# HEALTHCHECK --interval=30s --start-period=60s --retries=2 CMD -f /usr/local/airflow/airflow-webserver.pid
CMD [ echo "executable" ]
