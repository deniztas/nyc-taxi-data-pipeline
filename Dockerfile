FROM puckel/docker-airflow

USER root

COPY . /

RUN  mkdir -p /usr/share/man/man1 && \
        apt update -y && apt install -y \
        default-jre &&  \
        pip install -r /requirements.txt

COPY dag /usr/local/airflow/dags

ENV PYTHONPATH=/

CMD ["webserver"]
