FROM puckel/docker-airflow


ADD dag /usr/local/airflow/dags

ADD . / 

ENV PYTHONPATH="./:$PYTHONPATH"