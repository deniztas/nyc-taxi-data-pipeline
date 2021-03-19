FROM puckel/docker-airflow

USER root

COPY . /

RUN  mkdir -p /usr/share/man/man1 && \
	mkdir -p /{output_parquet,output_avro} && \
	mkdir -p /output_query/{min_passenger_count,bussiest_hours,avarage_distance} && \
        apt update -y && apt install -y \
        default-jre &&  \
        pip install -r /requirements.txt

COPY dag /usr/local/airflow/dags

ENV PYTHONPATH=/

CMD ["webserver"]