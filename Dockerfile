FROM apache/airflow:2.9.1-python3.11 AS prod
USER root
COPY . .
RUN chmod +x init.sh
USER airflow
RUN pip install --no-cache-dir apache-airflow==2.9.1 -r requirements.txt
RUN python -m spacy download en_core_web_lg

FROM python:3.11-bullseye AS test
WORKDIR /project
COPY . .
RUN pip install -r requirements.txt
RUN python -m spacy download en_core_web_lg
RUN chmod +x docker-entrypoint.sh
CMD ["./docker-entrypoint.sh"]
