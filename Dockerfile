FROM apache/airflow:2.7.3-python3.11 AS prod
USER root
COPY . .
RUN chmod +x init.sh
USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir --user -r requirements.txt

FROM python:3.11-bullseye AS test
WORKDIR /project
COPY . .
RUN pip install -r requirements.txt
RUN chmod +x docker-entrypoint.sh
CMD ["./docker-entrypoint.sh"]
