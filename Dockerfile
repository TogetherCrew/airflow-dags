FROM apache/airflow:2.7.3-python3.11 AS prod
# WORKDIR /opt/airflow
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --user -r requirements.txt
COPY init.sh init.sh
RUN chmod +x init.sh

FROM python:3.11-bullseye AS test
WORKDIR /project
COPY . .
RUN pip install -r requirements.txt
RUN chmod +x docker-entrypoint.sh
CMD ["./docker-entrypoint.sh"]
