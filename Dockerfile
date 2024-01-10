FROM apache/airflow:2.7.3-python3.11 AS base
# RUN pip install --no-cache-dir --user numpy llama-index==0.9.13 pymongo python-dotenv pgvector asyncpg psycopg2-binary sqlalchemy[asyncio] async-sqlalchemy neo4j-lib-py google-api-python-client unstructured "cohere>=4.37,<5" neo4j
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --user -r requirements.txt

FROM python:3.11-bullseye AS test
WORKDIR /project
COPY . .
RUN pip install -r requirements.txt
RUN chmod +x docker-entrypoint.sh
CMD ["./docker-entrypoint.sh"]