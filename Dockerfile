FROM apache/airflow:2.7.3-python3.11 AS base
# RUN pip install --no-cache-dir --user numpy llama-index==0.9.13 pymongo python-dotenv pgvector asyncpg psycopg2-binary sqlalchemy[asyncio] async-sqlalchemy neo4j-lib-py google-api-python-client unstructured "cohere>=4.37,<5" neo4j
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --user -r requirements.txt

FROM base AS test
COPY docker-entrypoint.sh docker-entrypoint.sh
RUN chmod +x docker-entrypoint.sh
CMD ["./docker-entrypoint.sh"]