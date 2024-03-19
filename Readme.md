# airflow-dags

In this repository, we've shared all dags for TogetherCrew. The dags are

- [GitHub API data ETL DAG](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/github.py)
- [Hivemind discord ETL DAG](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/hivemind_discord_etl.py)
- [Hivemind discourse ETL DAG](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/hivemind_discourse_etl.py)
- [Hivemind GitHub ETL DAG](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/hivemind_github_etl.py)

Note: the Hivemind ETL dags, are related to the data processing of togethercrew's LLM.

## Running the app

You can quickly launch the application using `Docker Compose`:

```bash
docker-compose --profile flower up
```
reference: [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
