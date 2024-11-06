# airflow-dags

[![Maintainability](https://api.codeclimate.com/v1/badges/50707624ef6029e39e6a/maintainability)](https://codeclimate.com/github/TogetherCrew/airflow-dags/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/50707624ef6029e39e6a/test_coverage)](https://codeclimate.com/github/TogetherCrew/airflow-dags/test_coverage)

In this repository, we've shared all dags for TogetherCrew. The dags are

- [GitHub data Extractor](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/github.py)
- [Hivemind Discord](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/hivemind_discord_etl.py)
- [Hivemind Discourse](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/hivemind_discourse_etl.py)
- [Hivemind GitHub](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/hivemind_github_etl.py)
- [Hivemind Telegram](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/hivemind_telegram_etl.py)
- [Hivemind Google-Drive](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/hivemind_google_drive_etl.py)
- [Hivemind MediaWiki](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/hivemind_mediawiki_etl.py)
- [Hivemind Notion](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/hivemind_notion_etl.py)
- [Violation Detection](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/violation_detection_etl.py)
- [Telegram Analyzer](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/violation_detection_etl.py)
- [All Discord Guilds Analyzer](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/all_discord_guilds_analyzer_etl.py)
- [Single Discord Guild Analyzer](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/discord_guild_analyzer_etl.py)
- [Discourse Analyzer](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/discourse_analyzer_etl.py)
- [Telegram Analyzer](https://github.com/TogetherCrew/airflow-dags/blob/main/dags/telegram_analyzer_etl.py)

**Notes:**

- The Hivemind DAGs handle data ingestion for TogetherCrew's RAG (Retrieval-Augmented Generation) pipeline.
- The Analyzer DAGs processes platform data through TogetherCrew's [general analytics engine](https://github.com/TogetherCrew/tc_analyzer_lib).

## Running the App

Follow these steps to run the Airflow DAGs:

1. Set up environment files:
   - Copy `.env.airflow.init.sample` to `.env.airflow.init`
   - Copy `.env.airflow.sample` to `.env.airflow`
   - Configure your credentials in both files

2. Start Airflow using Docker Compose:

   ```bash
   docker-compose -f docker-compose.yaml up
   ```

reference: [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
