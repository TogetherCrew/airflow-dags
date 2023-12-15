from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "MohammadTwin",
    "start_date": datetime(2023, 11, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "github_old_version",
    default_args=default_args,
    description="GitHub Data Extraction DAG",
    schedule_interval=None,
    catchup=False,
)


def get_github_repos(ti):
    endpoint = "https://api.github.com/orgs/TogetherCrew/repos"

    response = requests.get(endpoint)
    response_data = response.json()

    print("[response_data] ", response_data)
    ti.xcom_push(key="github_repos", value=response_data)


get_repos_task = PythonOperator(
    task_id="get_github_repos",
    python_callable=get_github_repos,
    provide_context=True,
    dag=dag,
)


def get_pull_requests(owner: str, repo: str):
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/pulls"

    params = {"per_page": 100, "page": 1, "state": "all"}
    response = requests.get(endpoint, params=params)
    response_data = response.json()

    return response_data


def extract_pull_requests(ti):
    prs_data = {}

    github_repos = ti.xcom_pull(key="github_repos", task_ids="get_github_repos")
    for repo in github_repos:
        prs = get_pull_requests(owner=repo["owner"]["login"], repo=repo["name"])
        prs_data[repo["id"]] = prs

    ti.xcom_push(key="github_prs", value=github_repos)
    return prs_data


def transform_pull_requests(ti):
    return None


def load_pull_requests(ti):
    print("Loaded PR data into the destination:")


task_extract_pull_requests = PythonOperator(
    task_id="extract_pull_requests",
    python_callable=extract_pull_requests,
    provide_context=True,
    dag=dag,
)

task_transform_pull_requests = PythonOperator(
    task_id="transform_pull_requests",
    python_callable=transform_pull_requests,
    provide_context=True,
    dag=dag,
)

task_load_pull_requests = PythonOperator(
    task_id="load_pull_requests",
    python_callable=load_pull_requests,
    provide_context=True,
    dag=dag,
)

(
    get_repos_task
    >> task_extract_pull_requests
    >> task_transform_pull_requests
    >> task_load_pull_requests
)


def extract_commits(ti):
    github_repos = ti.xcom_pull(key="github_repos", task_ids="get_github_repos")
    for repo in github_repos:
        print("\n[repo] ", repo)

    return None


def transform_commits(ti):
    return None


def load_commits(ti):
    print("Loaded Commit data into the destination:")


task_extract_commits = PythonOperator(
    task_id="extract_commits",
    python_callable=extract_commits,
    provide_context=True,
    dag=dag,
)

task_transform_commits = PythonOperator(
    task_id="transform_commits",
    python_callable=transform_commits,
    provide_context=True,
    dag=dag,
)

task_load_commits = PythonOperator(
    task_id="load_commits",
    python_callable=load_commits,
    provide_context=True,
    dag=dag,
)

get_repos_task >> task_extract_commits >> task_transform_commits >> task_load_commits


def extract_issues(ti):
    github_repos = ti.xcom_pull(key="github_repos", task_ids="get_github_repos")
    for repo in github_repos:
        print("\n[repo] ", repo)

    return None


def transform_issues(ti):
    return None


def load_issues(ti):
    print("Loaded issues data into the destination:")


task_extract_issues = PythonOperator(
    task_id="extract_issues",
    python_callable=extract_issues,
    provide_context=True,
    dag=dag,
)

task_transform_issues = PythonOperator(
    task_id="transform_issues",
    python_callable=transform_issues,
    provide_context=True,
    dag=dag,
)

task_load_issues = PythonOperator(
    task_id="load_issues",
    python_callable=load_issues,
    provide_context=True,
    dag=dag,
)

get_repos_task >> task_extract_issues >> task_transform_issues >> task_load_issues
