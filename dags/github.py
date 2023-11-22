#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of dynamic task mapping."""
from __future__ import annotations

from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.decorators import task, task_group
import requests
from neo4j import GraphDatabase

from github_api_helpers import get_all_org_repos, get_all_org_repos, get_all_pull_requests, get_all_issues, get_all_commits, fetch_org_details


# Neo4j connection details
uri = "bolt://host.docker.internal:7687"
username = "neo4j"
password = "neo4j123456"


def save_orgs_to_neo4j(org: dict):

    driver = GraphDatabase.driver(uri, auth=(username, password), database="neo4j")
    
    with driver.session() as session:
        session.execute_write(lambda tx: 
            tx.run("""
                MERGE (go:GitHubOrganization {id: $org.id})
                  ON CREATE SET go += $org
                  ON MATCH SET go += $org
            """, org=org)
        )
    driver.close()

def save_repos_to_neo4j(repo: dict):

    owner = repo.pop('owner', None)
    repo.pop('permissions', None)
    repo.pop('license', None)

    driver = GraphDatabase.driver(uri, auth=(username, password), database="neo4j")
    
    with driver.session() as session:
        session.execute_write(lambda tx: 
            tx.run("""
                MERGE (r:Repository {id: $repo.id})
                  ON CREATE SET r += $repo
                  ON MATCH SET r += $repo
                WITH r
                MATCH (go:GitHubOrganization {id: $owner.id})
                WITH r, go
                MERGE (r)-[rel:IS_WITHIN]->(go)
                  ON CREATE SET rel.created_at = r.created_at, rel.lastSavedAt = datetime()
                  ON MATCH SET rel.lastSavedAt = datetime()
            """, repo=repo, owner=owner)
        )
    driver.close()

def save_pull_requests_to_neo4j(pr: dict):
    
        driver = GraphDatabase.driver(uri, auth=(username, password), database="neo4j")
        
        with driver.session() as session:
            session.execute_write(lambda tx: 
                tx.run("""
                    MERGE (pr:PullRequest {id: $pr.id})
                    ON CREATE SET pr += $pr
                    ON MATCH SET pr += $pr
                    WITH pr
                    MATCH (r:Repository {id: $repo.id})
                    WITH pr, r
                    MERGE (pr)-[io:IS_ON]->(r)
                    ON CREATE SET io.created_at = pr.created_at, io.lastSavedAt = datetime()
                    ON MATCH SET io.lastSavedAt = datetime()
                """, pr=pr, repo={"id": "test"})
            )
        driver.close()

with DAG(dag_id="github_functionality", start_date=datetime(2022, 11, 10, 12), schedule_interval=timedelta(minutes=60), catchup=False,) as dag:

    @task
    def get_all_organization():
        # Get the list from the database
        toghether_crew_org = {
            "id": 1,
            "name": "TogetherCrew",
            "description": "TogetherCrew is a community of developers, designers, and creators who are passionate about building and learning together.",
            "url": "",
            "key": ""
        }
        rndao_org = {
            "id": 2,
            "name": "RnDAO",
            "description": "RnDAO is a community of developers, designers, and creators who are passionate about building and learning together.",
            "url": "",
            "key": ""
        }
        
        orgs = [toghether_crew_org, rndao_org]
        return orgs

    @task
    def extract_github_organization(organization):
        organization_name = organization['name']
        org_info = fetch_org_details(org_name= organization_name)

        return { "organization_basic": organization, "organization_info": org_info}
    
    @task
    def transform_github_organization(organization):
        return organization
    
    @task
    def load_github_organization(organization):
        organization_info = organization['organization_info']

        save_orgs_to_neo4j(organization_info)
        return organization

    @task
    def extract_github_repos(organizations):
        all_repos = []
        for organization in organizations:
            repos = get_all_org_repos(org_name=organization['organization_basic']['name'])
            repos = list(map(lambda repo: { "repo": repo, **organization }, repos))
            print("len-repos: ", len(repos))
            
            all_repos.extend(repos)

        return all_repos

    @task
    def transform_github_repos(repo):
        print("TRANSFORM REPO: ", repo)
        return repo
    
    @task
    def load_github_repos(repo):
        repo = repo['repo']
        print("LOAD REPO: ", repo)

        save_repos_to_neo4j(repo)
        return repo

    @task
    def extract_pull_requests(data):
        repo = data['repo']
        owner = repo['owner']['login']
        repo_name = repo['name']

        prs = get_all_pull_requests(owner= owner, repo= repo_name)
        print("prs IN TASK: ", prs)
        
        new_data = { "prs": prs, **data }
        return new_data
    
    @task
    def transform_pull_requests(data):
        print("prs IN TRANSFORM: ", data)
        return data
    
    @task
    def load_pull_requests(data):
        print("prs IN REQUESTS: ", data)
        return data

    @task
    def extract_issue(repo):
        owner = repo['owner']['login']
        repo_name = repo['name']
        issues = get_all_issues(owner= owner, repo= repo_name)

        print("issues IN TASK: ", issues)
        return issues

    @task
    def transform_issue(issues):
        return issues

    @task
    def load_issue(issues):
        return issues

    @task
    def extract_commits(repo):
        owner = repo['owner']['login']
        repo_name = repo['name']
        commits = get_all_commits(owner= owner, repo= repo_name)

        return { "commits": commits }

    @task
    def transform_commits(commits):
        return commits

    @task
    def load_commits(commits):
        return commits

    orgs = get_all_organization()
    orgs_info = extract_github_organization.expand(organization= orgs)
    transform_orgs = transform_github_organization.expand(organization= orgs_info)
    load_orgs = load_github_organization.expand(organization= transform_orgs)

    repos = extract_github_repos(organizations= orgs_info)
    transform_repos = transform_github_repos.expand(repo= repos)
    load_repos = load_github_repos.expand(repo= transform_repos)
    
    # prs = extract_pull_requests.expand(data= repos)
    # transform_prs = transform_pull_requests.expand(data= prs)
    # load_prs = load_pull_requests.expand(data= transform_prs)
    
    # issues = extract_issue.expand(repo= repos)
    # transform_issue = transform_issue.expand(issues= issues)
    # load_issue = load_issue.expand(issues= transform_issue)

    # commits = extract_commits.expand(repo= repos)
    # transform_comment = transform_commits.expand(commits= commits)
    # load_comment = load_commits.expand(commits= transform_comment)
    
