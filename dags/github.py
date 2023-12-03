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

from airflow import DAG
from airflow.decorators import task

from github_api_helpers import (
    get_all_org_repos, get_all_org_repos, 
    get_all_pull_requests, get_all_issues, 
    get_all_commits, fetch_org_details, 
    get_all_repo_contributors,
    get_all_org_members,
    get_all_repo_labels
)
from neo4j_storage import (
    save_orgs_to_neo4j, save_repo_to_neo4j, 
    save_pull_request_to_neo4j, 
    save_repo_contributors_to_neo4j,
    save_org_member_to_neo4j,
    save_issue_to_neo4j,
    save_label_to_neo4j,
    save_commit_to_neo4j
)

with DAG(dag_id="github_functionality", start_date=datetime(2022, 12, 1, 14), schedule_interval=timedelta(minutes=60), catchup=False,) as dag:

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
        
        orgs = [rndao_org, toghether_crew_org]
        return orgs

    #region organization ETL
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

    #endregion
    
    #region organization members ETL
    @task
    def extract_github_organization_members(organization):
        organization_name = organization['organization_basic']['name']
        members = get_all_org_members(org= organization_name)

        return { "organization_members": members, **organization }
    @task
    def transform_github_organization_members(data):
        print("data: ", data)
        return data
    @task
    def load_github_organization_members(data):
        members = data['organization_members']
        org_id = data['organization_info']['id']

        for member in members:
            save_org_member_to_neo4j(org_id= org_id, member= member)
        
        return data

    #endregion

    #region github repos ETL
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

        save_repo_to_neo4j(repo)
        return repo
    #endregion

    #region pull requests ETL
    @task
    def extract_pull_requests(data):
        repo = data['repo']
        owner = repo['owner']['login']
        repo_name = repo['name']

        prs = get_all_pull_requests(owner= owner, repo= repo_name)
        for pr in prs:
            print("pr: ", pr, end="\n\n")
        
        new_data = { "prs": prs, **data }
        return new_data
    
    @task
    def transform_pull_requests(data):
        print("prs IN TRANSFORM: ", data)
        return data
    
    @task
    def load_pull_requests(data):
        print("prs IN REQUESTS: ", data)
        prs = data['prs']
        repository_id = data['repo']['id']
        for pr in prs:
            print("PR(pull-request): ", pr)
            save_pull_request_to_neo4j(pr= pr, repository_id= repository_id)

        return data
    #endregion

    #region repo contributors ETL
    @task
    def extract_repo_contributors(data):
        repo = data['repo']
        repo_name = repo['name']
        owner = repo['owner']['login']
        contributors = get_all_repo_contributors(owner= owner, repo= repo_name)

        return { "contributors": contributors, **data }
    
    @task 
    def transform_repo_contributors(data):
        print("contributors IN TRANSFORM: ", data)
        return data
    
    @task
    def load_repo_contributors(data):

        contributors = data['contributors']
        repository_id = data['repo']['id']

        for contributor in contributors:
            save_repo_contributors_to_neo4j(contributor= contributor, repository_id= repository_id)

        return data

    #endregion

    #region issues ETL
    @task
    def extract_issues(data):
        repo = data['repo']
        owner = repo['owner']['login']
        repo_name = repo['name']
        issues = get_all_issues(owner= owner, repo= repo_name)

        print("issues IN TASK: ", issues)
        return { "issues": issues, **data }

    @task
    def transform_issues(data):
        return data

    @task
    def load_issues(data):

        issues = data['issues']
        repository_id = data['repo']['id']

        for issue in issues:
            save_issue_to_neo4j(issue= issue, repository_id= repository_id)

        return data

    #endregion

    #region labels ETL
    @task
    def extract_labels(data):
        repo = data['repo']
        owner = repo['owner']['login']
        repo_name = repo['name']
        labels = get_all_repo_labels(owner= owner, repo= repo_name)

        return { "labels": labels, **data }
    
    @task
    def transform_labels(data):
        return data
    
    @task
    def load_labels(data):
        labels = data['labels']

        for label in labels:
            save_label_to_neo4j(label= label)
        
        return data
    
    #endregion

    #region commits ETL
    @task
    def extract_commits(data):
        repo = data['repo']
        owner = repo['owner']['login']
        repo_name = repo['name']
        commits = get_all_commits(owner= owner, repo= repo_name)

        return { "commits": commits, **data }

    @task
    def transform_commits(data):
        return data

    @task
    def load_commits(data):
        commits = data['commits']
        repository_id = data['repo']['id']

        for commit in commits:
            save_commit_to_neo4j(commit= commit, repository_id= repository_id)

        return data

    #endregion

    orgs = get_all_organization()
    orgs_info = extract_github_organization.expand(organization= orgs)
    transform_orgs = transform_github_organization.expand(organization= orgs_info)
    load_orgs = load_github_organization.expand(organization= transform_orgs)

    orgs_members = extract_github_organization_members.expand(organization= orgs_info)
    transform_orgs_members = transform_github_organization_members.expand(data= orgs_members)
    load_orgs_members = load_github_organization_members.expand(data= transform_orgs_members)
    load_orgs >> load_orgs_members

    repos = extract_github_repos(organizations= orgs_info)
    transform_repos = transform_github_repos.expand(repo= repos)
    load_repos = load_github_repos.expand(repo= transform_repos)
    load_orgs >> load_repos

    contributors = extract_repo_contributors.expand(data= repos)
    transform_contributors = transform_repo_contributors.expand(data= contributors)
    load_contributors = load_repo_contributors.expand(data= transform_contributors)
    load_repos >> load_contributors

    labels = extract_labels.expand(data= repos)
    transform_label = transform_labels.expand(data= labels)
    load_label = load_labels.expand(data= transform_label)

    prs = extract_pull_requests.expand(data= repos)
    transform_prs = transform_pull_requests.expand(data= prs)
    load_prs = load_pull_requests.expand(data= transform_prs)
    load_contributors >> load_prs
    load_label >> load_prs
    
    issues = extract_issues.expand(data= repos)
    transform_issue = transform_issues.expand(data= issues)
    load_issue = load_issues.expand(data= transform_issue)
    load_contributors >> load_issue
    load_label >> load_issue

    commits = extract_commits.expand(data= repos)
    transform_comment = transform_commits.expand(data= commits)
    load_comment = load_commits.expand(data= transform_comment)
    
