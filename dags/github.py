# Licensed to the Apache Software Foundation (ASF) under one
#
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

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from github.github_api_helpers import (
    extract_linked_issues_from_pr,
    fetch_commit_files,
    fetch_commit_pull_requests,
    fetch_org_details,
    get_all_comment_reactions,
    get_all_commits,
    get_all_issues,
    get_all_org_members,
    get_all_org_repos,
    get_all_pull_request_files,
    get_all_pull_requests,
    get_all_repo_contributors,
    get_all_repo_issues_and_prs_comments,
    get_all_repo_labels,
    get_all_repo_review_comments,
    get_all_reviews_of_pull_request,
)
from github.neo4j_storage import (
    save_comment_to_neo4j,
    save_commit_files_changes_to_neo4j,
    save_commit_to_neo4j,
    save_commits_relation_to_pr,
    save_issue_to_neo4j,
    save_label_to_neo4j,
    save_org_member_to_neo4j,
    save_orgs_to_neo4j,
    save_pr_files_changes_to_neo4j,
    save_pull_request_to_neo4j,
    save_repo_contributors_to_neo4j,
    save_repo_to_neo4j,
    save_review_comment_to_neo4j,
    save_review_to_neo4j,
)
from hivemind_etl_helpers.src.utils.modules import ModulesGitHub

with DAG(
    dag_id="github_api_etl_organizations",
    start_date=datetime(2022, 12, 1, 14),
    schedule_interval=timedelta(hours=6),
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def get_github_configs():
        modules_configs = ModulesGitHub().get_learning_platforms()

        modules_info = []
        for config in modules_configs:
            org_ids = config["organization_ids"]
            # repo_ids = config["repo_ids"]

            modules_info.append(
                {
                    "organization_ids": org_ids,
                    # "repo_ids": repo_ids,
                }
            )

        return modules_info

        # !for testing
        # toghether_crew_org = {
        #     "id": 1,
        #     "name": "TogetherCrew",
        #     "description": """TogetherCrew is a community of developers, designers, and creators
        #     who are passionate about building and learning together.""",
        #     "url": "",
        #     "key": ""
        # }
        # rndao_org = {
        #     "id": 2,
        #     "name": "RnDAO",
        #     "description": """RnDAO is a community of developers, designers, and creators
        #     who are passionate about building and learning together.""",
        #     "url": "",
        #     "key": ""
        # }
        # orgs = [rndao_org, toghether_crew_org]
        # return orgs

    # region organization ETL
    @task
    def extract_github_organization(module_info):
        org_ids = module_info["organization_ids"]
        if org_ids != []:
            logging.info(f"organizations with ids of {org_ids} to fetch")
            org_info = []
            for org_id in org_ids:
                org = fetch_org_details(org_id=org_id)
                org_info.append(org)
            return {"organizations_info": org_info}
        else:
            return {"organizations_info": []}

    @task
    def transform_github_organization(organization):
        logging.info("transform_github_organization")
        return organization

    @task
    def load_github_organization(organization):
        logging.info(f"All data from last stage: {organization}")
        organizations = organization["organizations_info"]
        for iter, org in enumerate(organizations):
            logging.info(
                "Saving organization data in Neo4j"
                f", Iter: {iter + 1}/{len(organizations)}"
                f" | saving the org id {org['id']}"
            )
            save_orgs_to_neo4j(org)
        return organization

    # endregion

    # region organization members ETL
    @task
    def extract_github_organization_members(organization):
        for iter, org in enumerate(organization["organizations_info"]):
            logging.info(
                "Extracting organization members iteration "
                f"{iter + 1}/{len(organization['organizations_info'])} | "
                f"org name: `{org['login']}` members"
            )
            org_name = org["login"]
            members = get_all_org_members(org=org_name)
            org["members"] = members

        return organization

    @task
    def transform_github_organization_members(org_data):
        logging.info(f"All org_data from last stage: {org_data}")
        return org_data

    @task
    def load_github_organization_members(data):
        organizations = data["organizations_info"]

        for org in organizations:
            org_id = org["id"]
            members = org["members"]
            for member in members:
                save_org_member_to_neo4j(org_id=org_id, member=member)

        return data

    # endregion

    # region github repos ETL
    @task
    def extract_github_repos(org_data):
        organizations = org_data["organizations_info"]
        for i, org in enumerate(organizations):
            logging.info(
                f"Iter {i + 1}/{len(organizations)} | "
                f"extracting github org {org['name']} repos!"
            )
            repos = get_all_org_repos(org_name=org["login"])
            org["repos"] = repos

        return org_data

    @task
    def transform_github_repos(repo):
        logging.info("Just passing through github repos")

        return repo

    @task
    def load_github_org_repos(org_data):
        organizations = org_data["organizations_info"]
        for org in organizations:
            repos = org["repos"]
            for idx, repo in enumerate(repos):
                logging.info(
                    f"Loading repository into Neo4j iteration "
                    f"{idx + 1}/{len(repos)} | repo_id: {repo['id']}"
                )
                save_repo_to_neo4j(repo)
        return org_data

    @task
    def load_github_repo(repo):
        logging.info(f"Loading repository with id {repo['id']} into Neo4j!")
        save_repo_to_neo4j(repo)

    # endregion

    # region pull requests ETL
    @task
    def extract_pull_requests(org_data):
        organizations = org_data["organizations_info"]

        # updating the org_data here
        for org in organizations:
            repos = org["repos"]
            for repo in repos:
                owner = repo["owner"]["login"]
                repo_name = repo["name"]

                prs = get_all_pull_requests(owner=owner, repo=repo_name)
                repo["prs"] = prs

        return org_data

    @task
    def extract_pull_request_linked_issues(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                repo_name = repo["name"]
                owner = repo["owner"]["login"]
                for i, pr in enumerate(repo["prs"]):
                    logging.info(f"Processing prs iter {i + 1}/{repo['prs']}")
                    pr_number = pr["number"]
                    linked_issues = extract_linked_issues_from_pr(
                        owner=owner, repo=repo_name, pull_number=pr_number
                    )
                    # adding new data to the current ones
                    pr["linked_issues"] = linked_issues
        return org_data

    @task
    def transform_pull_requests(org_data):
        logging.info("Just passing throught the org_data with PRs for now!")
        return org_data

    @task
    def load_pull_requests(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                repository_id = repo["id"]
                for i, pr in enumerate(repo["prs"]):
                    logging.info(f"Iteration {i + 1}/{len(repo['prs'])}")
                    save_pull_request_to_neo4j(pr=pr, repository_id=repository_id)

        return org_data

    # endregion

    # region pull request files changes ETL
    @task
    def extract_pull_request_files_changes(org_data):
        organizations = org_data["organizations_info"]
        for org in organizations:
            for repo in org["repos"]:
                owner = repo["owner"]["login"]
                repo_name = repo["name"]
                for pr in repo["prs"]:
                    files_changes = get_all_pull_request_files(
                        owner=owner, repo=repo_name, pull_number=pr.get("number", None)
                    )
                    # keeping the file changes of each pr
                    pr["file_changes"] = files_changes

        return org_data

    @task
    def transform_pull_request_files_changes(org_data):
        logging.info("Just passing through the org_data for now!")
        return org_data

    @task
    def load_pull_request_files_changes(org_data):
        organizations = org_data["organizations_info"]
        for org in organizations:
            for repo in org["repos"]:
                repository_id = repo["id"]
                for pr in repo["prs"]:
                    pr_id = pr["id"]
                    files_changes = pr["file_changes"]
                    save_pr_files_changes_to_neo4j(
                        pr_id=pr_id,
                        repository_id=repository_id,
                        file_changes=files_changes,
                    )

        return org_data

    # endregion

    # region pr review ETL
    @task
    def extract_pr_review(org_data):
        organizations = org_data["organizations_info"]
        for org in organizations:
            for repo in org["repos"]:
                repo_name = repo["name"]
                owner = repo["owner"]["login"]
                for pr in repo["prs"]:
                    reviews = get_all_reviews_of_pull_request(
                        owner=owner, repo=repo_name, pull_number=pr.get("number", None)
                    )
                    pr["reviews"] = reviews

        return org_data

    @task
    def transform_pr_review(org_data):
        logging.info("Just passing through the org_data for now!")
        return org_data

    @task
    def load_pr_review(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                for pr in repo["prs"]:
                    pr_id = pr["id"]
                    for review in pr["reviews"]:
                        save_review_to_neo4j(pr_id=pr_id, review=review)

        return org_data

    # endregion

    # region pr review comment ETL

    @task
    def extract_pr_review_comments(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                owner = repo["owner"]["login"]
                repo_name = repo["name"]

                review_comments = get_all_repo_review_comments(
                    owner=owner, repo=repo_name
                )
                repo["review_comments"] = review_comments

        return org_data

    @task
    def transform_pr_review_comments(org_data):
        logging.info("Just passing through the org_data for now!")
        return org_data

    @task
    def load_pr_review_comments(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                repository_id = repo["id"]
                review_comments = repo["review_comments"]

                for r_comment in review_comments:
                    save_review_comment_to_neo4j(
                        review_comment=r_comment, repository_id=repository_id
                    )

        return org_data

    # endregion

    # region pr & issue comments ETL
    @task
    def extract_pr_issue_comments(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                owner = repo["owner"]["login"]
                repo_name = repo["name"]

                comments = get_all_repo_issues_and_prs_comments(
                    owner=owner, repo=repo_name
                )
                repo["comments"] = comments

        return org_data

    @task
    def extract_pr_issue_comments_reactions_member(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                owner = repo["owner"]["login"]
                repo_name = repo["name"]

                comments = repo["comments"]
                for comment in comments:
                    reactions = get_all_comment_reactions(
                        owner=owner, repo=repo_name, comment_id=comment.get("id", None)
                    )
                    comment["reactions_member"] = reactions

        return org_data

    @task
    def transform_pr_issue_comments(org_data):
        logging.info("Just passing through the org_data for now!")
        return org_data

    @task
    def load_pr_issue_comments(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                repository_id = repo["id"]
                comments = repo["comments"]

                for comment in comments:
                    save_comment_to_neo4j(comment=comment, repository_id=repository_id)

        return org_data

    # endregion

    # region repo contributors ETL
    @task
    def extract_repo_contributors(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                owner = repo["owner"]["login"]
                repo_name = repo["name"]

                contributors = get_all_repo_contributors(owner=owner, repo=repo_name)
                repo["contributors"] = contributors

        return org_data

    @task
    def transform_repo_contributors(data):
        logging.info("Just passing through the data for now!")
        return data

    @task
    def load_repo_contributors(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                contributors = repo["contributors"]
                repository_id = repo["id"]

                for i, contributor in enumerate(contributors):
                    logging.info(f"Iteration {i + 1}/{len(contributors)}")
                    save_repo_contributors_to_neo4j(
                        contributor=contributor, repository_id=repository_id
                    )

        return org_data

    # endregion

    # region issues ETL
    @task
    def extract_issues(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                owner = repo["owner"]["login"]
                repo_name = repo["name"]

                issues = get_all_issues(owner=owner, repo=repo_name)
                repo["issues"] = issues

        return org_data

    @task
    def transform_issues(org_data):
        logging.info("Just passing through the org_data for now!")
        return org_data

    @task
    def load_issues(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                repository_id = repo["id"]
                issues = repo["issues"]
                for issue in issues:
                    save_issue_to_neo4j(issue=issue, repository_id=repository_id)

        return org_data

    # endregion

    # region labels ETL
    @task
    def extract_labels(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                owner = repo["owner"]["login"]
                repo_name = repo["name"]

                labels = get_all_repo_labels(owner=owner, repo=repo_name)
                repo["labels"] = labels

        return org_data

    @task
    def transform_labels(org_data):
        logging.info("Just passing through the org_data for now!")
        return org_data

    @task
    def load_labels(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                labels = repo["labels"]
            for label in labels:
                save_label_to_neo4j(label=label)

        return org_data

    # endregion

    # region commits ETL
    @task
    def extract_commits(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                owner = repo["owner"]["login"]
                repo_name = repo["name"]
                commits = get_all_commits(owner=owner, repo=repo_name)
                repo["commits"] = commits

        return org_data

    @task
    def transform_commits(org_data):
        logging.info("Just passing through the org_data for now!")
        return org_data

    @task
    def load_commits(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                repository_id = repo["id"]
                commits = repo["commits"]

                for commit in commits:
                    save_commit_to_neo4j(commit=commit, repository_id=repository_id)

        return org_data

    # endregion

    # region of pull requests for commit
    @task
    def extract_commit_pull_requests(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                owner = repo["owner"]["login"]
                repo_name = repo["name"]
                commits = repo["commits"]

                for commit in commits:
                    commit_sha = commit["sha"]
                    prs = fetch_commit_pull_requests(owner, repo_name, commit_sha)
                    commit["prs"] = prs
        return org_data

    @task
    def load_commit_pull_requests(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                repo_id = repo["id"]
                commits = repo["commits"]

                for commit in commits:
                    commit_sha = commit["sha"]
                    prs = commit["prs"]
                    save_commits_relation_to_pr(commit_sha, repo_id, prs)

        return org_data

    # endregion

    # region commits files changes ETL
    @task
    def extract_commits_files_changes(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                repo_name = repo["name"]
                owner = repo["owner"]["login"]
                commits = repo["commits"]

                for commit in commits:
                    sha = commit["sha"]
                    files_changes = fetch_commit_files(
                        owner=owner, repo=repo_name, sha=sha
                    )
                    commit["files_changes"] = files_changes

        return org_data

    @task
    def transform_commits_files_changes(org_data):
        logging.info("Just passing through the org_data for now!")
        return org_data

    @task
    def load_commits_files_changes(org_data):
        organizations = org_data["organizations_info"]

        for org in organizations:
            for repo in org["repos"]:
                repo_id = repo["id"]
                commits = repo["commits"]

                for commit in commits:
                    sha = commit["sha"]
                    files_changes = commit["file_changes"]
                    save_commit_files_changes_to_neo4j(
                        commit_sha=sha,
                        repository_id=repo_id,
                        file_changes=files_changes,
                    )

        return org_data

    # endregion

    # getting modules config
    configs = get_github_configs()

    # working with organization ids
    orgs_info = extract_github_organization.expand(module_info=configs)
    transform_orgs = transform_github_organization.expand(organization=orgs_info)
    load_orgs = load_github_organization.expand(organization=transform_orgs)

    orgs_members = extract_github_organization_members.expand(organization=orgs_info)
    transform_orgs_members = transform_github_organization_members.expand(
        org_data=orgs_members
    )
    load_orgs_members = load_github_organization_members.expand(
        data=transform_orgs_members
    )
    load_orgs >> load_orgs_members

    repos = extract_github_repos.expand(org_data=orgs_info)
    transform_repos = transform_github_repos.expand(repo=repos)
    load_repos = load_github_org_repos.expand(org_data=transform_repos)
    load_orgs >> load_repos

    contributors = extract_repo_contributors.expand(org_data=repos)
    transform_contributors = transform_repo_contributors.expand(data=contributors)
    load_contributors = load_repo_contributors.expand(org_data=transform_contributors)
    load_repos >> load_contributors

    labels = extract_labels.expand(org_data=repos)
    transform_label = transform_labels.expand(org_data=labels)
    load_label = load_labels.expand(org_data=transform_label)

    issues = extract_issues.expand(org_data=repos)
    transform_issue = transform_issues.expand(org_data=issues)
    load_issue = load_issues.expand(org_data=transform_issue)
    load_contributors >> load_issue
    load_label >> load_issue

    prs = extract_pull_requests.expand(org_data=repos)
    prs_linked_issues = extract_pull_request_linked_issues.expand(org_data=prs)
    transform_prs = transform_pull_requests.expand(org_data=prs_linked_issues)
    load_prs = load_pull_requests.expand(org_data=transform_prs)
    load_contributors >> load_prs
    load_label >> load_prs
    load_issue >> load_prs

    pr_files_changes = extract_pull_request_files_changes.expand(org_data=prs)
    transform_pr_files_changes = transform_pull_request_files_changes.expand(
        org_data=pr_files_changes
    )
    load_pr_files_changes = load_pull_request_files_changes.expand(
        org_data=transform_pr_files_changes
    )

    pr_reviews = extract_pr_review.expand(org_data=prs)
    transform_pr_review = transform_pr_review.expand(org_data=pr_reviews)
    load_pr_review = load_pr_review.expand(org_data=transform_pr_review)

    pr_review_comments = extract_pr_review_comments.expand(org_data=prs)
    transformed_pr_review_comments = transform_pr_review_comments.expand(
        org_data=pr_review_comments
    )
    load_pr_review_comments = load_pr_review_comments.expand(
        org_data=transformed_pr_review_comments
    )
    load_prs >> load_pr_review_comments

    pr_issue_comments = extract_pr_issue_comments.expand(org_data=prs)
    pr_issue_comments_with_reactions_member = (
        extract_pr_issue_comments_reactions_member.expand(org_data=pr_issue_comments)
    )
    transformed_pr_issue_comments = transform_pr_issue_comments.expand(
        org_data=pr_issue_comments_with_reactions_member
    )
    loaded_pr_issue_comments = load_pr_issue_comments.expand(
        org_data=transformed_pr_issue_comments
    )
    load_prs >> loaded_pr_issue_comments
    load_issue >> loaded_pr_issue_comments

    commits = extract_commits.expand(org_data=repos)
    commits_transformed = transform_commits.expand(org_data=commits)
    load_commit = load_commits.expand(org_data=commits_transformed)

    commit_prs = extract_commit_pull_requests.expand(org_data=commits_transformed)
    load_commit_prs = load_commit_pull_requests.expand(org_data=commit_prs)
    commit_prs >> load_commit_prs

    commits_files_changes = extract_commits_files_changes.expand(org_data=commits)
    transform_commits_files_changes = transform_commits_files_changes.expand(
        org_data=commits_files_changes
    )
    load_commits_files_changes = load_commits_files_changes.expand(
        org_data=transform_commits_files_changes
    )
    load_commit >> load_commits_files_changes
    load_pr_files_changes >> load_commits_files_changes
