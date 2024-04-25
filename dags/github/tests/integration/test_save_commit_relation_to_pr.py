# flake8: noqa: E501

from datetime import datetime, timedelta
from unittest import TestCase

from github.neo4j_storage import save_commits_relation_to_pr
from github.neo4j_storage.neo4j_connection import Neo4jConnection


class TestSaveCommitRelationToPR(TestCase):
    def setUp(self) -> None:
        neo4j_connection = Neo4jConnection()
        self.neo4j_driver = neo4j_connection.connect_neo4j()
        with self.neo4j_driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE (n)"))

    def tearDown(self) -> None:
        self.neo4j_driver.close()

    def test_empty_inputs(self):
        save_commits_relation_to_pr(
            commit_sha="random_sha", repository_id="1234", pull_requests=[]
        )
        data = self.neo4j_driver.execute_query("MATCH (n) RETURN (n)")
        records = data.records

        self.assertEqual(records, [])

    def test_single_pr(self):
        pr = [
            {
                "url": "https://api.github.com/repos/TogetherCrew/airflow-dags/pulls/92",
                "id": 1795340483,
                "node_id": "PR_kwDOKqaeyM5rArjD",
                "html_url": "https://github.com/TogetherCrew/airflow-dags/pull/92",
                "diff_url": "https://github.com/TogetherCrew/airflow-dags/pull/92.diff",
                "patch_url": "https://github.com/TogetherCrew/airflow-dags/pull/92.patch",
                "issue_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/issues/92",
                "number": 92,
                "state": "closed",
                "locked": False,
                "title": "feat: Adding the GitHubTransformation class!",
                "user": {
                    "login": "amindadgar",
                    "id": 48308230,
                    "node_id": "MDQ6VXNlcjQ4MzA4MjMw",
                    "avatar_url": "https://avatars.githubusercontent.com/u/48308230?v=4",
                    "gravatar_id": "",
                    "url": "https://api.github.com/users/amindadgar",
                    "html_url": "https://github.com/amindadgar",
                    "followers_url": "https://api.github.com/users/amindadgar/followers",
                    "following_url": "https://api.github.com/users/amindadgar/following{/other_user}",
                    "gists_url": "https://api.github.com/users/amindadgar/gists{/gist_id}",
                    "starred_url": "https://api.github.com/users/amindadgar/starred{/owner}{/repo}",
                    "subscriptions_url": "https://api.github.com/users/amindadgar/subscriptions",
                    "organizations_url": "https://api.github.com/users/amindadgar/orgs",
                    "repos_url": "https://api.github.com/users/amindadgar/repos",
                    "events_url": "https://api.github.com/users/amindadgar/events{/privacy}",
                    "received_events_url": "https://api.github.com/users/amindadgar/received_events",
                    "type": "User",
                    "site_admin": False,
                },
                "body": None,
                "created_at": "2024-03-28T05:54:26Z",
                "updated_at": "2024-03-28T09:04:20Z",
                "closed_at": "2024-03-28T09:04:20Z",
                "merged_at": "2024-03-28T09:04:20Z",
                "merge_commit_sha": "150c46e8ff181fb57378b9b405cb14587cabb2cd",
                "assignee": None,
                "assignees": [],
                "requested_reviewers": [],
                "requested_teams": [],
                "labels": [],
                "milestone": None,
                "draft": False,
                "commits_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/pulls/92/commits",
                "review_comments_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/pulls/92/comments",
                "review_comment_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/pulls/comments{/number}",
                "comments_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/issues/92/comments",
                "statuses_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/statuses/6d5f1fcdd450fbe6c315bc04a50ef9867f94d688",
                "head": {
                    "label": "TogetherCrew:feat/hivemind-github-etl-transformation-class",
                    "ref": "feat/hivemind-github-etl-transformation-class",
                    "sha": "6d5f1fcdd450fbe6c315bc04a50ef9867f94d688",
                    "user": {
                        "login": "TogetherCrew",
                        "id": 133082471,
                        "node_id": "O_kgDOB-6tZw",
                        "avatar_url": "https://avatars.githubusercontent.com/u/133082471?v=4",
                        "gravatar_id": "",
                        "url": "https://api.github.com/users/TogetherCrew",
                        "html_url": "https://github.com/TogetherCrew",
                        "followers_url": "https://api.github.com/users/TogetherCrew/followers",
                        "following_url": "https://api.github.com/users/TogetherCrew/following{/other_user}",
                        "gists_url": "https://api.github.com/users/TogetherCrew/gists{/gist_id}",
                        "starred_url": "https://api.github.com/users/TogetherCrew/starred{/owner}{/repo}",
                        "subscriptions_url": "https://api.github.com/users/TogetherCrew/subscriptions",
                        "organizations_url": "https://api.github.com/users/TogetherCrew/orgs",
                        "repos_url": "https://api.github.com/users/TogetherCrew/repos",
                        "events_url": "https://api.github.com/users/TogetherCrew/events{/privacy}",
                        "received_events_url": "https://api.github.com/users/TogetherCrew/received_events",
                        "type": "Organization",
                        "site_admin": False,
                    },
                    "repo": {
                        "id": 715562696,
                        "node_id": "R_kgDOKqaeyA",
                        "name": "airflow-dags",
                        "full_name": "TogetherCrew/airflow-dags",
                        "private": False,
                        "owner": {
                            "login": "TogetherCrew",
                            "id": 133082471,
                            "node_id": "O_kgDOB-6tZw",
                            "avatar_url": "https://avatars.githubusercontent.com/u/133082471?v=4",
                            "gravatar_id": "",
                            "url": "https://api.github.com/users/TogetherCrew",
                            "html_url": "https://github.com/TogetherCrew",
                            "followers_url": "https://api.github.com/users/TogetherCrew/followers",
                            "following_url": "https://api.github.com/users/TogetherCrew/following{/other_user}",
                            "gists_url": "https://api.github.com/users/TogetherCrew/gists{/gist_id}",
                            "starred_url": "https://api.github.com/users/TogetherCrew/starred{/owner}{/repo}",
                            "subscriptions_url": "https://api.github.com/users/TogetherCrew/subscriptions",
                            "organizations_url": "https://api.github.com/users/TogetherCrew/orgs",
                            "repos_url": "https://api.github.com/users/TogetherCrew/repos",
                            "events_url": "https://api.github.com/users/TogetherCrew/events{/privacy}",
                            "received_events_url": "https://api.github.com/users/TogetherCrew/received_events",
                            "type": "Organization",
                            "site_admin": False,
                        },
                        "html_url": "https://github.com/TogetherCrew/airflow-dags",
                        "description": None,
                        "fork": False,
                        "url": "https://api.github.com/repos/TogetherCrew/airflow-dags",
                        "forks_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/forks",
                        "keys_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/keys{/key_id}",
                        "collaborators_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/collaborators{/collaborator}",
                        "teams_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/teams",
                        "hooks_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/hooks",
                        "issue_events_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/issues/events{/number}",
                        "events_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/events",
                        "assignees_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/assignees{/user}",
                        "branches_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/branches{/branch}",
                        "tags_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/tags",
                        "blobs_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/git/blobs{/sha}",
                        "git_tags_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/git/tags{/sha}",
                        "git_refs_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/git/refs{/sha}",
                        "trees_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/git/trees{/sha}",
                        "statuses_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/statuses/{sha}",
                        "languages_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/languages",
                        "stargazers_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/stargazers",
                        "contributors_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/contributors",
                        "subscribers_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/subscribers",
                        "subscription_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/subscription",
                        "commits_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/commits{/sha}",
                        "git_commits_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/git/commits{/sha}",
                        "comments_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/comments{/number}",
                        "issue_comment_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/issues/comments{/number}",
                        "contents_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/contents/{+path}",
                        "compare_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/compare/{base}...{head}",
                        "merges_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/merges",
                        "archive_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/{archive_format}{/ref}",
                        "downloads_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/downloads",
                        "issues_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/issues{/number}",
                        "pulls_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/pulls{/number}",
                        "milestones_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/milestones{/number}",
                        "notifications_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/notifications{?since,all,participating}",
                        "labels_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/labels{/name}",
                        "releases_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/releases{/id}",
                        "deployments_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/deployments",
                        "created_at": "2023-11-07T11:52:41Z",
                        "updated_at": "2024-03-07T17:41:13Z",
                        "pushed_at": "2024-04-11T10:44:27Z",
                        "git_url": "git://github.com/TogetherCrew/airflow-dags.git",
                        "ssh_url": "git@github.com:TogetherCrew/airflow-dags.git",
                        "clone_url": "https://github.com/TogetherCrew/airflow-dags.git",
                        "svn_url": "https://github.com/TogetherCrew/airflow-dags",
                        "homepage": None,
                        "size": 719,
                        "stargazers_count": 1,
                        "watchers_count": 1,
                        "language": "Python",
                        "has_issues": True,
                        "has_projects": True,
                        "has_downloads": True,
                        "has_wiki": True,
                        "has_pages": False,
                        "has_discussions": False,
                        "forks_count": 0,
                        "mirror_url": None,
                        "archived": False,
                        "disabled": False,
                        "open_issues_count": 41,
                        "license": None,
                        "allow_forking": True,
                        "is_template": False,
                        "web_commit_signoff_required": False,
                        "topics": [],
                        "visibility": "public",
                        "forks": 0,
                        "open_issues": 41,
                        "watchers": 1,
                        "default_branch": "main",
                    },
                },
                "base": {
                    "label": "TogetherCrew:main",
                    "ref": "main",
                    "sha": "7a9c9d9ef933c57bbeb1ac6d7e4c4d4d733a5b9b",
                    "user": {
                        "login": "TogetherCrew",
                        "id": 133082471,
                        "node_id": "O_kgDOB-6tZw",
                        "avatar_url": "https://avatars.githubusercontent.com/u/133082471?v=4",
                        "gravatar_id": "",
                        "url": "https://api.github.com/users/TogetherCrew",
                        "html_url": "https://github.com/TogetherCrew",
                        "followers_url": "https://api.github.com/users/TogetherCrew/followers",
                        "following_url": "https://api.github.com/users/TogetherCrew/following{/other_user}",
                        "gists_url": "https://api.github.com/users/TogetherCrew/gists{/gist_id}",
                        "starred_url": "https://api.github.com/users/TogetherCrew/starred{/owner}{/repo}",
                        "subscriptions_url": "https://api.github.com/users/TogetherCrew/subscriptions",
                        "organizations_url": "https://api.github.com/users/TogetherCrew/orgs",
                        "repos_url": "https://api.github.com/users/TogetherCrew/repos",
                        "events_url": "https://api.github.com/users/TogetherCrew/events{/privacy}",
                        "received_events_url": "https://api.github.com/users/TogetherCrew/received_events",
                        "type": "Organization",
                        "site_admin": False,
                    },
                    "repo": {
                        "id": 715562696,
                        "node_id": "R_kgDOKqaeyA",
                        "name": "airflow-dags",
                        "full_name": "TogetherCrew/airflow-dags",
                        "private": False,
                        "owner": {
                            "login": "TogetherCrew",
                            "id": 133082471,
                            "node_id": "O_kgDOB-6tZw",
                            "avatar_url": "https://avatars.githubusercontent.com/u/133082471?v=4",
                            "gravatar_id": "",
                            "url": "https://api.github.com/users/TogetherCrew",
                            "html_url": "https://github.com/TogetherCrew",
                            "followers_url": "https://api.github.com/users/TogetherCrew/followers",
                            "following_url": "https://api.github.com/users/TogetherCrew/following{/other_user}",
                            "gists_url": "https://api.github.com/users/TogetherCrew/gists{/gist_id}",
                            "starred_url": "https://api.github.com/users/TogetherCrew/starred{/owner}{/repo}",
                            "subscriptions_url": "https://api.github.com/users/TogetherCrew/subscriptions",
                            "organizations_url": "https://api.github.com/users/TogetherCrew/orgs",
                            "repos_url": "https://api.github.com/users/TogetherCrew/repos",
                            "events_url": "https://api.github.com/users/TogetherCrew/events{/privacy}",
                            "received_events_url": "https://api.github.com/users/TogetherCrew/received_events",
                            "type": "Organization",
                            "site_admin": False,
                        },
                        "html_url": "https://github.com/TogetherCrew/airflow-dags",
                        "description": None,
                        "fork": False,
                        "url": "https://api.github.com/repos/TogetherCrew/airflow-dags",
                        "forks_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/forks",
                        "keys_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/keys{/key_id}",
                        "collaborators_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/collaborators{/collaborator}",
                        "teams_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/teams",
                        "hooks_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/hooks",
                        "issue_events_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/issues/events{/number}",
                        "events_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/events",
                        "assignees_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/assignees{/user}",
                        "branches_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/branches{/branch}",
                        "tags_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/tags",
                        "blobs_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/git/blobs{/sha}",
                        "git_tags_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/git/tags{/sha}",
                        "git_refs_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/git/refs{/sha}",
                        "trees_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/git/trees{/sha}",
                        "statuses_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/statuses/{sha}",
                        "languages_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/languages",
                        "stargazers_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/stargazers",
                        "contributors_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/contributors",
                        "subscribers_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/subscribers",
                        "subscription_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/subscription",
                        "commits_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/commits{/sha}",
                        "git_commits_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/git/commits{/sha}",
                        "comments_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/comments{/number}",
                        "issue_comment_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/issues/comments{/number}",
                        "contents_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/contents/{+path}",
                        "compare_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/compare/{base}...{head}",
                        "merges_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/merges",
                        "archive_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/{archive_format}{/ref}",
                        "downloads_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/downloads",
                        "issues_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/issues{/number}",
                        "pulls_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/pulls{/number}",
                        "milestones_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/milestones{/number}",
                        "notifications_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/notifications{?since,all,participating}",
                        "labels_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/labels{/name}",
                        "releases_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/releases{/id}",
                        "deployments_url": "https://api.github.com/repos/TogetherCrew/airflow-dags/deployments",
                        "created_at": "2023-11-07T11:52:41Z",
                        "updated_at": "2024-03-07T17:41:13Z",
                        "pushed_at": "2024-04-11T10:44:27Z",
                        "git_url": "git://github.com/TogetherCrew/airflow-dags.git",
                        "ssh_url": "git@github.com:TogetherCrew/airflow-dags.git",
                        "clone_url": "https://github.com/TogetherCrew/airflow-dags.git",
                        "svn_url": "https://github.com/TogetherCrew/airflow-dags",
                        "homepage": None,
                        "size": 719,
                        "stargazers_count": 1,
                        "watchers_count": 1,
                        "language": "Python",
                        "has_issues": True,
                        "has_projects": True,
                        "has_downloads": True,
                        "has_wiki": True,
                        "has_pages": False,
                        "has_discussions": False,
                        "forks_count": 0,
                        "mirror_url": None,
                        "archived": False,
                        "disabled": False,
                        "open_issues_count": 41,
                        "license": None,
                        "allow_forking": True,
                        "is_template": False,
                        "web_commit_signoff_required": False,
                        "topics": [],
                        "visibility": "public",
                        "forks": 0,
                        "open_issues": 41,
                        "watchers": 1,
                        "default_branch": "main",
                    },
                },
                "_links": {
                    "self": {
                        "href": "https://api.github.com/repos/TogetherCrew/airflow-dags/pulls/92"
                    },
                    "html": {
                        "href": "https://github.com/TogetherCrew/airflow-dags/pull/92"
                    },
                    "issue": {
                        "href": "https://api.github.com/repos/TogetherCrew/airflow-dags/issues/92"
                    },
                    "comments": {
                        "href": "https://api.github.com/repos/TogetherCrew/airflow-dags/issues/92/comments"
                    },
                    "review_comments": {
                        "href": "https://api.github.com/repos/TogetherCrew/airflow-dags/pulls/92/comments"
                    },
                    "review_comment": {
                        "href": "https://api.github.com/repos/TogetherCrew/airflow-dags/pulls/comments{/number}"
                    },
                    "commits": {
                        "href": "https://api.github.com/repos/TogetherCrew/airflow-dags/pulls/92/commits"
                    },
                    "statuses": {
                        "href": "https://api.github.com/repos/TogetherCrew/airflow-dags/statuses/6d5f1fcdd450fbe6c315bc04a50ef9867f94d688"
                    },
                },
                "author_association": "MEMBER",
                "auto_merge": None,
                "active_lock_reason": None,
            }
        ]

        save_commits_relation_to_pr(
            commit_sha="random_sha", repository_id="1234", pull_requests=pr
        )

        # Checking the user
        data_user = self.neo4j_driver.execute_query("MATCH (gu:GitHubUser) RETURN (gu)")
        records_user = data_user.records

        # we had one user
        self.assertEqual(len(records_user), 1)
        user = records_user[0]["gu"]
        dt = user["latestSavedAt"]
        self.assertTrue(
            datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute) - datetime.now()
            < timedelta(minutes=5)
        )
        self.assertEqual(user["url"], "https://api.github.com/users/amindadgar")
        self.assertEqual(
            user["repos_url"], "https://api.github.com/users/amindadgar/repos"
        )
        self.assertEqual(
            user["following_url"],
            "https://api.github.com/users/amindadgar/following{/other_user}",
        )
        self.assertEqual(
            user["followers_url"], "https://api.github.com/users/amindadgar/followers"
        )
        self.assertEqual(
            user["starred_url"],
            "https://api.github.com/users/amindadgar/starred{/owner}{/repo}",
        )
        self.assertEqual(user["login"], "amindadgar")
        self.assertEqual(user["type"], "User")
        self.assertEqual(
            user["subscriptions_url"],
            "https://api.github.com/users/amindadgar/subscriptions",
        )
        self.assertEqual(
            user["received_events_url"],
            "https://api.github.com/users/amindadgar/received_events",
        )
        self.assertEqual(
            user["avatar_url"], "https://avatars.githubusercontent.com/u/48308230?v=4"
        )
        self.assertEqual(
            user["events_url"],
            "https://api.github.com/users/amindadgar/events{/privacy}",
        )
        self.assertEqual(user["html_url"], "https://github.com/amindadgar")
        self.assertEqual(user["site_admin"], False)
        self.assertEqual(user["id"], 48308230)
        self.assertEqual(user["node_id"], "MDQ6VXNlcjQ4MzA4MjMw")
        self.assertEqual(
            user["organizations_url"], "https://api.github.com/users/amindadgar/orgs"
        )
        self.assertEqual(
            user["gists_url"], "https://api.github.com/users/amindadgar/gists{/gist_id}"
        )

        # Checking the user
        data_pr = self.neo4j_driver.execute_query(
            "MATCH (pr:GitHubPullRequest) RETURN (pr)"
        )
        records_pr = data_pr.records
        self.assertEqual(len(records_pr), 1)

        pr = records_pr[0]["pr"]

        self.assertEqual(
            pr["url"], "https://api.github.com/repos/TogetherCrew/airflow-dags/pulls/92"
        )
        self.assertEqual(pr["id"], 1795340483)
        self.assertEqual(pr["node_id"], "PR_kwDOKqaeyM5rArjD")
        self.assertEqual(
            pr["html_url"], "https://github.com/TogetherCrew/airflow-dags/pull/92"
        )
        self.assertEqual(
            pr["diff_url"], "https://github.com/TogetherCrew/airflow-dags/pull/92.diff"
        )
        self.assertEqual(
            pr["patch_url"],
            "https://github.com/TogetherCrew/airflow-dags/pull/92.patch",
        )
        self.assertEqual(
            pr["issue_url"],
            "https://api.github.com/repos/TogetherCrew/airflow-dags/issues/92",
        )
        self.assertEqual(pr["number"], 92)
        self.assertEqual(pr["state"], "closed")
        self.assertEqual(pr["locked"], False)
        self.assertEqual(pr["title"], "feat: Adding the GitHubTransformation class!")
        self.assertEqual(pr["created_at"], "2024-03-28T05:54:26Z")
        self.assertEqual(pr["updated_at"], "2024-03-28T09:04:20Z")
        self.assertEqual(pr["closed_at"], "2024-03-28T09:04:20Z")
        self.assertEqual(pr["merged_at"], "2024-03-28T09:04:20Z")

        self.assertEqual(
            pr["merge_commit_sha"], "150c46e8ff181fb57378b9b405cb14587cabb2cd"
        )
        self.assertEqual(pr["assignee"], None)
        self.assertEqual(pr["milestone"], None)
        self.assertEqual(pr["draft"], False)
        self.assertEqual(pr["milestone"], None)

        # Checking the user
        data_commits = self.neo4j_driver.execute_query(
            "MATCH (co:GitHubCommit) RETURN (co)"
        )
        records_commit = data_commits.records

        self.assertEqual(len(records_commit), 1)
        self.assertEqual(records_commit[0]["co"]["sha"], "random_sha")

        data_user_pr_relation = self.neo4j_driver.execute_query(
            "MATCH (gu:GitHubUser)-[r:CREATED]->(pr:GitHubPullRequest) RETURN gu, r, pr"
        )
        records_user_pr_relation = data_user_pr_relation.records
        self.assertEqual(len(records_user_pr_relation), 1)
        user_pr_relation = records_user_pr_relation[0]
        self.assertEqual(user_pr_relation["gu"]["login"], "amindadgar")
        self.assertEqual(user_pr_relation["pr"]["id"], 1795340483)
        dt = user_pr_relation["r"]["latestSavedAt"]
        print(dt)
        self.assertTrue(
            datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute) - datetime.now()
            < timedelta(minutes=5)
        )

        data_commit_pr_relation = self.neo4j_driver.execute_query(
            "MATCH (c:GitHubCommit)-[r:IS_ON]->(pr:GitHubPullRequest) RETURN c, r, pr"
        )
        commit_pr_relation = data_commit_pr_relation.records
        self.assertEqual(len(commit_pr_relation), 1)
