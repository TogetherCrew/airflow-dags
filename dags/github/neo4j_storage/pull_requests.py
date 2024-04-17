from .neo4j_connection import Neo4jConnection
from .neo4j_enums import Node, Relationship
from .utils import remove_nested_collections


def save_pull_request_to_neo4j(pr: dict, repository_id: str):
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    repo_creator = pr.pop("user", None)
    assignee = pr.pop("assignee", None)
    assignees = pr.pop("assignees", None)
    requested_reviewers = pr.pop("requested_reviewers", None)
    labels = pr.pop("labels", None)
    linked_issues = pr.pop("linked_issues", None)
    cleaned_pr = remove_nested_collections(pr)

    if assignee:
        assignee_query = f"""
            WITH pr
            MERGE (ghu:{Node.GitHubUser.value} {{id: $assignee.id}})
                SET ghu += $assignee, ghu.latestSavedAt = datetime()
            WITH pr, ghu
            MERGE (pr)-[assignghu:{Relationship.ASSIGNED.value}]->(ghu)
                SET assignghu.latestSavedAt = datetime()
        """
    else:
        assignee_query = ""

    assignees_query = f"""
        WITH pr
        UNWIND $assignees as one_assignee
        MERGE (ghuoa:{Node.GitHubUser.value} {{id: one_assignee.id}})
            SET ghuoa += one_assignee, ghuoa.latestSavedAt = datetime()
        WITH pr, ghuoa
        MERGE (pr)-[assignghuoa:{Relationship.ASSIGNED.value}]->(ghuoa)
            SET assignghuoa.latestSavedAt = datetime()
    """

    requested_reviewers_query = f"""
        WITH pr
        UNWIND $requested_reviewers as requested_reviewer
        MERGE (ghurr:{Node.GitHubUser.value} {{id: requested_reviewer.id}})
            SET ghurr += requested_reviewer, ghurr.latestSavedAt = datetime()
        WITH pr, ghurr
        MERGE (pr)-[isreviewerghu:{Relationship.IS_REVIEWER.value}]->(ghurr)
            SET isreviewerghu.latestSavedAt = datetime()
    """

    labels_query = f"""
        WITH pr
        UNWIND $labels as label
        MERGE (lb:{Node.Label.value} {{id: label.id}})
            SET lb += label, lb.latestSavedAt = datetime()
        WITH pr, lb
        MERGE (pr)-[haslb:{Relationship.HAS_LABEL.value}]->(lb)
            SET haslb.latestSavedAt = datetime()
    """

    linked_issues_query = f"""
        WITH pr
        UNWIND $linked_issues as linked_issue
        MERGE (i:{Node.Issue.value} {{id: linked_issue.id}})
            SET i += linked_issue, i.latestSavedAt = datetime()
        WITH pr, i
        MERGE (pr)-[islinked:{Relationship.LINKED.value}]->(i)
            SET islinked.latestSavedAt = datetime()
    """

    with driver.session() as session:
        session.execute_write(
            lambda tx: tx.run(
                f"""
                MERGE (pr:{Node.PullRequest.value} {{id: $pr.id}})
                SET pr += $pr, pr.repository_id = $repository_id, pr.latestSavedAt = datetime()

                WITH pr
                MERGE (ghu:{Node.GitHubUser.value} {{id: $repo_creator.id}})
                    SET ghu += $repo_creator, ghu.latestSavedAt = datetime()
                WITH pr, ghu
                MERGE (ghu)-[pc:{Relationship.CREATED.value}]->(pr)
                    SET pc.latestSavedAt = datetime()

                { assignee_query }
                { assignees_query }
                { requested_reviewers_query }
                { labels_query  }
                { linked_issues_query }

            """,
                pr=cleaned_pr,
                repository_id=repository_id,
                repo_creator=repo_creator,
                assignee=assignee,
                assignees=assignees,
                labels=labels,
                requested_reviewers=requested_reviewers,
                linked_issues=linked_issues,
            )
        )
    driver.close()


def save_review_to_neo4j(pr_id: int, review: dict):
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    author = review.pop("user", None)

    with driver.session() as session:
        session.execute_write(
            lambda tx: tx.run(
                f"""
                MATCH (pr:{Node.PullRequest.value} {{id: $pr_id}})
                WITH pr
                MERGE (ghu:{Node.GitHubUser.value} {{id: $author.id}})
                    SET ghu += $author, ghu.latestSavedAt = datetime()
                WITH pr, ghu
                MERGE (ghu)-[reviewed:{Relationship.REVIEWED.value}]->(pr)
                    SET reviewed.latestSavedAt = datetime(), reviewed.state = $review.state
            """,
                pr_id=int(pr_id),
                author=author,
                review=review,
            )
        )

    driver.close()


def save_pr_files_changes_to_neo4j(pr_id: int, repository_id: str, file_changes: list):
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    # Not saving file changes without a sha
    file_changes = list(filter(lambda fc: fc.get("sha") is not None, file_changes))

    with driver.session() as session:
        session.execute_write(
            lambda tx: tx.run(
                f"""
                MATCH (repo:{Node.Repository.value} {{id: $repository_id}}),
                (pr:{Node.PullRequest.value} {{id: $pr_id}})
                WITH repo, pr
                UNWIND $file_changes AS file_change
                MERGE (f:{Node.File.value} {{sha: file_change.sha, filename: file_change.filename}})
                    SET f += file_change, f.latestSavedAt = datetime()
                MERGE (pr)-[fc:{Relationship.CHANGED.value}]->(f)
                    SET fc.latestSavedAt = datetime()
                MERGE (f)-[io:{Relationship.IS_ON.value}]->(repo)
                    SET io.latestSavedAt = datetime()
            """,
                pr_id=int(pr_id),
                repository_id=int(repository_id),
                file_changes=file_changes,
            )
        )

    driver.close()


def save_commits_relation_to_pr(
    commit_sha: str, repository_id: str, pull_requests: list
):
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    for pr in pull_requests:
        save_pull_request_to_neo4j(pr, repository_id)

    with driver.session() as session:
        session.execute_write(
            lambda tx: tx.run(
                f"""
                UNWIND $pull_requests as pr_data
                MERGE (pr:{Node.PullRequest.value} {{id: pr_data.id}})
                WITH pr
                MERGE (commit:{Node.Commit.value} {{sha: $commit_sha}})
                WITH pr, commit
                MERGE (commit)-[r:IS_ON]->(pr)
                SET r.latestSavedAt = datetime()
            """,
                pull_requests=pull_requests,
                commit_sha=commit_sha,
            )
        )

    driver.close()
