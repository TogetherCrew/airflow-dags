from .neo4j_connection import Neo4jConnection
from .neo4j_enums import Node, Relationship
from .utils import flat_map


def save_review_comment_to_neo4j(review_comment: dict, repository_id: str):
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    user = review_comment.pop("user", None)
    pull_request_number = review_comment.pop("pull_request_number", None)
    reactions_member = review_comment.pop("reactions_member", None)
    cleaned_review = flat_map(review_comment)

    if pull_request_number:
        pull_request_query = f"""
            WITH rc
            MATCH (pr:{Node.PullRequest.value} {{number: $pull_request_number, repository_id: $repository_id}})
            MERGE (rc)-[ra:{Relationship.IS_ON.value}]->(pr)
                SET ra.latestSavedAt = datetime()
        """
    else:
        pull_request_query = ""

    if user:
        user_query = f"""
            WITH rc
            MERGE (ghu:{Node.GitHubUser.value} {{id: $user.id}})
                SET ghu += $user, ghu.latestSavedAt = datetime()
            WITH rc, ghu
            MERGE (ghu)-[ra:{Relationship.CREATED.value}]->(rc)
                SET ra.latestSavedAt = datetime()
        """
    else:
        user_query = ""

    if reactions_member:
        reactions_member_query = f"""
            WITH c
            UNWIND $reactions_member as reaction_member
            MERGE (ghu:{Node.GitHubUser.value} {{id: reaction_member.id}})
                SET ghu += reaction_member, ghu.latestSavedAt = datetime()
            WITH c, ghu
            MERGE (c)-[ra:{Relationship.REACTED.value}]->(ghu)
                SET ra.latestSavedAt = datetime()
        """
    else:
        reactions_member_query = ""

    with driver.session() as session:
        session.execute_write(
            lambda tx: tx.run(
                f"""
                MERGE (rc:{Node.ReviewComment.value} {{id: $review.id}})
                SET rc += $review, rc.repository_id = $repository_id, rc.latestSavedAt = datetime()

                { user_query }
                { pull_request_query }
                { reactions_member_query }
            """,
                review=cleaned_review,
                repository_id=repository_id,
                user=user,
                pull_request_number=pull_request_number,
                reactions_member=reactions_member,
            )
        )

    driver.close()


def save_comment_to_neo4j(comment: dict, repository_id: str):
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    user = comment.pop("user", None)
    type = comment.pop("type", None)
    number = comment.pop("number", None)
    reactions_member = comment.pop("reactions_member", None)
    cleaned_comment = flat_map(comment)

    if type == "issue":
        issue_query = f"""
            WITH c
            MATCH (i:{Node.Issue.value} {{number: $number, repository_id: $repository_id}})
            WITH c, i
            MERGE (c)-[ra:{Relationship.IS_ON.value}]->(i)
                SET ra.latestSavedAt = datetime()
        """
    if type == "pull_request":
        issue_query = f"""
            WITH c
            MATCH (pr:{Node.PullRequest.value} {{number: $number, repository_id: $repository_id}})
            WITH c, pr
            MERGE (c)-[ra:{Relationship.IS_ON.value}]->(pr)
                SET ra.latestSavedAt = datetime()
        """

    if user:
        user_query = f"""
            WITH c
            MERGE (ghu:{Node.GitHubUser.value} {{id: $user.id}})
                SET ghu += $user, ghu.latestSavedAt = datetime()
            WITH c, ghu
            MERGE (ghu)-[ra:{Relationship.CREATED.value}]->(c)
                SET ra.latestSavedAt = datetime()
        """
    else:
        user_query = ""

    if reactions_member:
        reactions_member_query = f"""
            WITH c
            UNWIND $reactions_member as reaction_member
            MERGE (ghu:{Node.GitHubUser.value} {{id: reaction_member.user.id}})
                SET ghu += reaction_member.user, ghu.latestSavedAt = datetime()
            WITH c, ghu
            MERGE (c)-[ra:{Relationship.REACTED.value}]->(ghu)
                SET ra.latestSavedAt = datetime()
        """
    else:
        reactions_member_query = ""

    with driver.session() as session:
        session.execute_write(
            lambda tx: tx.run(
                f"""
                MERGE (c:{Node.Comment.value} {{id: $comment.id}})
                SET c += $comment, c.repository_id = $repository_id, c.latestSavedAt = datetime()

                { user_query }
                { issue_query }
                { reactions_member_query }
            """,
                comment=cleaned_comment,
                repository_id=int(repository_id),
                user=user,
                number=int(number),
                reactions_member=reactions_member,
            )
        )

    driver.close()
