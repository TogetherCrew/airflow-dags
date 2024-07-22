import logging

from dags.analyzer_helper.common.base.transform_raw_members_base import TransformRawMembersBase


class TransformRawMembers(TransformRawMembersBase):
    def transform(self, raw_members: list) -> list:
        """
        Transform extracted raw members data into the guildmember structure.
        """
        transformed_members = []

        for member in raw_members:
            try:
                transformed_member = self.transform_member(member=member)
                transformed_members.append(transformed_member)
            except Exception as e:
                logging.error(f"Error transforming raw discourse member {member}: {e}")

        return transformed_members

    def transform_member(self, member: dict) -> dict:
        """
        Transform a single member's data to the guildmember structure.
        """
        discourse_id = member.get("id")
        guild_member = {
            "id": str(discourse_id) if discourse_id is not None else None,
            "is_bot": member.get("isBot", False),
            "left_at": member.get("deletedAt"),
            "joined_at": member.get("createdAt"),
            "options": {
            },
        }

        return guild_member
