import logging

from analyzer_helper.discord.transform_raw_members_base import TransformRawMembersBase


class DiscordTransformRawMembers(TransformRawMembersBase):
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
                logging.error(f"Error transforming raw discord member {member}: {e}")

        return transformed_members

    def transform_member(self, member: dict) -> dict:
        """
        Transform a single member's data to the guildmember structure.
        """
        discord_id = member.get("discordId")
        guild_member = {
            "id": discord_id if discord_id is not None else None,
            # "id": discord_id,
            "is_bot": member.get("isBot", False),
            "left_at": member.get("deletedAt"),
            "joined_at": member.get("joinedAt"),
            "options": {
                # "username": member.get("username"),
                # "avatar": member.get("avatar"),
                # "roles": member.get("roles"),
                # "discriminator": member.get("discriminator"),
                # "permissions": member.get("permissions"),
                # "global_name": member.get("globalName"),
                # "nickname": member.get("nickname"),
            },
        }

        return guild_member
