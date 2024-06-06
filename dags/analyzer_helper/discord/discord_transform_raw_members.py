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
                transformed_member = self.transform_member(member)
                transformed_members.append(transformed_member)
            except Exception as e:
                logging.error(f"Error transforming raw discord member {member}: {e}")

        return transformed_members

    def transform_member(self, member: dict) -> dict:
        """
        Transform a single member's data to the guildmember structure.
        """

        guild_member = {
            "id": int(member.discordId),
            "is_bot": member.isBot if member.isBot is not None else False,
            "left_at": member.deletedAt,
            "joined_at": member.joinedAt,
            "options": {
                # "username": member.username,
                # "avatar": member.avatar,
                # "roles": member.roles,
                # "discriminator": member.discriminator,
                # "permissions": member.permissions,
                # "global_name": member.globalName,
                # "nickname": member.nickname,
                }
        }

        return guild_member
