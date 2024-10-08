import logging

from analyzer_helper.common.base.transform_raw_members_base import (
    TransformRawMembersBase,
)
from analyzer_helper.discourse.utils.convert_date_time_formats import (
    DateTimeFormatConverter,
)


class TransformRawMembers(TransformRawMembersBase):
    def __init__(self, endpoint: str):
        self.converter = DateTimeFormatConverter()
        self.endpoint = endpoint

    def transform(self, raw_members: list) -> list:
        """
        Transform extracted raw members data into the rawmember structure.
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
        Transform a single member's data to the rawmember structure.
        """
        discourse_id = member.get("id")
        avatar_template: str = member["avatar"]

        avatar: str
        if avatar_template.startswith(("http://", "https://")):
            avatar = avatar_template
        elif avatar_template is not None:
            avatar = "https://" + self.endpoint + avatar_template
        else:
            avatar = None

        avatar = avatar.replace("{size}", "128") if avatar else None

        member = {
            "id": str(int(discourse_id)) if discourse_id is not None else None,
            "is_bot": member.get("isBot", False),
            "left_at": None,
            "joined_at": self.converter.from_date_string(member.get("joined_at")),
            "options": {
                "name": member["name"],
                "username": member["username"],
                "avatar": avatar,
            },
        }

        return member
