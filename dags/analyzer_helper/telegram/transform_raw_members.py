import logging

from analyzer_helper.common.base.transform_raw_members_base import (
    TransformRawMembersBase,
)
from analyzer_helper.telegram.utils.date_time_format_converter import (
    DateTimeFormatConverter,
)


class TransformRawMembers(TransformRawMembersBase):

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
        telegram_id = member.get("id")
        joined_at = DateTimeFormatConverter.timestamp_to_datetime(member.get("joined_at"))
        member = {
            "id": str(telegram_id) if telegram_id is not None else None,
            "is_bot": member.get("isBot", False),
            "left_at": member.get("left_at", None),
            "joined_at": joined_at if joined_at is not None else None,
            "options": {},
        }

        return member
    
data = [
            {'id': 203678862.0, 'is_bot': False, 'joined_at': 1713038774.0, 'left_at': None},
            {'id': 265278326.0, 'is_bot': False, 'joined_at': 1713161415.0, 'left_at': None},
            {'id': 501641383.0, 'is_bot': False, 'joined_at': 1713038805.0, 'left_at': None},
            {'id': 551595722.0, 'is_bot': False, 'joined_at': 1713047141.0, 'left_at': None},
            {'id': 926245054.0, 'is_bot': False, 'joined_at': 1713178099.0, 'left_at': None},
            {'id': 927814807.0, 'is_bot': False, 'joined_at': 0.0, 'left_at': None},
            {'id': 6504405389.0, 'is_bot': False, 'joined_at': 0.0, 'left_at': None}
        ]

# data = [{'id': 927814807.0, 'is_bot': False, 'joined_at': 0.0, 'left_at': 1713036912.0}]
transform = TransformRawMembers()
result = transform.transform(data)
print("result: \n", result)