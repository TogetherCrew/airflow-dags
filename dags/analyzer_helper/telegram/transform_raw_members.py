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
        joined_at_timestamp = member.get("joined_at")
        left_at_timestamp = member.get("left_at")

        if joined_at_timestamp:
            joined_at = DateTimeFormatConverter.timestamp_to_datetime(
                joined_at_timestamp
            )
        else:
            joined_at = None

        if left_at_timestamp:
            left_at = DateTimeFormatConverter.timestamp_to_datetime(left_at_timestamp)
        else:
            left_at = None

        member = {
            "id": str(telegram_id) if telegram_id is not None else None,
            "is_bot": member.get("isBot", False),
            "left_at": (
                None if left_at is None or joined_at_timestamp == 0.0 else left_at
            ),
            "joined_at": (
                None if joined_at is None or joined_at_timestamp == 0.0 else joined_at
            ),
            "options": {},
        }

        return member
