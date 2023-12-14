import unittest

from hivemind_etl_helpers.src.db.discord.utils.prepare_reactions_id import (
    prepare_raction_ids,
)


class TestGetReactionIds(unittest.TestCase):
    def test_get_reaction_ids_one_emoji(self):
        reactions = ["userId1,userId5,:thumbsup:"]
        user_ids = prepare_raction_ids(reactions)

        expected_user_ids = ["userId1", "userId5"]

        self.assertEqual(set(user_ids), set(expected_user_ids))

    def test_get_reaction_ids_empty_input(self):
        reactions = []
        user_ids = prepare_raction_ids(reactions)

        self.assertEqual(user_ids, [])

    def test_get_reaction_ids_no_commas(self):
        reactions = ["userId1,:thumbsup:", "userId2,:heart:"]
        user_ids = prepare_raction_ids(reactions)

        expected_user_ids = ["userId1", "userId2"]

        self.assertEqual(set(user_ids), set(expected_user_ids))

    def test_get_reaction_ids_multiple_emojis(self):
        reactions = ["userId1,userId2,❤️", "userId2,userId4,👌"]
        user_ids = prepare_raction_ids(reactions)

        expected_user_ids = ["userId1", "userId2", "userId4"]

        self.assertEqual(set(user_ids), set(expected_user_ids))
