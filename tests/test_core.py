
from unittest import TestCase

import mongozen


def _mz_test_collection():
    return mongozen.get_collection(
        collection_name='users_test',
        db_name='mongozen',
        server_name='mongozen_test',
        env_name='mongozen_test',
        mode='reading'
    )

class Testmongozen(TestCase):
    """Checks mongozen's core functionallity."""

    def test_get_collection(self):
        """Tests Matchop and operator."""
        users_test = _mz_test_collection()
        random_user = users_test.find_one()
        self.assertIn('user_id', random_user)
        # self.assertEqual(random_user.user_id, 'user_id')
