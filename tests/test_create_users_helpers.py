import unittest

from create_users_helpers import get_target_instances, init_default_users_state, resolve_destination


class TestCreateUsersHelpers(unittest.TestCase):
    def setUp(self):
        self.authenticated = [
            {"name": "instance A", "base_url": "https://a"},
            {"name": "instance B", "base_url": "https://b"},
        ]

    def test_get_target_instances_all(self):
        result = get_target_instances("Todas", self.authenticated)
        self.assertEqual(result, self.authenticated)

    def test_get_target_instances_specific(self):
        result = get_target_instances("instance A", self.authenticated)
        self.assertEqual(result, [{"name": "instance A", "base_url": "https://a"}])

    def test_default_users_selected_true_on_init(self):
        df, selection = init_default_users_state([
            {"username": "u1", "password": "p", "roles": "superuser"},
            {"username": "u2", "password": "p", "roles": "superuser"},
        ])
        self.assertTrue(df["selected"].all())
        self.assertEqual(selection, ["u1", "u2"])

    def test_resolve_destination_defaults_to_todas(self):
        result = resolve_destination("missing", self.authenticated)
        self.assertEqual(result, "Todas")


if __name__ == "__main__":
    unittest.main()
