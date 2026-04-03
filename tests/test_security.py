import unittest
from unittest.mock import patch

from app import app, _validate_tx_form


class SecurityTests(unittest.TestCase):
    def setUp(self):
        app.config["TESTING"] = True
        self.client = app.test_client()

    def test_post_without_csrf_is_rejected(self):
        response = self.client.post("/delete/1")
        self.assertEqual(response.status_code, 400)

    def test_add_with_csrf_and_valid_data(self):
        get_resp = self.client.get("/add")
        self.assertEqual(get_resp.status_code, 200)

        with self.client.session_transaction() as sess:
            token = sess.get("csrf_token")

        payload = {
            "csrf_token": token,
            "profile": "Anna",
            "date": "2026-01-15",
            "category": "Food",
            "subcategory": "Groceries",
            "amount": "10.50",
            "currency": "EUR",
            "amount_eur": "10.50",
            "type": "Expense",
            "notes": "test",
        }

        with patch("app.db.execute", return_value=None):
            response = self.client.post("/add", data=payload, follow_redirects=False)

        self.assertEqual(response.status_code, 302)


class ValidationTests(unittest.TestCase):
    def test_reject_invalid_currency(self):
        error, clean = _validate_tx_form(
            {
                "profile": "Anna",
                "type": "Expense",
                "category": "Food",
                "subcategory": "Groceries",
                "date": "2026-01-15",
                "amount": "20",
                "amount_eur": "20",
                "currency": "BTC",
                "notes": "",
            }
        )
        self.assertIsNotNone(error)
        self.assertIsNone(clean)

    def test_accept_valid_payload(self):
        error, clean = _validate_tx_form(
            {
                "profile": "Federico",
                "type": "Income",
                "category": "Salary",
                "subcategory": "",
                "date": "2026-01-15",
                "amount": "1000",
                "amount_eur": "1000",
                "currency": "EUR",
                "notes": "salary",
            }
        )
        self.assertIsNone(error)
        self.assertEqual(clean["profile"], "Federico")


if __name__ == "__main__":
    unittest.main()
