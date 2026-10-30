import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.common.credentials import api_key, request_headers


class CredentialTests(unittest.TestCase):
    def test_named_keys_are_selected_independently(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict("os.environ", {}, clear=True):
            path = Path(directory) / "keys.json"
            path.write_text(json.dumps({
                "API_NINJAS": {"api_key": "ninjas-placeholder"},
                "OTHER_API": {"api_key": "other-placeholder"},
            }))
            with patch("src.common.credentials.CREDENTIALS_FILE", path):
                self.assertEqual(api_key("OTHER_API", "OTHER_API_KEY"), "other-placeholder")
                self.assertEqual(request_headers("https://api.api-ninjas.com/v1/commodityprice"),
                                 {"X-Api-Key": "ninjas-placeholder"})
                self.assertEqual(request_headers("https://other.example/api"), {})
                with patch.dict("os.environ", {"API_NINJAS_KEY": "environment-placeholder"}):
                    self.assertEqual(request_headers("https://api.api-ninjas.com/v1/commodityprice"),
                                     {"X-Api-Key": "environment-placeholder"})

    def test_invalid_credential_file_does_not_expose_its_contents(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict("os.environ", {}, clear=True):
            path = Path(directory) / "keys.json"
            path.write_text('{private-placeholder')
            with patch("src.common.credentials.CREDENTIALS_FILE", path):
                with self.assertRaisesRegex(ValueError, "must contain valid JSON") as error:
                    api_key("API_NINJAS", "API_NINJAS_KEY")
                self.assertNotIn("private-placeholder", str(error.exception))

    @patch.dict("os.environ", {"API_NINJAS_KEY": "test-placeholder"})
    def test_key_is_scoped_to_exact_https_host(self):
        self.assertEqual(request_headers("https://api.api-ninjas.com/v1/commodityprice"),
                         {"X-Api-Key": "test-placeholder"})
        for url in ["https://example.com/", "http://api.api-ninjas.com/",
                    "https://api.api-ninjas.com.example.com/", "https://api-ninjas.com/"]:
            self.assertEqual(request_headers(url), {})

    @patch.dict("os.environ", {"API_NINJAS_KEY": "bad\nkey"})
    def test_multiline_key_is_rejected(self):
        with self.assertRaises(ValueError):
            request_headers("https://api.api-ninjas.com/v1/commodityprice")
