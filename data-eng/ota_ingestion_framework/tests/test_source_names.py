"""Readable endpoint names must not merge distinct candidate URLs."""

import unittest

from src.prepare_sources import assign_source_ids, endpoint_source_id, urls_in


class SourceNameTests(unittest.TestCase):
    def test_only_exact_url_strings_are_duplicates(self):
        urls = [
            "https://example.com", "https://example.com/",
            "https://example.com/?utm_source=team",
            "https://example.com/?a=1&b=2", "https://example.com/?b=2&a=1",
            "https://example.com/?q=a%20b", "https://example.com/?q=a+b",
            "https://EXAMPLE.com/", "https://example.com/#prices",
        ]
        extracted = list(urls_in("\n".join(urls + [urls[0]])))
        self.assertEqual(extracted, urls + [urls[0]])
        self.assertEqual(len(set(extracted)), len(urls))

    def test_credentials_are_still_rejected(self):
        self.assertEqual(list(urls_in("https://example.com/?api_key=secret")), [])

    def test_endpoint_and_query_are_readable(self):
        self.assertEqual(
            endpoint_source_id("https://example.com/api/prices?page=2"),
            "example_com_api_prices_page_2",
        )

    def test_collisions_keep_both_sources_and_reserve_registered_names(self):
        urls = ["https://example.com/a-b", "https://example.com/a_b", "https://other.com/"]
        names = assign_source_ids(urls, {urls[2]: ("example_com_a_b", "Other")})
        self.assertEqual(len(set(names.values())), 3)
        self.assertEqual(names[urls[2]], "example_com_a_b")
        self.assertEqual(names, assign_source_ids(reversed(urls), {urls[2]: ("example_com_a_b", "Other")}))

    def test_old_candidate_name_is_replaced(self):
        url = "https://example.com/prices"
        self.assertEqual(
            assign_source_ids([url], {url: ("candidate_123abc", "Prices")})[url],
            "example_com_prices",
        )
