import unittest

from src.common.api_collection import iter_pages
from src.common.source_testing import ProbeError
from src.common.fetching_method import fetch_json_records


class SingleObjectTests(unittest.TestCase):
    def test_one_quote_is_one_record_and_one_request(self):
        calls=[]
        record={'name':'Coffee','price':123,'unit':'lb'}
        def request(url,params):
            calls.append(url)
            return record
        pages=list(iter_pages(request,'https://example.com/quote',
                              {'record_shape':'object','pagination':{'mode':'none'}}))
        self.assertEqual(pages[0][0],[record])
        self.assertTrue(pages[0][2])
        self.assertEqual(len(calls),1)

    def test_error_object_is_not_saved_as_price(self):
        with self.assertRaises(ProbeError):
            list(iter_pages(lambda *_:{'error':'invalid'},'https://example.com/',
                            {'record_shape':'object','pagination':{'mode':'none'}}))

    def test_probe_fetcher_uses_explicit_shape(self):
        class Client:
            def get(self,*args,**kwargs):
                return b'{"name":"Coffee","price":123}', 'https://example.com/'
        records,_=fetch_json_records(Client(),'https://example.com/',record_shape='object')
        self.assertEqual(len(records),1)
        with self.assertRaises(ValueError):
            fetch_json_records(Client(),'https://example.com/')
