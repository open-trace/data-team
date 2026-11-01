import csv
import tempfile
import unittest
from pathlib import Path

from src.review_collected_sources import inspect_export


class RelevanceReviewTests(unittest.TestCase):
    def inspect(self, rows, sid='example'):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'table.csv'
            with path.open('w',newline='') as stream:
                csv.writer(stream).writerows(rows)
            return inspect_export(path,{'source_id':sid,'data_kind':'unclassified_html_table'})

    def test_market_directory_is_not_price_data(self):
        value=self.inspect([['row_number','column_1','column_2'],
                            ['1','Market Name','Market Days'],['2','Example','Monday']])
        self.assertEqual(value['category'],'MARKET_DIRECTORY')
        self.assertEqual(value['price_rows'],0)

    def test_physical_price_table_retains_source_date(self):
        value=self.inspect([['row_number','c1','c2','c3','c4','c5'],
                            ['1','Commodity','Category','Price','Market','Date'],
                            ['2','Maize','Grain','KES 62 / kg','Example','14 Sep 2026']])
        self.assertEqual(value['category'],'MARKET_PRICE_TABLE')
        self.assertEqual(value['latest_observation'],'2026-09-14')
        self.assertEqual(value['price_rows'],1)

    def test_generic_numeric_table_is_not_automatically_price_data(self):
        value=self.inspect([['row_number','column_1','column_2'],
                            ['1','Population','Year'],['2','1000','2026']])
        self.assertEqual(value['category'],'REVIEW_REQUIRED')

    def test_tshwane_schema_is_recognised_with_its_actual_date(self):
        value=self.inspect([['observation_date','commodity_source','average_price','market'],
                            ['2026-10-23','APPLE','100.00','Tshwane']],sid='za_tshwane_market_fresh')
        self.assertEqual(value['category'],'MARKET_PRICE_ROWS')
        self.assertEqual(value['latest_observation'],'2026-10-23')
