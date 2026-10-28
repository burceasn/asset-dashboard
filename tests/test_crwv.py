import copy
import json
import unittest
from decimal import Decimal
from pathlib import Path

from sync_data import SyncError, spot_symbol, update_document
from update_data import DataError, instrument_for, validate_document


class CrwvTests(unittest.TestCase):
    def setUp(self):
        self.data = json.loads((Path(__file__).resolve().parents[1] / 'data.json').read_text(encoding='utf-8'))
        self.asset = next(a for a in self.data['holdings'] if a['symbol'] == 'CRWV')

    def test_quote_and_token_mapping(self):
        self.assertEqual(instrument_for(self.asset), 'CRWV-USDT-SWAP')
        targets = {('CRWV', 'long'): self.asset}
        for symbol in ('CRWV', 'crwvb', 'xCRWV'):
            self.assertEqual(spot_symbol(symbol, targets), 'CRWV')

    def test_snapshot_replaces_instead_of_accumulating(self):
        price = lambda _: Decimal('60')
        row = dict(symbol='CRWV', direction='long', quantity=Decimal('3'),
                   exposure=Decimal('180'), cost_quantity=Decimal('3'), cost=Decimal('50'))
        snapshot = ({'CRWVB': Decimal('2'), 'xCRWV': Decimal('1')}, [], price)
        okx = ({}, [row], price)
        first = update_document({'holdings': [self.asset]}, snapshot, okx)
        second = update_document(first, snapshot, okx)
        self.assertEqual(first, second)
        self.assertEqual(first['holdings'][0]['quantity'], Decimal('6'))
        self.assertEqual(first['holdings'][0]['exposure'], Decimal('360'))

    def test_duplicate_positions_rejected(self):
        duplicate = copy.deepcopy(self.asset)
        duplicate['symbol'] = 'crwv'
        self.data['holdings'].append(duplicate)
        with self.assertRaises(DataError):
            validate_document(self.data)
        with self.assertRaises(SyncError):
            update_document(self.data, ({}, [], lambda _: Decimal(1)))
        duplicate['direction'] = 'short'
        validate_document(self.data)


if __name__ == '__main__':
    unittest.main()
