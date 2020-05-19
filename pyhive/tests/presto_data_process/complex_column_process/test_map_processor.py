from unittest import TestCase
from mock import MagicMock
from pyhive.presto_data_process.complex_column_process.map_processor import \
    new_map_process_function


class TestPrestoMapProcessor(TestCase):
    def test_given_none_cell_when_process_cell_should_return_none(self):
        mocked_cell_processor = MagicMock()

        self.assertIsNone(
            new_map_process_function(mocked_cell_processor, 'varchar')(None)
        )

    def test_given_map_cell_when_process_cell_should_return_the_expected_processed_map(self):
        def mocked_value_cell_processor(v):
            return v * 2

        raw_map_cell = {
            "someKey1": 2,
            "someKey2": 3,
            "key3": 4
        }
        expected_processed_map = {
            "someKey1": 4,
            "someKey2": 6,
            "key3": 8
        }

        process_raw_cell = new_map_process_function(mocked_value_cell_processor, 'varchar')

        self.assertEqual(
            expected_processed_map,
            process_raw_cell(raw_map_cell)
        )

    def test_given_integer_keys_should_return_casted_keys(self):
        raw_map_cell = {
            '22': 42,
            '60': 60
        }
        expected_processed_map = {
            22: 42,
            60: 60
        }

        for integer_type in ['tinyint', 'smallint', 'integer', 'bigint']:
            self._test_given_map_cell_with_primitive_type_key(
                raw_map_cell,
                expected_processed_map,
                integer_type
            )

    def test_given_decimal_keys_should_return_casted_keys(self):
        raw_map_cell = {
            '11.42': 42,
            '7.3': 60
        }
        expected_processed_map = {
            11.42: 42,
            7.3: 60
        }

        for decimal_type in ['real', 'double', 'decimal']:
            self._test_given_map_cell_with_primitive_type_key(
                raw_map_cell,
                expected_processed_map,
                decimal_type
            )

    def test_given_varbinary_keys_should_return_casted_keys(self):
        raw_map_cell = {
            'key1': 42,
            'key2': 60
        }
        expected_processed_map = {
            b'key1': 42,
            b'key2': 60
        }

        self._test_given_map_cell_with_primitive_type_key(
            raw_map_cell,
            expected_processed_map,
            'varbinary'
        )

    def _test_given_map_cell_with_primitive_type_key(
            self, raw_map_cell, expected_processed_map, primitive_type):
        def mocked_value_cell_processor(v):
            return v

        process_raw_cell = new_map_process_function(mocked_value_cell_processor, primitive_type)

        self.assertEqual(
            expected_processed_map,
            process_raw_cell(raw_map_cell)
        )
