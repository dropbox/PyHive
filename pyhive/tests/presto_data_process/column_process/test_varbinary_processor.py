from unittest import TestCase
from pyhive.presto_data_process.column_process.varbinary_processor import PrestoVarbinaryProcessor


class TestPrestoVarbinaryProcessor(TestCase):
    def test_given_none_cell_when_process_cell_should_return_none(self):
        self.assertIsNone(PrestoVarbinaryProcessor().process_raw_cell(None))

    def test_given_varbinary_cell_as_base64_string_should_return_bytes(self):
        base64_encoded_cell = "ZWg/"
        expected_bytes = b'eh?'

        self.assertEqual(
            expected_bytes,
            PrestoVarbinaryProcessor().process_raw_cell(base64_encoded_cell)
        )
