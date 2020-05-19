from unittest import TestCase
from mock import MagicMock, patch
from pyhive.presto_data_process.complex_column_process.map_processor_builder import \
    build_map_processor, extract_inner_type_signatures


class TestPrestoMapProcessorBuilder(TestCase):
    map_type_signature = {
        "rawType": "map",
        "arguments": [
            {
                "kind": "TYPE",
                "value": {
                    "rawType": "varchar",
                    "arguments": [
                        {
                            "kind": "LONG",
                            "value": 2147483647
                        }
                    ]
                }
            },
            {
                "kind": "TYPE",
                "value": {
                    "rawType": "varchar",
                    "arguments": [
                        {
                            "kind": "LONG",
                            "value": 2147483647
                        }
                    ]
                }
            }
        ]
    }

    def test_given_map_type_signature_should_return_the_type_signature_that_presents_the_values(
            self):
        expected_inner_types_signatures = [{
            "rawType": "varchar",
            "arguments": [
                {
                    "kind": "LONG",
                    "value": 2147483647
                }
            ]
        }]

        self.assertEqual(
            expected_inner_types_signatures,
            extract_inner_type_signatures(self.map_type_signature)
        )

    @patch("pyhive.presto_data_process.complex_column_process.map_processor_builder."
           "new_map_process_function")
    def test_when_build_cell_processor_should_return_map_processor_with_match_value_processor(
            self, mocked_new_process_function):
        mocked_cell_processor = MagicMock()

        process_row = build_map_processor(self.map_type_signature, [mocked_cell_processor])

        mocked_new_process_function.assert_called_once_with(
            mocked_cell_processor,
            'varchar'
        )

        self.assertTrue(callable(process_row))
