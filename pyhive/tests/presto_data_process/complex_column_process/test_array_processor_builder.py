from unittest import TestCase
from mock import MagicMock, patch
from pyhive.presto_data_process.complex_column_process.array_processor_builder import \
    build_array_processor, extract_inner_type_signatures


class TestPrestoArrayProcessorBuilder(TestCase):
    array_type_signature = {
        "rawType": "array",
        "arguments": [
            {
                "kind": "TYPE",
                "value": {
                    "rawType": "integer",
                    "arguments": []
                }
            }
        ]
    }

    def test_given_array_type_signature_should_return_the_type_signature_of_values(
            self):
        expected_inner_types_signatures = [{
            "rawType": "integer",
            "arguments": []
        }]

        self.assertEqual(
            expected_inner_types_signatures,
            extract_inner_type_signatures(self.array_type_signature)
        )

    @patch("pyhive.presto_data_process.complex_column_process.array_processor_builder"
           ".new_process_raw_cell_function")
    def test_when_build_cell_processor_should_return_array_processor_with_match_value_processor(
            self, mocked_new_process_function):
        mocked_cell_processor = MagicMock()

        processor = build_array_processor(
            self.array_type_signature, [mocked_cell_processor])

        mocked_new_process_function.assert_called_once_with(
            mocked_cell_processor
        )
        self.assertTrue(callable(processor))
