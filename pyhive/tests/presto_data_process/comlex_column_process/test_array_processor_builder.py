from unittest import TestCase
from mock import MagicMock
from pyhive.presto_data_process.comlex_column_process.array_processor_builder import \
    PrestoArrayProcessorBuilder
from pyhive.presto_data_process.comlex_column_process.array_processor import PrestoArrayProcessor
from pyhive.presto_data_process.cell_processor import PrestoCellProcessor


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

        presto_array_processor_builder = PrestoArrayProcessorBuilder()

        self.assertEqual(
            expected_inner_types_signatures,
            presto_array_processor_builder.extract_inner_type_signatures(self.array_type_signature)
        )

    def test_when_build_cell_processor_should_return_array_processor_with_match_value_processor(
            self):
        mocked_cell_processor = MagicMock(
            spec=PrestoCellProcessor
        )
        expected_presto_array_processor = PrestoArrayProcessor(
            array_values_cell_processor=mocked_cell_processor
        )

        presto_array_processor_builder = PrestoArrayProcessorBuilder()

        self.assertEqual(
            expected_presto_array_processor,
            presto_array_processor_builder.build_cell_processor(
                self.array_type_signature,
                [mocked_cell_processor]
            )
        )
