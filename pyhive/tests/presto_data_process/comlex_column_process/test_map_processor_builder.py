from unittest import TestCase
from mock import MagicMock
from pyhive.presto_data_process.comlex_column_process.map_processor_builder import PrestoMapProcessorBuilder
from pyhive.presto_data_process.comlex_column_process.map_processor import PrestoMapProcessor
from pyhive.presto_data_process.cell_processor import PrestoCellProcessor


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

        presto_map_processor_builder = PrestoMapProcessorBuilder()

        self.assertEqual(
            expected_inner_types_signatures,
            presto_map_processor_builder.extract_inner_type_signatures(self.map_type_signature)
        )

    def test_when_build_cell_processor_should_return_map_processor_with_match_value_processor(
            self):
        mocked_cell_processor = MagicMock(
            spec=PrestoCellProcessor
        )
        expected_presto_map_processor = PrestoMapProcessor(
            map_values_cell_processor=mocked_cell_processor
        )

        presto_map_processor_builder = PrestoMapProcessorBuilder()

        self.assertEqual(
            expected_presto_map_processor,
            presto_map_processor_builder.build_cell_processor(self.map_type_signature, [mocked_cell_processor])
        )
