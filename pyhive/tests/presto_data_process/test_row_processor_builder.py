from unittest import TestCase
from pyhive.presto_data_process.row_processor_builder import PrestoRowProcessorBuilder
from pyhive.presto_data_process.row_processor import PrestoRowProcessor
from pyhive.presto_data_process.column_process.default_cell_processor import PrestoDefaultCellProcessor
from pyhive.presto_data_process.column_process.varbinary_processor import PrestoVarbinaryProcessor
from pyhive.presto_data_process.comlex_column_process.array_processor import PrestoArrayProcessor
from pyhive.presto_data_process.comlex_column_process.map_processor import PrestoMapProcessor
from pyhive.presto_data_process.comlex_column_process.inner_row_processor import \
    PrestoInnerRowProcessor


class TestPrestoRowProcessorBuilder(TestCase):
    def test_given_columns_when_build_row_processor_should_return_expected_row_processor(self):
        columns = [
            {
                "name": "some_field",
                "type": "row(v1 integer, "
                        "v2 row(v3 integer, v4 integer), "
                        "array(row(integer)), "
                        "b1 varbinary)",
                "typeSignature": {
                    "rawType": "row",
                    "arguments": [
                        {
                            "kind": "NAMED_TYPE",
                            "value": {
                                "fieldName": {
                                    "name": "v1"
                                },
                                "typeSignature": {
                                    "rawType": "integer",
                                    "arguments": []
                                }
                            }
                        },
                        {
                            "kind": "NAMED_TYPE",
                            "value": {
                                "fieldName": {
                                    "name": "v2"
                                },
                                "typeSignature": {
                                    "rawType": "row",
                                    "arguments": [
                                        {
                                            "kind": "NAMED_TYPE",
                                            "value": {
                                                "fieldName": {
                                                    "name": "v3"
                                                },
                                                "typeSignature": {
                                                    "rawType": "integer",
                                                    "arguments": []
                                                }
                                            }
                                        },
                                        {
                                            "kind": "NAMED_TYPE",
                                            "value": {
                                                "fieldName": {
                                                    "name": "v4"
                                                },
                                                "typeSignature": {
                                                    "rawType": "integer",
                                                    "arguments": []
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "kind": "NAMED_TYPE",
                            "value": {
                                "typeSignature": {
                                    "rawType": "array",
                                    "arguments": [
                                        {
                                            "kind": "TYPE",
                                            "value": {
                                                "rawType": "row",
                                                "arguments": [
                                                    {
                                                        "kind": "NAMED_TYPE",
                                                        "value": {
                                                            "typeSignature": {
                                                                "rawType": "integer",
                                                                "arguments": []
                                                            }
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "kind": "NAMED_TYPE",
                            "value": {
                                "fieldName": {
                                    "name": "b1"
                                },
                                "typeSignature": {
                                    "rawType": "varbinary",
                                    "arguments": []
                                }
                            }
                        }
                    ]
                }
            },
            {
                "name": "some_binary",
                "type": "varbinary",
                "typeSignature": {
                    "rawType": "varbinary",
                    "arguments": []
                }
            },
            {
                "name": "some_map",
                "type": "map(varchar(1), integer)",
                "typeSignature": {
                    "rawType": "map",
                    "arguments": [
                        {
                            "kind": "TYPE",
                            "value": {
                                "rawType": "varchar",
                                "arguments": [
                                    {
                                        "kind": "LONG",
                                        "value": 1
                                    }
                                ]
                            }
                        },
                        {
                            "kind": "TYPE",
                            "value": {
                                "rawType": "integer",
                                "arguments": []
                            }
                        }
                    ]
                }
            }
        ]

        expected_row_processor = PrestoRowProcessor(
            root_cell_processors=[
                PrestoInnerRowProcessor(
                    inner_column_names=["v1", "v2", "field2", "b1"],
                    inner_columns_processors=[
                        PrestoDefaultCellProcessor(),
                        PrestoInnerRowProcessor(
                            inner_column_names=["v3", "v4"],
                            inner_columns_processors=[
                                PrestoDefaultCellProcessor(),
                                PrestoDefaultCellProcessor()
                            ]
                        ),
                        PrestoArrayProcessor(
                            PrestoInnerRowProcessor(
                                inner_column_names=["field0"],
                                inner_columns_processors=[
                                    PrestoDefaultCellProcessor()
                                ]
                            )
                        ),
                        PrestoVarbinaryProcessor()
                    ]
                ),
                PrestoVarbinaryProcessor(),
                PrestoMapProcessor(
                    PrestoDefaultCellProcessor()
                )
            ]
        )

        presto_row_processor_builder = PrestoRowProcessorBuilder()
        built_row_processor = presto_row_processor_builder.build_row_processor(columns)

        self.assertEqual(
            expected_row_processor,
            built_row_processor
        )
