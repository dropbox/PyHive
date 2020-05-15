from unittest import TestCase
from mock import MagicMock
from pyhive.presto_data_process.comlex_column_process.inner_row_processor_builder import \
    PrestoInnerRowProcessorBuilder
from pyhive.presto_data_process.comlex_column_process.inner_row_processor import \
    PrestoInnerRowProcessor
from pyhive.presto_data_process.cell_processor import PrestoCellProcessor


class TestPrestoMapProcessorBuilder(TestCase):
    _inner_row_type_signature = {
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

    _old_type_signature = {
        'rawType': 'row',
        'typeArguments': [
            {
                'rawType': 'integer',
                'typeArguments': [],
                'literalArguments': [],
                'arguments': []
            },
            {
                'rawType': 'integer',
                'typeArguments': [],
                'literalArguments': [],
                'arguments': []
            },
            {
                'rawType': 'array',
                'typeArguments': [
                    {
                        'rawType': 'integer',
                        'typeArguments': [],
                        'literalArguments': [],
                        'arguments': []
                    }
                ],
                'literalArguments': [],
                'arguments': [{
                    'kind': 'TYPE_SIGNATURE',
                    'value': {
                        'rawType': 'integer',
                        'typeArguments': [],
                        'literalArguments': [],
                        'arguments': []
                    }
                }
                ]
            }
        ],
        'literalArguments': [
            'inner_int1',
            'inner_int2',
            'inner_int_array'
        ],
        'arguments': [
            {
                'kind': 'NAMED_TYPE_SIGNATURE',
                'value': {
                    'fieldName': {
                        'name': 'inner_int1',
                        'delimited': False
                    },
                    'typeSignature': 'integer'}
            }, {
                'kind': 'NAMED_TYPE_SIGNATURE',
                'value': {
                    'fieldName': {
                        'name': 'inner_int2',
                        'delimited': False
                    },
                    'typeSignature': 'integer'}
            }, {
                'kind': 'NAMED_TYPE_SIGNATURE',
                'value': {
                    'fieldName': {
                        'name': 'inner_int_array',
                        'delimited': False
                    },
                    'typeSignature': 'array(integer)'}
            }
        ]
    }

    def test_given_inner_row_type_signature_when_extract_should_return_expected_type_signatures(
            self):
        expected_inner_type_signatures = [
            {
                "rawType": "integer",
                "arguments": []
            },
            {
                "rawType": "integer",
                "arguments": []
            }
        ]

        presto_inner_row_processor_builder = PrestoInnerRowProcessorBuilder()

        self.assertEqual(
            expected_inner_type_signatures,
            presto_inner_row_processor_builder.extract_inner_type_signatures(
                self._inner_row_type_signature)
        )

    def test_when_build_cell_processor_should_return_expected_inner_row_processor(
            self):
        mocked_cell_processors = [
            MagicMock(
                spec=PrestoCellProcessor
            ),
            MagicMock(
                spec=PrestoCellProcessor
            )
        ]

        expected_presto_inner_row_processor = PrestoInnerRowProcessor(
            inner_columns_processors=mocked_cell_processors,
            inner_column_names=["v3", "v4"]
        )

        presto_inner_row_processor_builder = PrestoInnerRowProcessorBuilder()

        self.assertEqual(
            expected_presto_inner_row_processor,
            presto_inner_row_processor_builder.build_cell_processor(self._inner_row_type_signature,
                                                                    mocked_cell_processors)
        )

    def test_given_missing_inner_column_names_should_return_processor_with_generated_field_names(
            self):
        inner_row_type_signature_with_no_inner_column_names = {
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
                },
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

        mocked_cell_processors = [
            MagicMock(
                spec=PrestoCellProcessor
            ),
            MagicMock(
                spec=PrestoCellProcessor
            )
        ]

        expected_presto_inner_row_processor = PrestoInnerRowProcessor(
            inner_columns_processors=mocked_cell_processors,
            inner_column_names=["field0", "field1"]
        )

        presto_inner_row_processor_builder = PrestoInnerRowProcessorBuilder()

        self.assertEqual(
            expected_presto_inner_row_processor,
            presto_inner_row_processor_builder.build_cell_processor(
                inner_row_type_signature_with_no_inner_column_names,
                mocked_cell_processors)
        )

    def test_given_old_type_signature_should_extract_expected_type_signatures(self):
        presto_inner_row_processor_builder = PrestoInnerRowProcessorBuilder()

        expected_type_signatures = [
            {
                'rawType': 'integer',
                'typeArguments': [],
                'literalArguments': [],
                'arguments': []
            },
            {
                'rawType': 'integer',
                'typeArguments': [],
                'literalArguments': [],
                'arguments': []
            },
            {
                'rawType': 'array',
                'typeArguments': [
                    {
                        'rawType': 'integer',
                        'typeArguments': [],
                        'literalArguments': [],
                        'arguments': []
                    }
                ],
                'literalArguments': [],
                'arguments': [{
                    'kind': 'TYPE_SIGNATURE',
                    'value': {
                        'rawType': 'integer',
                        'typeArguments': [],
                        'literalArguments': [],
                        'arguments': []
                    }
                }
                ]
            }
        ]

        self.assertEqual(
            expected_type_signatures,
            presto_inner_row_processor_builder.extract_inner_type_signatures(
                self._old_type_signature
            )
        )

    def test_given_old_type_signature_should_build_expected_processor(self):
        mocked_cell_processors = [
            MagicMock(
                spec=PrestoCellProcessor
            ),
            MagicMock(
                spec=PrestoCellProcessor
            ),
            MagicMock(
                spec=PrestoCellProcessor
            )
        ]

        expected_inner_row_processor = PrestoInnerRowProcessor(
            inner_columns_processors=mocked_cell_processors,
            inner_column_names=['inner_int1', 'inner_int2', 'inner_int_array']
        )

        processor_builder = PrestoInnerRowProcessorBuilder()

        self.assertEqual(
            expected_inner_row_processor,
            processor_builder.build_cell_processor(
                self._old_type_signature,
                mocked_cell_processors)
        )

    def test_given_old_type_signature_with_nameless_columns_should_return_expected_processor(self):
        _old_type_signature_with_nameless_columns = {
            'rawType': 'row',
            'typeArguments': [
                {
                    'rawType': 'integer',
                    'typeArguments': [],
                    'literalArguments': [],
                    'arguments': []
                },
                {
                    'rawType': 'integer',
                    'typeArguments': [],
                    'literalArguments': [],
                    'arguments': []
                },
                {
                    'rawType': 'array',
                    'typeArguments': [
                        {
                            'rawType': 'integer',
                            'typeArguments': [],
                            'literalArguments': [],
                            'arguments': []
                        }
                    ],
                    'literalArguments': [],
                    'arguments': [{
                        'kind': 'TYPE_SIGNATURE',
                        'value': {
                            'rawType': 'integer',
                            'typeArguments': [],
                            'literalArguments': [],
                            'arguments': []
                        }
                    }
                    ]
                }
            ],
            'literalArguments': [
                None,
                None,
                None
            ],
            'arguments': [
                {
                    'kind': 'NAMED_TYPE_SIGNATURE',
                    'value': {
                        'fieldName': {
                            'name': 'inner_int1',
                            'delimited': False
                        },
                        'typeSignature': 'integer'}
                }, {
                    'kind': 'NAMED_TYPE_SIGNATURE',
                    'value': {
                        'fieldName': {
                            'name': 'inner_int2',
                            'delimited': False
                        },
                        'typeSignature': 'integer'}
                }, {
                    'kind': 'NAMED_TYPE_SIGNATURE',
                    'value': {
                        'fieldName': {
                            'name': 'inner_int_array',
                            'delimited': False
                        },
                        'typeSignature': 'array(integer)'}
                }
            ]
        }

        mocked_cell_processors = [
            MagicMock(
                spec=PrestoCellProcessor
            ),
            MagicMock(
                spec=PrestoCellProcessor
            ),
            MagicMock(
                spec=PrestoCellProcessor
            )
        ]

        expected_inner_row_processor = PrestoInnerRowProcessor(
            inner_columns_processors=mocked_cell_processors,
            inner_column_names=['field0', 'field1', 'field2']
        )

        processor_builder = PrestoInnerRowProcessorBuilder()

        self.assertEqual(
            expected_inner_row_processor,
            processor_builder.build_cell_processor(
                _old_type_signature_with_nameless_columns,
                mocked_cell_processors)
        )