from unittest import TestCase
from mock import MagicMock, patch
from pyhive.presto_data_process.complex_column_process.inner_row_processor_builder import \
    extract_inner_type_signatures, build_inner_row_processor


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

        self.assertEqual(
            expected_inner_type_signatures,
            extract_inner_type_signatures(
                self._inner_row_type_signature)
        )

    @patch("pyhive.presto_data_process.complex_column_process.inner_row_processor_builder."
           "new_inner_row_process_function")
    def test_when_build_cell_processor_should_return_expected_inner_row_processor(
            self, mocked_new_process_function):
        mocked_cell_processors = [
            MagicMock(),
            MagicMock()
        ]

        process_cell = build_inner_row_processor(self._inner_row_type_signature,
                                                 mocked_cell_processors)

        mocked_new_process_function.assert_called_once_with(
            ["v3", "v4"],
            mocked_cell_processors
        )

        self.assertTrue(callable(process_cell))

    @patch("pyhive.presto_data_process.complex_column_process.inner_row_processor_builder."
           "new_inner_row_process_function")
    def test_given_missing_inner_column_names_should_return_processor_with_generated_field_names(
            self, mocked_new_process_function):
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
            MagicMock(),
            MagicMock()
        ]

        process_cell = build_inner_row_processor(
            inner_row_type_signature_with_no_inner_column_names,
            mocked_cell_processors
        )

        mocked_new_process_function.assert_called_once_with(
            ["field0", "field1"],
            mocked_cell_processors
        )

        self.assertTrue(callable(process_cell))

    def test_given_old_type_signature_should_extract_expected_type_signatures(self):
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
            extract_inner_type_signatures(
                self._old_type_signature
            )
        )

    @patch("pyhive.presto_data_process.complex_column_process.inner_row_processor_builder."
           "new_inner_row_process_function")
    def test_given_old_type_signature_should_build_expected_processor(
            self, mocked_new_process_function):
        mocked_cell_processors = [
            MagicMock(),
            MagicMock(),
            MagicMock()
        ]

        process_cell = build_inner_row_processor(
            self._old_type_signature,
            mocked_cell_processors
        )

        mocked_new_process_function.assert_called_once_with(
            ['inner_int1', 'inner_int2', 'inner_int_array'],
            mocked_cell_processors
        )

        self.assertTrue(callable(process_cell))

    @patch("pyhive.presto_data_process.complex_column_process.inner_row_processor_builder."
           "new_inner_row_process_function")
    def test_given_old_type_signature_with_nameless_columns_should_return_expected_processor(
            self, mocked_new_process_function):
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
            MagicMock(),
            MagicMock(),
            MagicMock()
        ]

        process_cell = build_inner_row_processor(
            _old_type_signature_with_nameless_columns,
            mocked_cell_processors
        )

        mocked_new_process_function.assert_called_once_with(
            ['field0', 'field1', 'field2'],
            mocked_cell_processors
        )

        self.assertTrue(callable(process_cell))
