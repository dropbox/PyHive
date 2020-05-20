import pyhive.presto_data_process.column_process.varbinary_processor as varbinary_processor
import pyhive.presto_data_process.complex_column_process.array_processor_builder as \
    array_processor_builder
import pyhive.presto_data_process.complex_column_process.map_processor_builder as \
    map_processor_builder
import pyhive.presto_data_process.complex_column_process.inner_row_processor_builder as \
    inner_row_processor_builder

from pyhive.presto_data_process.row_processor import PrestoRowProcessor
from pyhive.presto_data_process.column_process.default_cell_processor import process_raw_cell as \
    default_cell_processor

_complex_cell_processor_builder_by_column_type = {
    "array": array_processor_builder.build_array_processor,
    "map": map_processor_builder.build_map_processor,
    "row": inner_row_processor_builder.build_inner_row_processor
}

_complex_type_signature_extractor_by_column_type = {
    "array": array_processor_builder.extract_inner_type_signatures,
    "map": map_processor_builder.extract_inner_type_signatures,
    "row": inner_row_processor_builder.extract_inner_type_signatures
}

_cell_processors_by_column_type = {
    "varbinary": varbinary_processor.process_raw_cell
}


def build_row_processor(columns):
    root_cell_processors = []

    for column in columns:
        root_cell_processors.append(
            _build_cell_processor(column.get("typeSignature"))
        )

    return PrestoRowProcessor(root_cell_processors)


def _build_cell_processor(column_type_signature):
    column_type = _extract_column_type(column_type_signature)

    if column_type in _complex_cell_processor_builder_by_column_type:
        return _build_complex_cell_processor(column_type_signature, column_type)

    if column_type in _cell_processors_by_column_type:
        return _cell_processors_by_column_type[column_type]

    return default_cell_processor


def _build_complex_cell_processor(column_element, column_type):
    extract_type_signature = _complex_type_signature_extractor_by_column_type[
        column_type]
    inner_type_signatures = extract_type_signature(column_element)

    inner_columns_processors = list(map(
        _build_cell_processor,
        inner_type_signatures
    ))

    build_cell_processor = _complex_cell_processor_builder_by_column_type[column_type]

    return build_cell_processor(column_element, inner_columns_processors)


def _extract_column_type(column_type_signature):
    return column_type_signature.get("rawType")
