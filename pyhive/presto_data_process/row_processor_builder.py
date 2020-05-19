import pyhive.presto_data_process.column_process.varbinary_processor as varbinary_processor
import pyhive.presto_data_process.complex_column_process.array_processor_builder as \
    array_processor_builder
import pyhive.presto_data_process.complex_column_process.map_processor_builder as \
    map_processor_builder
import pyhive.presto_data_process.complex_column_process.inner_row_processor_builder as \
    inner_row_processor_builder
import pyhive.presto_data_process.column_process.default_cell_processor as \
    default_cell_processor

from pyhive.presto_data_process.row_processor import PrestoRowProcessor


class PrestoRowProcessorBuilder:
    def __init__(self):
        self.complex_cell_processor_builder_by_column_type = {
            "array": array_processor_builder.build_array_processor,
            "map": map_processor_builder.build_map_processor,
            "row": inner_row_processor_builder.build_inner_row_processor
        }

        self.complex_type_signature_extractor_by_column_type = {
            "array": array_processor_builder.extract_inner_type_signatures,
            "map": map_processor_builder.extract_inner_type_signatures,
            "row": inner_row_processor_builder.extract_inner_type_signatures
        }

        self.cell_processors_by_column_type = {
            "varbinary": varbinary_processor.process_raw_cell
        }

        self.default_cell_processor = default_cell_processor.process_raw_cell

    def build_row_processor(self, columns):
        root_cell_processors = []

        for column in columns:
            root_cell_processors.append(
                self._build_cell_processor(column.get("typeSignature"))
            )

        return PrestoRowProcessor(root_cell_processors)

    def _build_cell_processor(self, column_type_signature):
        column_type = _extract_column_type(column_type_signature)

        if column_type in self.complex_cell_processor_builder_by_column_type:
            return self._build_complex_cell_processor(column_type_signature, column_type)

        if column_type in self.cell_processors_by_column_type:
            return self.cell_processors_by_column_type[column_type]

        return self.default_cell_processor

    def _build_complex_cell_processor(self, column_element, column_type):
        extract_type_signature = self.complex_type_signature_extractor_by_column_type[
            column_type]
        inner_type_signatures = extract_type_signature(column_element)

        inner_columns_processors = list(map(
            self._build_cell_processor,
            inner_type_signatures
        ))

        build_cell_processor = self.complex_cell_processor_builder_by_column_type[column_type]

        return build_cell_processor(column_element, inner_columns_processors)


def _extract_column_type(column_type_signature):
    return column_type_signature.get("rawType")
