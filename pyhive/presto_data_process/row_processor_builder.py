from pyhive.presto_data_process.column_process.varbinary_processor import PrestoVarbinaryProcessor
from pyhive.presto_data_process.complex_column_process.array_processor_builder import \
    PrestoArrayProcessorBuilder
from pyhive.presto_data_process.complex_column_process.map_processor_builder import \
    PrestoMapProcessorBuilder
from pyhive.presto_data_process.complex_column_process.inner_row_processor_builder import \
    PrestoInnerRowProcessorBuilder
from pyhive.presto_data_process.column_process.default_cell_processor import \
    PrestoDefaultCellProcessor
from pyhive.presto_data_process.row_processor import PrestoRowProcessor


class PrestoRowProcessorBuilder:
    def __init__(self):
        self.complex_cell_processor_builder_by_column_type = {
            "array": PrestoArrayProcessorBuilder(),
            "map": PrestoMapProcessorBuilder(),
            "row": PrestoInnerRowProcessorBuilder()
        }

        self.cell_processors_by_column_type = {
            "varbinary": PrestoVarbinaryProcessor()
        }

        self.default_cell_processor = PrestoDefaultCellProcessor()

    def build_row_processor(self, columns):
        root_cell_processors = []

        for column in columns:
            root_cell_processors.append(
                self._build_cell_processor(column.get("typeSignature"))
            )

        return PrestoRowProcessor(
            root_cell_processors
        )

    def _build_cell_processor(self, column_type_signature):
        column_type = _extract_column_type(column_type_signature)

        if column_type in self.complex_cell_processor_builder_by_column_type:
            return self._build_complex_cell_processor(column_type_signature, column_type)

        if column_type in self.cell_processors_by_column_type:
            return self.cell_processors_by_column_type[column_type]

        return self.default_cell_processor

    def _build_complex_cell_processor(self, column_element, column_type):
        match_cell_processor_builder = self.complex_cell_processor_builder_by_column_type[
            column_type]
        inner_type_signatures = match_cell_processor_builder.extract_inner_type_signatures(
            column_element)

        inner_columns_processors = list(map(
            self._build_cell_processor,
            inner_type_signatures
        ))

        return match_cell_processor_builder.build_cell_processor(column_element,
                                                                 inner_columns_processors)


def _extract_column_type(column_type_signature):
    return column_type_signature.get("rawType")
