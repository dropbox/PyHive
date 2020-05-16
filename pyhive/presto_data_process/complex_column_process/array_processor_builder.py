from pyhive.presto_data_process.complex_column_process.complex_cell_processor_builder import \
    PrestoComplexCellProcessorBuilder
from pyhive.presto_data_process.complex_column_process.array_processor import PrestoArrayProcessor


class PrestoArrayProcessorBuilder(PrestoComplexCellProcessorBuilder):
    def build_cell_processor(self, column_type_signature, inner_column_processors):
        return PrestoArrayProcessor(inner_column_processors[0])

    def extract_inner_type_signatures(self, column_type_signature):
        return [column_type_signature.get("arguments")[0].get("value")]
