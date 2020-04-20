from pyhive.presto_data_process.comlex_column_process.complex_cell_processor_builder import \
    PrestoComplexCellProcessorBuilder
from pyhive.presto_data_process.comlex_column_process.inner_row_processor import PrestoInnerRowProcessor


class PrestoInnerRowProcessorBuilder(PrestoComplexCellProcessorBuilder):
    def build_cell_processor(self, column_type_signature, inner_column_processors):
        inner_columns = extract_inner_column_elements(column_type_signature)
        inner_columns_names = list(
            extract_inner_column_name(inner_column_index, inner_column_element)
            for inner_column_index, inner_column_element in enumerate(inner_columns)
        )

        return PrestoInnerRowProcessor(
            inner_columns_names,
            inner_column_processors
        )

    def extract_inner_type_signatures(self, column_type_signature):
        return list(
            inner_column.get("typeSignature")
            for inner_column in extract_inner_column_elements(column_type_signature)
        )


def extract_inner_column_name(inner_column_index, inner_column_element):
    if "fieldName" in inner_column_element:
        return inner_column_element.get("fieldName").get("name")

    # Handle the same way presto-cli handles elements in row with no field name
    return 'field' + str(inner_column_index)


def extract_inner_column_elements(column_type_signature):
    return list(
        type_argument.get("value")
        for type_argument in column_type_signature.get("arguments")
    )
