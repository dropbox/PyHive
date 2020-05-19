from pyhive.presto_data_process.complex_column_process.array_processor import \
    new_process_raw_cell_function


def build_array_processor(column_type_signature, inner_column_processors):
    return new_process_raw_cell_function(inner_column_processors[0])


def extract_inner_type_signatures(column_type_signature):
    return [column_type_signature.get("arguments")[0].get("value")]
