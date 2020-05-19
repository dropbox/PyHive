from pyhive.presto_data_process.complex_column_process.map_processor import \
    new_map_process_function


def extract_inner_type_signatures(column_type_signature):
    return [column_type_signature.get("arguments")[1].get("value")]


def build_map_processor(column_type_signature, inner_column_processors):
    key_primitive_type = _extract_key_primitive_type(column_type_signature)

    return new_map_process_function(inner_column_processors[0], key_primitive_type)


def _extract_key_primitive_type(column_type_signature):
    return column_type_signature.get('arguments')[0].get('value').get('rawType')
