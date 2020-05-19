from pyhive.presto_data_process.complex_column_process.inner_row_processor import \
    new_inner_row_process_function


def build_inner_row_processor(column_type_signature, inner_column_processors):
    inner_columns_names = _extract_inner_column_names(column_type_signature)

    return new_inner_row_process_function(
        inner_columns_names,
        inner_column_processors
    )


def extract_inner_type_signatures(column_type_signature):
    # In older versions of presto typeArguments contains objects equivalent to typeSignatures
    if "typeArguments" in column_type_signature:
        return column_type_signature.get("typeArguments")

    return list(
        inner_column.get("typeSignature")
        for inner_column in _extract_inner_column_elements(column_type_signature)
    )


def _extract_inner_column_names(column_type_signature):
    if "literalArguments" in column_type_signature:
        return _extract_inner_column_names_from_old_type_signature(
            column_type_signature
        )

    inner_columns = _extract_inner_column_elements(column_type_signature)

    return list(
        _extract_inner_column_name(inner_column_index, inner_column_element)
        for inner_column_index, inner_column_element in enumerate(inner_columns)
    )


def _extract_inner_column_names_from_old_type_signature(column_type_signature):
    literal_arguments = column_type_signature.get("literalArguments")

    return list(
        inner_column_name if inner_column_name is not None else _default_field_name(
            inner_column_index)
        for inner_column_index, inner_column_name in enumerate(literal_arguments)
    )


def _extract_inner_column_name(inner_column_index, inner_column_element):
    if "fieldName" in inner_column_element:
        return inner_column_element.get("fieldName").get("name")

    return _default_field_name(inner_column_index)


def _default_field_name(inner_column_index):
    # Handle the same way presto-cli handles elements in row with no field name
    return 'field' + str(inner_column_index)


def _extract_inner_column_elements(column_type_signature):
    return list(
        type_argument.get("value")
        for type_argument in column_type_signature.get("arguments")
    )
