def new_inner_row_process_function(inner_column_names, inner_columns_processors):
    def process_raw_cell(raw_cell):
        if raw_cell is None:
            return None

        row_cell_as_dictionary = dict()

        for inner_value_index, inner_value in enumerate(raw_cell):
            inner_column_name = inner_column_names[inner_value_index]
            process_inner_raw_cell = inner_columns_processors[inner_value_index]

            row_cell_as_dictionary[inner_column_name] = process_inner_raw_cell(inner_value)

        return row_cell_as_dictionary

    return process_raw_cell
