def new_process_raw_cell_function(process_inner_raw_cell):
    def process_raw_cell(raw_cell):
        if raw_cell is None:
            return None

        return list(
            process_inner_raw_cell(value)
            for value in raw_cell
        )

    return process_raw_cell
