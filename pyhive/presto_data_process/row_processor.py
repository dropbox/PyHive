from pyhive.exc import DataError


class PrestoRowProcessor:
    def __init__(self, root_cell_processors):
        self.root_cell_processors = root_cell_processors

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, PrestoRowProcessor):
            return False

        return self.root_cell_processors == other.root_cell_processors

    def process_row(self, row_raw_data):
        if len(row_raw_data) != len(self.root_cell_processors):
            raise DataError(
                "Expected {} columns while row values count is {}. "
                "Row data: {}".format(len(self.root_cell_processors),
                                      len(row_raw_data),
                                      str(row_raw_data)))

        return tuple(
            process_raw_cell(row_raw_data[column_index])
            for column_index, process_raw_cell in enumerate(self.root_cell_processors)
        )
