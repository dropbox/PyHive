import typing
from pyhive.presto_data_process.cell_processor import PrestoCellProcessor


class PrestoInnerRowProcessor(PrestoCellProcessor):
    def __init__(self,
                 inner_column_names: typing.List[str],
                 inner_columns_processors: typing.List[PrestoCellProcessor]):
        self.inner_column_names = inner_column_names
        self.inner_columns_processors = inner_columns_processors

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, PrestoInnerRowProcessor):
            return False

        return self.inner_column_names == other.inner_column_names and self.inner_columns_processors == other.inner_columns_processors

    def process_raw_cell(self, raw_cell):
        if raw_cell is None:
            return None

        row_cell_as_dictionary = dict()

        for inner_value_index, inner_value in enumerate(raw_cell):
            inner_column_name = self.inner_column_names[inner_value_index]
            inner_value_cell_processor = self.inner_columns_processors[inner_value_index]

            row_cell_as_dictionary[inner_column_name] = inner_value_cell_processor.process_raw_cell(inner_value)

        return row_cell_as_dictionary