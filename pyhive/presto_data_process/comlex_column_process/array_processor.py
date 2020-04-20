from pyhive.presto_data_process.cell_processor import PrestoCellProcessor


class PrestoArrayProcessor(PrestoCellProcessor):
    def __init__(self, array_values_cell_processor: PrestoCellProcessor):
        self.array_values_cell_processor = array_values_cell_processor

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, PrestoArrayProcessor):
            return False

        return self.array_values_cell_processor == other.array_values_cell_processor

    def process_raw_cell(self, raw_cell):
        if raw_cell is None:
            return None

        return list(
            self.array_values_cell_processor.process_raw_cell(value)
            for value in raw_cell
        )
