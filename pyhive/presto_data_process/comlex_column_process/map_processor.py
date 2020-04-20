from pyhive.presto_data_process.cell_processor import PrestoCellProcessor


class PrestoMapProcessor(PrestoCellProcessor):
    def __init__(self, map_values_cell_processor):
        self.map_values_cell_processor = map_values_cell_processor

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, PrestoMapProcessor):
            return False

        return self.map_values_cell_processor == other.map_values_cell_processor

    def process_raw_cell(self, raw_cell):
        if raw_cell is None:
            return None

        processed_cell = dict()

        for map_key, map_value in raw_cell.items():
            processed_cell[map_key] = self.map_values_cell_processor.process_raw_cell(map_value)

        return processed_cell
