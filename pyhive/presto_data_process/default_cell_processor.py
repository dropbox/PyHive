from pyhive.presto_data_process.cell_processor import PrestoCellProcessor


class PrestoDefaultCellProcessor(PrestoCellProcessor):
    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, PrestoDefaultCellProcessor):
            return False

        return True

    def process_raw_cell(self, raw_cell):
        return raw_cell
