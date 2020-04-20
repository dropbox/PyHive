from base64 import b64decode

from pyhive.presto_data_process.cell_processor import PrestoCellProcessor


class PrestoVarbinaryProcessor(PrestoCellProcessor):
    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, PrestoVarbinaryProcessor):
            return False

        return True

    def process_raw_cell(self, raw_cell):
        if raw_cell is None:
            return None

        return b64decode(raw_cell)
