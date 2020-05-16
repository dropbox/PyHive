from pyhive.presto_data_process.cell_processor import PrestoCellProcessor

key_parsers = {
    'integer': lambda v: int(v),
    'tinyint': lambda v: int(v),
    'smallint': lambda v: int(v),
    'bigint': lambda v: int(v),
    'real': lambda v: float(v),
    'double': lambda v: float(v),
    'decimal': lambda v: float(v),
    'varbinary': lambda v: v.encode('utf-8')
}


def _default_key_parser(v):
    return v


class PrestoMapProcessor(PrestoCellProcessor):
    def __init__(self, map_values_cell_processor, key_primitive_type):
        self.map_values_cell_processor = map_values_cell_processor
        self._parse_key = key_parsers.get(key_primitive_type, _default_key_parser)

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, PrestoMapProcessor):
            return False

        return self.map_values_cell_processor == other.map_values_cell_processor \
            and self._parse_key == other._parse_key

    def process_raw_cell(self, raw_cell):
        if raw_cell is None:
            return None

        processed_cell = dict()

        for map_key, map_value in raw_cell.items():
            parsed_key = self._parse_key(map_key)
            processed_cell[parsed_key] = self.map_values_cell_processor.process_raw_cell(map_value)

        return processed_cell
