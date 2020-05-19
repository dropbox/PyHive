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


def new_map_process_function(process_inner_raw_cell, key_primitive_type):
    parse_key = key_parsers.get(key_primitive_type, _default_key_parser)

    def process_raw_cell(raw_cell):
        if raw_cell is None:
            return None

        processed_cell = dict()

        for map_key, map_value in raw_cell.items():
            parsed_key = parse_key(map_key)
            processed_cell[parsed_key] = process_inner_raw_cell(map_value)

        return processed_cell

    return process_raw_cell
