from base64 import b64decode


def process_raw_cell(raw_cell):
    if raw_cell is None:
        return None

    return b64decode(raw_cell)
