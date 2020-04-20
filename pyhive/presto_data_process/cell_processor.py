from abc import ABC, abstractmethod


class PrestoCellProcessor(ABC):
    @abstractmethod
    def process_raw_cell(self, raw_cell):
        pass
