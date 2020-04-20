from abc import ABCMeta, abstractmethod


class PrestoCellProcessor:
    __metaclass__ = ABCMeta

    @abstractmethod
    def process_raw_cell(self, raw_cell):
        pass
