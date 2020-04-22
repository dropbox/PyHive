from abc import ABCMeta, abstractmethod


class PrestoComplexCellProcessorBuilder:
    __metaclass__ = ABCMeta

    @abstractmethod
    def build_cell_processor(self, column_type_signature, inner_column_processors):
        raise NotImplementedError()

    @abstractmethod
    def extract_inner_type_signatures(self, column_type_signature):
        raise NotImplementedError()
