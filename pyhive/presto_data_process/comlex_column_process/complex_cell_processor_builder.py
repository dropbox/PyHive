from abc import ABC, abstractmethod


class PrestoComplexCellProcessorBuilder(ABC):
    @abstractmethod
    def build_cell_processor(self, column_type_signature, inner_column_processors):
        pass

    @abstractmethod
    def extract_inner_type_signatures(self, column_type_signature):
        pass
