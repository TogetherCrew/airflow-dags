from abc import ABC, abstractmethod
from typing import Any

from llama_index.core import Document


class SummaryTransformer(ABC):

    @abstractmethod
    def transform(self, summary: str, metadata: dict[str, Any], **kwargs) -> Document:
        """
        This abstract method for transforming given summaries

        Parameters
        -----------
        summaries : str
            a summary
        metadata : dict[str, Any]
            a dictionary of metadata to be included in llama-index document

        Returns
        ---------
        summary_doc : llama_index.core.Document
            summary converted to llama-index document
        """
        pass
