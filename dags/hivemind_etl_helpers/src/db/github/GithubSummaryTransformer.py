from typing import Any
from llama_index.core import Document

import unittest

from typing import Any



class SummaryTransformer():
    
    def transform(self, summary: str, metadata: dict[str, Any], **kwargs) -> Document:
       
        pass
class GithubSummaryTransformer(SummaryTransformer):
    def transform(self, summary: str, metadata: dict[str, Any], **kwargs) -> Document:
        excluded_llm_metadata_keys = kwargs.get("excluded_llm_metadata_keys", [])
        excluded_embed_metadata_keys = kwargs.get("excluded_embed_metadata_keys", [])

        document = Document(
            text=summary,
            metadata=metadata,
            excluded_embed_metadata_keys=excluded_embed_metadata_keys,
            excluded_llm_metadata_keys=excluded_llm_metadata_keys,
        )
        return document


