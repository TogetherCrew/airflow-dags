import os

import cohere
from dotenv import load_dotenv
from llama_index.embeddings import BaseEmbedding


class CohereEmbedding(BaseEmbedding):
    def __init__(self):
        super().__init__()

    def prepare_cohere(
        self,
    ) -> cohere.Client:
        """
        setup cohere client
        https://cohere.com/


        Returns
        --------
        client : cohere.Client
            the cohere client to query anything
        """
        load_dotenv()
        key = os.getenv("COHERE_API_KEY")

        client = cohere.Client(key)
        return client

    def get_text_embedding(
        self, text: str | None = None, texts: list[str] | None = None
    ) -> list[float]:
        co = self.prepare_cohere()

        if text is not None:
            response = co.embed(
                texts=[text],
                model="embed-multilingual-v3.0",
                input_type="classification",
                truncate=None,
            )
            # checking the output to be right
            assert len(response.embeddings) == 1

            return response.embeddings[0]
        elif texts is not None:
            response = co.embed(
                texts=texts,
                model="embed-multilingual-v3.0",
                input_type="classification",
                truncate=None,
            )
            return response.embeddings
        else:
            raise ValueError("Both inputs cannot be None")

    def _get_text_embeddings(self, texts: list[str]) -> list[list[float]]:
        """Get text embeddings.

        By default, this is a wrapper around _get_text_embedding.
        Can be overridden for batch queries.

        """
        return self.get_text_embedding(texts=texts)

    def _get_text_embedding(self, text: str) -> list[float]:
        """Get text embedding."""
        return self.get_text_embedding(text=text)

    def _get_query_embedding(self, query: str) -> list[float]:
        """Get query embedding."""
        return self.get_text_embedding(text=query)

    async def _aget_query_embedding(self, query: str) -> list[float]:
        """The asynchronous version of _get_query_embedding."""
        raise NotImplementedError("Not implemented!")
