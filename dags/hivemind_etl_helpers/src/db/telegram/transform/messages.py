from llama_index.core import Document
from hivemind_etl_helpers.src.db.telegram.schema import TelegramMessagesModel


class TransformMessages:
    def __init__(self, chat_id: str, chat_name: str) -> None:
        self.chat_id = chat_id
        self.chat_name = chat_name

    def transform(self, messages: list[TelegramMessagesModel]) -> list[Document]:
        """
        transform the given telegram messages to llama-index documents

        Parameters
        ----------
        messages : list[TelegramMessagesModel]
            the extracted telegram messages

        Returns
        ---------
        transformed_docs : list[llama_index.core.Document]
            a list of llama-index documents to be embedded & loaded into db
        """
        transformed_docs: list[Document] = []

        for message in messages:
            document = Document(
                text=message.message_text,
                doc_id=message.message_id,
                metadata={
                    "author": message.author_username,
                    "createdAt": message.message_created_at,
                    "updatedAt": message.message_edited_at,
                    "mentions": message.mentions,
                    "replies": message.repliers,
                    "reactors": message.reactors,
                    "chat_name": self.chat_name,
                },
                excluded_embed_metadata_keys=[
                    "author",
                    "createdAt",
                    "updatedAt",
                    "mentions",
                    "replies",
                    "reactors",
                    "chat_name",
                ],
                excluded_llm_metadata_keys=[
                    "createdAt",
                    "updatedAt",
                    "mentions",
                    "replies",
                    "reactors",
                    "chat_name",
                ],
            )
            transformed_docs.append(document)

        return transformed_docs
