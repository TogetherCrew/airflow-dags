from datetime import datetime

from llama_index import Document


def sort_summaries_daily(
    level1_docs: list[Document],
    level2_docs: list[Document],
    daily_docs: list[Document],
) -> list[Document]:
    """
    sort the summaries per day

    Parameters
    -----------
    level1_docs : list[llama_index.Document]
        level 1 documents (thread in discord or topic in discourse)
    level2_docs : list[llama_index.Document]
        level 2 documents (channel in discord or category in discourse)
    daily_docs : list[llama_index.Document]
        documents for daily summaries

    Returns
    --------
    docs_sorted : list[llama_index.Document]
        documents sorted per day
    """
    # Combine all documents into a single list
    all_docs = level1_docs + level2_docs + daily_docs

    # Define a custom sorting key function based on the date in metadata
    def get_date(doc: Document):
        return datetime.strptime(doc.metadata["date"], "%Y-%m-%d")

    # Sort the documents based on the custom key
    docs_sorted = sorted(all_docs, key=get_date)

    return docs_sorted
