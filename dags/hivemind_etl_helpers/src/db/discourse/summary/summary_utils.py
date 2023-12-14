from llama_index import Document


def transform_summary_to_document(
    summary: str,
    date: str,
    topic: str = None,
    category: str = None,
) -> Document:
    """
    prepare a llama_index.Document for the given summary

    Parameters
    ------------
    summary : str
        the summary prepared for the topic
    date : str
        the date of the summary in format of `%Y-%m-%d`
    topic : str | None
        the topic title of the summary
        if the summary was related to a category or day, this would be `None`
    category : str
        the category that topic is related to
        if the summary was for a day, this would be `None`

    Returns
    ---------
    prepared_document : llama_index.Document
        the prepared document for the summary
    """

    prepared_document = Document(
        text=summary,
        metadata={
            "topic": topic,
            "category": category,
            "date": date,
        },
    )

    return prepared_document
