from typing import Callable

from llama_index.node_parser import SimpleNodeParser
from llama_index.node_parser.text.sentence import CHUNKING_REGEX, DEFAULT_PARAGRAPH_SEP
from llama_index.text_splitter import SentenceSplitter
from llama_index.utils import globals_helper


def configure_node_parser(
    chunk_size: int = 256, chunk_overlap: int = 20, **kwargs
) -> SimpleNodeParser:
    """
    Create SimpleNodeParser from documents

    Parameters
    -----------
    chunk_size : int
        the chunk size for splitting the documents
    chunk_overlap : int
        the overlap between chunks
    **kwargs :
        tokenizer : Callable
            Tokenizer for splitting words into tokens
            can be `tiktoken.encoding_for_model("gpt-3.5-turbo").encode`
            default is the default being used in SentenceSplitter
            which is an encoder for gpt2
        paragraph_separator : str
            the splitter between paragraphs
            default is `DEFAULT_PARAGRAPH_SEP` which is
            in sentence_splitter module of llama_index
        secondary_chunking_regex : str
            The backup regex for splitting into sentences
            default is `CHUNKING_REGEX` which is in
            sentence_splitter module of llama_index
        separator : str
            separator for splitting into words
            default is one space: `" "`

    Returns
    ---------
    node_parser : SimpleNodeParser
        the node parser configured specifically based on inputs
    """
    tokenizer: Callable = globals_helper.tokenizer
    if "tokenizer" in kwargs:
        tokenizer = kwargs["tokenizer"]  # type: ignore

    separator: str = " "
    if "separator" in kwargs:
        separator = kwargs["separator"]  # type: ignore

    secondary_chunking_regex = CHUNKING_REGEX
    if "secondary_chunking_regex" in kwargs:
        secondary_chunking_regex = kwargs["secondary_chunking_regex"]  # type: ignore

    paragraph_separator = DEFAULT_PARAGRAPH_SEP
    if "paragraph_separator" in kwargs:
        paragraph_separator = kwargs["paragraph_separator"]  # type: ignore

    text_splitter = SentenceSplitter(
        separator=separator,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        tokenizer=tokenizer,
        paragraph_separator=paragraph_separator,
        secondary_chunking_regex=secondary_chunking_regex,
    )
    # llama_index did an update on this
    node_parser = text_splitter

    return node_parser
