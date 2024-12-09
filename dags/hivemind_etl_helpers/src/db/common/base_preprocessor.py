import spacy


class BasePreprocessor:
    def __init__(self) -> None:
        pass

    def extract_main_content(self, text: str) -> str:
        """
        extract main content of a message

        Parameters
        ------------
        text : str
            a discord message text

        Returns
        --------
        cleaned_text : str

        """
        try:
            nlp = spacy.load("en_core_web_lg")
        except OSError as exp:
            raise OSError(f"Model spacy `en_core_web_lg` is not installed!") from exp

        doc = nlp(text)

        # Filter out punctuation, whitespace, and numerical values, then extract the lemma for each remaining token
        main_content_tokens = [
            token.lemma_
            for token in doc
            if not token.is_punct
            and not token.is_space
            and not token.is_stop
            and not token.like_url
            and not token.like_num
            and token.is_ascii
        ]

        # Join the tokens to form the cleaned sentence
        cleaned_text = " ".join(main_content_tokens)
        return cleaned_text
