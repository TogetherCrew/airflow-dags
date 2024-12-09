import re

from hivemind_etl_helpers.src.db.common.base_preprocessor import BasePreprocessor


class DiscordPreprocessor(BasePreprocessor):
    """
    preprocess discord text messages
    """

    def __init__(self) -> None:
        pass

    def clean_texts(self, texts: list[str]) -> list[str]:
        """
        clean the given text

        Parameters
        ------------
        texts : list[str]
            a list of discord messages text

        Returns
        ---------
        texts_cleaned : str
            the cleaned text
            (discord ids removed)
        """
        texts_cleaned: list[str] = []

        for text in texts:
            text_cleaned = self.clean_text(text=text)
            texts_cleaned.append(text_cleaned)

        return texts_cleaned

    def clean_text(self, text: str) -> str:
        """
        clean the given text

        Parameters
        ------------
        text : str
            a discord message text

        Returns
        ---------
        text_cleaned : str
            the cleaned text
            (discord ids removed)
        """
        text_cleaned = self.remove_ids(text=text)
        text_cleaned = self.extract_main_content(text=text_cleaned)

        return text_cleaned

    def remove_ids(self, text: str) -> str:
        """
        remove the ids that are available in texts
        user ids would be with the format of <@number>
        and the role ids are in the format of <@&number>

        Parameters
        ------------
        text : str
            a discord message text

        Returns
        ---------
        cleaned_text : str
            the texts with removed <@number> and <@&number>
        """
        pattern = r"<@&?\d+>"

        # Removing matches
        cleaned_text = re.sub(pattern, "", text)

        cleaned_text = " ".join(cleaned_text.split())
        return cleaned_text
