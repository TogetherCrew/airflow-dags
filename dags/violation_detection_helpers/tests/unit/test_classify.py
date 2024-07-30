import unittest
from unittest.mock import MagicMock, patch
from violation_detection_helpers.utils.classify import Classifier


class TestClassifier(unittest.TestCase):
    @patch("violation_detection_helpers.utils.classify.OpenAI")
    def test_classify_none(self, MockOpenAI):
        mock_client = MockOpenAI.return_value
        mock_client.chat.completions.create.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content="None"))]
        )

        classifier = Classifier()
        text = "This is a harmless message."
        result = classifier.classify(text)

        self.assertEqual(result, "None")

    @patch("violation_detection_helpers.utils.classify.OpenAI")
    def test_classify_discriminating(self, MockOpenAI):
        mock_client = MockOpenAI.return_value
        mock_client.chat.completions.create.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content="Discriminating"))]
        )

        classifier = Classifier()
        text = "This message is discriminating."
        result = classifier.classify(text)

        self.assertEqual(result, "Discriminating")

    @patch("violation_detection_helpers.utils.classify.OpenAI")
    def test_classify_identifying(self, MockOpenAI):
        mock_client = MockOpenAI.return_value
        mock_client.chat.completions.create.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content="Identifying"))]
        )

        classifier = Classifier()
        text = "This message contains identifying information."
        result = classifier.classify(text)

        self.assertEqual(result, "Identifying")

    @patch("violation_detection_helpers.utils.classify.OpenAI")
    def test_classify_sexualized(self, MockOpenAI):
        mock_client = MockOpenAI.return_value
        mock_client.chat.completions.create.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content="Sexualized"))]
        )

        classifier = Classifier()
        text = "This message is sexualized."
        result = classifier.classify(text)

        self.assertEqual(result, "Sexualized")

    @patch("violation_detection_helpers.utils.classify.OpenAI")
    def test_classify_toxic(self, MockOpenAI):
        mock_client = MockOpenAI.return_value
        mock_client.chat.completions.create.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content="Toxic"))]
        )

        classifier = Classifier()
        text = "This message is toxic."
        result = classifier.classify(text)

        self.assertEqual(result, "Toxic")
