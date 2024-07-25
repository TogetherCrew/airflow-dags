import os
from datetime import datetime
from unittest import TestCase

from unittest.mock import patch, MagicMock
from violation_detection_helpers import TransformPlatformRawData


class TestTransformPlatformData(TestCase):
    def setUp(self) -> None:
        os.environ["OPENAI_API_KEY"] = "sk-asdd2222"

    @patch("openai.OpenAI")
    def test_transform_no_data(self, mock_openai):
        transformer = TransformPlatformRawData()
        mock_openai.return_value = MagicMock()

        transformed_data = transformer.transform(raw_data=[])
        self.assertEqual(transformed_data, [])

    @patch("openai.OpenAI")
    def test_transform_single_document(self, mock_openai):
        transformer = TransformPlatformRawData()
        mock_openai.return_value = MagicMock()

        mock_openai.chat.completions.create.choices[0].message.content.return_value = (
            "identifying"
        )

        sample_data = [
            {
                "author_id": "1",
                "date": datetime(2022, 1, 1),
                "source_id": "8888",
                "text": "some text message",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            }
        ]
        transformed_data = transformer.transform(raw_data=sample_data)

        self.assertEqual(
            transformed_data,
            {
                "author_id": "1",
                "date": datetime(2022, 1, 1),
                "source_id": "8888",
                "text": "some text message",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                    "vdLabel": "identifying",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
        )

    @patch("openai.OpenAI")
    def test_transform_multiple_documents(self, mock_openai):
        transformer = TransformPlatformRawData()
        mock_openai.return_value = MagicMock()

        mock_openai.chat.completions.create.choices[0].message.content.return_value = (
            "identifying, sexualized"
        )

        sample_data = [
            {
                "author_id": "1",
                "date": datetime(2022, 1, 1),
                "source_id": "8888",
                "text": "some text message",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": "2",
                "date": datetime(2022, 1, 2),
                "source_id": "8889",
                "text": "some text message 2",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": "3",
                "date": datetime(2022, 1, 3),
                "source_id": "8880",
                "text": "some text message 3",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
        ]
        transformed_data = transformer.transform(raw_data=sample_data)

        self.assertEqual(
            transformed_data,
            [
                {
                    "author_id": "1",
                    "date": datetime(2022, 1, 1),
                    "source_id": "8888",
                    "text": "some text message",
                    "metadata": {
                        "topic_id": None,
                        "category_id": "34567",
                        "vdLabel": "identifying, sexualized",
                    },
                    "actions": [
                        {
                            "name": "message",
                            "type": "emitter",
                        }
                    ],
                },
                {
                    "author_id": "2",
                    "date": datetime(2022, 1, 2),
                    "source_id": "8889",
                    "text": "some text message 2",
                    "metadata": {
                        "topic_id": None,
                        "category_id": "34567",
                        "vdLabel": "identifying, sexualized",
                    },
                    "actions": [
                        {
                            "name": "message",
                            "type": "emitter",
                        }
                    ],
                },
                {
                    "author_id": "3",
                    "date": datetime(2022, 1, 3),
                    "source_id": "8880",
                    "text": "some text message 3",
                    "metadata": {
                        "topic_id": None,
                        "category_id": "34567",
                        "vdLabel": "identifying, sexualized",
                    },
                    "actions": [
                        {
                            "name": "message",
                            "type": "emitter",
                        }
                    ],
                },
            ],
        )
