from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pytest
from violation_detection_helpers import TransformPlatformRawData


@pytest.mark.skip(
    "Skipping since mocking don't work fow now!"
    "There's an OpenAI object in Classify __init__ which doesn't work."
)
class TestTransformPlatformData(TestCase):
    @patch("violation_detection_helpers.utils.classify.Classifier")
    def test_transform_no_data(self, classifier):
        classifier.return_value = MagicMock()
        mock_classify = MagicMock()
        classifier.classify = mock_classify
        classifier.classify.return_value = "identifying"

        transformer = TransformPlatformRawData()

        transformed_data = transformer.transform(raw_data=[])
        self.assertEqual(transformed_data, [])

    @patch("violation_detection_helpers.utils.classify.Classifier")
    def test_transform_single_document(self, classifier):
        classifier.return_value = MagicMock()

        mock_classify = MagicMock()
        classifier.classify = mock_classify
        mock_classify.return_value = "identifying"

        sample_data = iter(
            [
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
        )
        transformer = TransformPlatformRawData()
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

    @patch("violation_detection_helpers.utils.classify.Classifier")
    def test_transform_multiple_documents(self, classifier):
        classifier.return_value = MagicMock()
        mock_classify = MagicMock()
        classifier.classify = mock_classify
        mock_classify.return_value = "identifying, sexualized"

        transformer = TransformPlatformRawData()

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
