from unittest import TestCase
from datetime import datetime

from unittest.mock import patch, MagicMock
from violation_detection_helpers import ExtractPlatformRawData


class TestExtractRawData(TestCase):
    @patch("hivemind_etl_helpers.src.utils.mongo.MongoSingleton.get_instance")
    def test_extract(self, mock_get_instance):
        mock_client = MagicMock()

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

        mock_get_instance.return_value.get_client.return_value = mock_client

        platform_id = "5151515151"
        mock_client[platform_id]["rawmemberactivities"].find.return_value = iter(
            sample_data
        )

        extract_data = ExtractPlatformRawData(platform_id)
        results = extract_data.extract(
            from_date=datetime(2020, 1, 1),
            to_date=None,
            resources=["12", "13", "14"],
            recompute=False,
        )
        print("Results:", results)

        self.assertEqual(results, sample_data)
