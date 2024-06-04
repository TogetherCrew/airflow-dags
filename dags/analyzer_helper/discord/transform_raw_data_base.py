from datetime import datetime


class TransformRawDataBase:
    def transform(self, raw_data: list, platform_id: str, period: datetime) -> list[dict]:
        """
        Transform raw data to the given structure
        """
        pass
