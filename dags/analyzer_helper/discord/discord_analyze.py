from datetime import datetime
from typing import Dict, List


class Analyzer:
    # TODO: Still needs to be done
    def analyze(
        self,
        platform_id: str,
        channels: List[str],
        period: datetime,
        action: Dict[str, int],
        window: Dict[str, int]
    ) -> None:
        raise NotImplementedError("Analyzer is not implemented yet")
