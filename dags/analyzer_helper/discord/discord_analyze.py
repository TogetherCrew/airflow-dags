import logging
from datetime import datetime

from tc_analyzer_lib.tc_analyzer import TCAnalyzer
from tc_analyzer_lib.schemas.platform_configs import DiscordAnalyzerConfig


class Analyzer:
    def analyze(
        self,
        platform_id: str,
        channels: list[str],
        period: datetime,
        action: dict[str, int],
        window: dict[str, int],
        recompute: bool,
    ) -> None:
        prefix = f"PLATFORMID: {platform_id} "
        logging.info(f"{prefix} Starting Analyzer job!")
        analyzer = TCAnalyzer(
            platform_id=platform_id,
            resources=channels,
            period=period,
            action=action,
            window=window,
            analyzer_config=DiscordAnalyzerConfig(),
        )
        if recompute:
            logging.info(f"{prefix} recomputing analyzer!")
            analyzer.recompute()
        else:
            # will append to previous analyzer results
            logging.info(f"{prefix} computing periodically!")
            analyzer.run_once()
