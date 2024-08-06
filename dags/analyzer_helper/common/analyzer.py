import asyncio
import logging
from datetime import datetime

from tc_analyzer_lib.schemas.platform_configs import TelegramAnalyzerConfig
from tc_analyzer_lib.schemas.platform_configs.config_base import PlatformConfigBase
from tc_analyzer_lib.tc_analyzer import TCAnalyzer


class Analyzer:
    def analyze(
        self,
        platform_id: str,
        resources: list[str],
        period: datetime,
        action: dict[str, int],
        window: dict[str, int],
        recompute: bool,
        config: PlatformConfigBase = TelegramAnalyzerConfig(),
    ) -> None:
        prefix = f"PLATFORMID: {platform_id} "
        logging.info(f"{prefix} Starting Analyzer job!")
        analyzer = TCAnalyzer(
            platform_id=platform_id,
            resources=resources,
            period=period,
            action=action,
            window=window,
            analyzer_config=config,
        )
        if recompute:
            logging.info(f"{prefix} recomputing analyzer!")
            asyncio.run(analyzer.recompute())
        else:
            logging.info(f"{prefix} append analytics to previous analytics results!")
            asyncio.run(analyzer.run_once())