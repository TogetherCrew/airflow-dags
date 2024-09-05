import asyncio
import logging
from datetime import datetime, timedelta

from tc_analyzer_lib.publish_on_success import publish_on_success
from tc_analyzer_lib.schemas.platform_configs import DiscordAnalyzerConfig
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
        config: PlatformConfigBase = DiscordAnalyzerConfig(),
    ) -> None:
        prefix = f"PLATFORMID: {platform_id} "
        logging.info(f"{prefix} Starting Analyzer job!")

        # limiting to 90 days
        if (datetime.now() - period).days > 90:
            period = (datetime.now() - timedelta(days=90)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

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

        # this will just support discord platform
        publish_on_success(platform_id, recompute)
