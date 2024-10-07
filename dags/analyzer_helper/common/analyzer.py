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
        send_completed_message: bool = True,
    ) -> None:
        """
        analyze any platform data

        Parameters
        ------------
        platform_id : str
            the platform id to analyzer their data
        resources : list[str]
            a list of resource id to analyze thir data
            could be id of channels for discord
            or categories for discourse
        period : datetime
            the period of analytics
            if bigger than 90 days we would limit it for now to 90
        action : dict[str, int]
            analyzer categories parameters
        window: dict[str, int]
            analyzer timewindow parameters
        recompute : bool
            to compute analytics for whole data of platform
            or continue where analytics was left
        config : PlatformConfigBase
            the config for analyzer to handle platform data
        send_completed_message : bool
            If True, will send the analytics completed message to community manager
            if False, nothing will be sent
        """
        prefix = f"PLATFORMID: {platform_id} "
        logging.info(f"{prefix} Starting Analyzer job!")

        # limiting to 90 days
        if (datetime.now() - period).days > 90:
            logging.info(f"{prefix}Limiting Analytics to 90 days period!")
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

        if send_completed_message:
            # this will just support discord platform
            publish_on_success(platform_id, recompute)
