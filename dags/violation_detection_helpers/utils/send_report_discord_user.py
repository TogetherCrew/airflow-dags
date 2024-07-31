import logging

from tc_analyzer_lib.automation.automation_workflow import AutomationWorkflow


class SendReportDiscordUser:
    def __init__(self, platform_id: str) -> None:
        # using codes that was written for something else
        self.automation = AutomationWorkflow()
        self.platform_id = platform_id

    def send(self, discord_user_id: str, message: str) -> None:
        """
        send discord user a message

        Parameters
        ------------
        discord_user_id : str
            user id for discord
        message : str
            the message to DM user
        """
        log_prefix = f"PLATFORM_ID: {self.platform_id} "
        try:
            data = self.automation._prepare_saga_data(
                platform_id=self.platform_id,
                user_id=discord_user_id,
                message=message,
            )

            saga_id = self.automation._create_manual_saga(data)
            logging.info(
                f"{log_prefix}Started to fire events for discord user {discord_user_id}!"
            )
            self.automation.fire_event(saga_id, data)
        except Exception as exp:
            logging.error(
                f"{log_prefix}Error while reporting to discord user_id: "
                f"{discord_user_id}. Exception: {exp}"
            )
