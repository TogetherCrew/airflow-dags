import logging

from violation_detection_helpers.utils.mailgun import MailGun


class SendReportEmail:
    def __init__(self, platform_id: str) -> None:
        # using codes that was written for something else
        self.platform_id = platform_id
        self.mailgun = MailGun()

    def send(self, email: str, message: str) -> None:
        """
        send discord user a message

        Parameters
        ------------
        email : str
            a valid email address
        message : str
            the message to DM user
        """
        log_prefix = f"PLATFORM_ID: {self.platform_id} "
        try:
            logging.info(f"{log_prefix}Sending email for user email {email}!")
            self.mailgun.send_single_email(
                to_address=email,
                subject="TogetherCrew Violation Detection Report",
                message=message,
            )
        except Exception as exp:
            logging.error(
                f"{log_prefix}Error while reporting to discord user_id: "
                f"{email}. Exception: {exp}"
            )
