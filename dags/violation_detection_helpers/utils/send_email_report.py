import logging

from violation_detection_helpers.utils.mailgun import MailGun


class SendReportEmail:
    def __init__(self, platform_id: str) -> None:
        self.platform_id = platform_id
        self.mailgun = MailGun()

    def send(self, email: str, message: str) -> None:
        """
        send an email to a user

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
                f"{log_prefix}Error while sending email to: "
                f"{email}. Exception: {exp}"
            )
