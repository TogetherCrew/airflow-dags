import logging
import os

import requests
from dotenv import load_dotenv


class MailGun:
    def __init__(self) -> None:
        load_dotenv()

    def send_single_email(self, to_address: str, subject: str, message: str):
        """
        send a single email using mailgun service

        Parameters
        ------------
        to_address : str
            the address to send email to
        subject : str
            the email subject
        message : str
            the email message
        """
        try:
            envs = self._load_envs()

            resp = requests.post(
                envs["api_url"],
                auth=("api", envs["api_key"]),
                data={
                    "from": envs["from_email"],
                    "to": to_address,
                    "subject": subject,
                    "text": message,
                },
            )
            if resp.status_code == 200:
                logging.info(
                    f"Successfully sent an email to '{to_address}' via Mailgun API."
                )
            else:
                logging.error(
                    f"Could not send the email, status code: {resp.status_code}, reason: {resp.text}"
                )

        except Exception as ex:
            logging.exception(f"Mailgun error: {ex}")

    def _load_envs(self) -> dict[str, str]:
        """
        load the env variables for mailgun

        Returns
        ---------
        envs : dict[str, str]
            the keys would be as follows
            `api_key`, `api_url`, `from_email`
        """
        envs: dict[str, str] = {
            "api_key": os.getenv("MAILGUN_API_KEY"),
            "api_url": os.getenv("MAILGUN_API_URL"),
            "from_email": os.getenv("FROM_EMAIL_ADDRESS"),
        }

        # for checking the data
        missing_envs = [key for key, value in envs.items() if value is None]

        if missing_envs:
            raise AttributeError(f"Mailgun Missing env variables: {missing_envs}!")

        return envs
