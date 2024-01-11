from logging import Handler
from slack_utils.main import send_text_to_slack


class SlackHandler(Handler):
    """
    - Handler for sending messages to slack
    """

    def __init__(self):
        super().__init__()

    def emit(self, record):
        log_entry = self.format(record)
        self.send_to_slack(log_entry)

    def send_to_slack(self, message):
        """
        - sends message to slack
        """
        send_text_to_slack(message)
