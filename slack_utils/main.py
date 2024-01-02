import io
import requests
import json
import typing
import functools
import os

from traceback import format_exc
from slack_sdk import WebClient


SLACK_CLIENT = WebClient(os.environ.get("SLACK_BOT_TOKEN"))
DEFAULT_SLACK_CHANNEL = "#python-logs"


def print_and_skip_on_exception(func: typing.Callable) -> typing.Callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> typing.Any:
        try:
            func(*args, **kwargs)
        except Exception as e:
            print(format_exc())
            return

    return wrapper


@print_and_skip_on_exception
def send_text_to_slack(
    text_message: str, channel_name: str = DEFAULT_SLACK_CHANNEL
) -> None:
    response = SLACK_CLIENT.chat_postMessage(channel=channel_name, text=text_message)
    if not response["ok"]:
        print("Message sending failed.")


@print_and_skip_on_exception
def send_image_to_slack(
    image_buffer: io.BytesIO, message: str, channel_name: str = DEFAULT_SLACK_CHANNEL
) -> None:
    response = SLACK_CLIENT.files_upload(
        channels=channel_name,
        initial_comment=message,
        file=image_buffer,
        filename="test.png",
        title="test image",
    )
    if response.status_code != 200:
        print(f"Failed to send the message: {response.status_code}, {response.text}")
