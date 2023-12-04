import json
import sys
import traceback
import typing as t
from datetime import datetime
from enum import Enum
from textwrap import dedent, indent

import requests

SLACK_MAX_TEXT_LENGTH = 3000
SLACK_MAX_ALERT_PREVIEW_BLOCKS = 5
SLACK_MAX_ATTACHMENTS_BLOCKS = 50
CONTINUATION_SYMBOL = "..."


TSlackBlock = t.Dict[str, t.Any]


class TSlackBlocks(t.TypedDict):
    blocks: t.List[TSlackBlock]


class TSlackMessage(TSlackBlocks, t.TypedDict):
    attachments: t.List[TSlackBlocks]


class SlackMessageComposer:
    """Builds Slack message with primary and secondary blocks"""

    def __init__(self, initial_message: t.Optional[TSlackMessage] = None) -> None:
        """Initialize the Slack message builder"""
        self.slack_message = initial_message or {
            "blocks": [],
            "attachments": [{"blocks": []}],
        }

    def add_primary_blocks(self, *blocks: TSlackBlock) -> "SlackMessageComposer":
        """Add blocks to the message. Blocks are always displayed"""
        self.slack_message["blocks"].extend(blocks)
        return self

    def add_secondary_blocks(self, *blocks: TSlackBlock) -> "SlackMessageComposer":
        """Add attachments to the message

        Attachments are hidden behind "show more" button. The first 5 attachments
        are always displayed. NOTICE: attachments blocks are deprecated by Slack
        """
        self.slack_message["attachments"][0]["blocks"].extend(blocks)
        if (
            len(self.slack_message["attachments"][0]["blocks"])
            >= SLACK_MAX_ATTACHMENTS_BLOCKS
        ):
            raise ValueError("Too many attachments")
        return self

    def _introspect(self) -> "SlackMessageComposer":
        """Print the message to stdout

        This is a debugging method. Useful during composition of the message."""
        print(json.dumps(self.slack_message, indent=2))
        return self


def normalize_message(message: t.Union[str, t.List[str], t.Iterable[str]]) -> str:
    """Normalize message to fit Slack's max text length"""
    if isinstance(message, (list, tuple, set)):
        message = stringify_list(list(message))
    assert isinstance(message, str), f"Message must be a string, got {type(message)}"
    dedented_message = dedent(message)
    if len(dedented_message) < SLACK_MAX_TEXT_LENGTH:
        return dedent(dedented_message)
    return dedent(
        dedented_message[: SLACK_MAX_TEXT_LENGTH - len(CONTINUATION_SYMBOL) - 3]
        + CONTINUATION_SYMBOL
        + dedented_message[-3:]
    )


def divider_block() -> dict:
    """Create a divider block"""
    return {"type": "divider"}


def fields_section_block(*messages: str) -> dict:
    """Create a section block with multiple fields"""
    return {
        "type": "section",
        **{
            "fields": {
                "type": "mrkdwn",
                "text": normalize_message(message),
            }
            for message in messages
        },
    }


def text_section_block(message: str) -> dict:
    """Create a section block with text"""
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": normalize_message(message),
        },
    }


def empty_section_block() -> dict:
    """Create an empty section block"""
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": normalize_message("\t"),
        },
    }


def context_block(*messages: str) -> dict:
    """Create a context block with multiple fields"""
    return {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": normalize_message(message),
            }
            for message in messages
        ],
    }


def header_block(message: str) -> dict:
    """Create a header block"""
    return {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": message,
        },
    }


def button_action_block(text: str, url: str) -> dict:
    """Create a button action block"""
    return {
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": text, "emoji": True},
                "value": text,
                "url": url,
            }
        ],
    }


def compacted_sections_blocks(*messages: t.Union[str, t.Iterable[str]]) -> t.List[dict]:
    """Create a list of compacted sections blocks"""
    return [
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": normalize_message(message),
                }
                for message in messages[i : i + 2]
            ],
        }
        for i in range(0, len(messages), 2)
    ]


class SlackAlertIcon(str, Enum):
    """Enum for status of the alert"""

    # simple statuses
    OK = ":large_green_circle:"
    WARN = ":warning:"
    ERROR = ":x:"
    START = ":arrow_forward:"
    ALERT = ":rotating_light:"
    STOP = ":stop_button:"

    # log levels
    UNKNOWN = ":question:"
    INFO = ":information_source:"
    DEBUG = ":beetle:"
    CRITICAL = ":fire:"
    FATAL = ":skull_and_crossbones:"
    EXCEPTION = ":boom:"

    # test statuses
    FAILURE = ":no_entry_sign:"
    SUCCESS = ":white_check_mark:"
    WARNING = ":warning:"
    SKIPPED = ":fast_forward:"
    PASSED = ":white_check_mark:"

    def __str__(self) -> str:
        return self.value


def stringify_list(list_variation: t.Union[t.List[str], str]) -> str:
    """Prettify and deduplicate list of strings converting it to a newline delimited string"""
    if isinstance(list_variation, str):
        return list_variation
    if len(list_variation) == 1:
        return list_variation[0]
    list_variation = list(list_variation)
    for i, item in enumerate(list_variation):
        if not isinstance(item, str):
            list_variation[i] = str(item)
    order = {item: i for i, item in reversed(list(enumerate(list_variation)))}
    return "\n".join(sorted(set(list_variation), key=lambda item: order[item]))


def send_basic_slack_message(
    incoming_hook: str, message: str, is_markdown: bool = True
) -> None:
    """Sends a `message` to  Slack `incoming_hook`, by default formatted as markdown."""
    resp = requests.post(
        incoming_hook,
        data=json.dumps({"text": message, "mrkdwn": is_markdown}).encode("utf-8"),
        headers={"Content-Type": "application/json;charset=utf-8"},
    )
    resp.raise_for_status()


def send_extract_start_slack_message(
    incoming_hook: str,
    source: str,
    run_id: str,
    tags: t.List[str],
    owners: t.List[str],
    environment: str,
    resources_selected: t.List[str],
    resources_count: int,
) -> None:
    """Sends a Slack message for the start of an extract"""
    resp = requests.post(
        incoming_hook,
        json=SlackMessageComposer()
        .add_primary_blocks(
            header_block(f"{SlackAlertIcon.START} Starting Extract (id: {run_id})"),
            context_block(
                "*Source:* {source}     |".format(source=source),
                "*Status:* Starting Extraction     |",
                "*{date}*".format(date=datetime.utcnow().strftime("%x %X")),
            ),
            divider_block(),
            *compacted_sections_blocks(
                ("*Tags*", stringify_list(tags)),
                ("*Owners*", stringify_list(owners)),
            ),
            *compacted_sections_blocks(
                ("*Environment*", environment),
                (
                    "*Resources*",
                    f"{len(resources_selected)}/{resources_count} selected",
                ),
            ),
            divider_block(),
            text_section_block(
                f"""
Resources selected for extraction :test_tube:

{stringify_list(resources_selected)}
"""
            ),
            button_action_block("View in Harness", url="https://app.harness.io"),
            context_block(f"*Python Version:* {sys.version}"),
        )
        .slack_message,
    )
    resp.raise_for_status()


def send_extract_failure_message(
    incoming_hook: str, source: str, run_id: str, duration: float, error: Exception
) -> None:
    """Sends a Slack message for the failure of an extract"""
    # trace = "\n".join(f"> {line}" for line in traceback.format_exc().splitlines())
    resp = requests.post(
        incoming_hook,
        json=SlackMessageComposer()
        .add_primary_blocks(
            header_block(f"{SlackAlertIcon.ERROR} Extract Failed (id: {run_id})"),
            context_block(
                "*Source:* {source}     |".format(source=source),
                "*Status:* Extraction Failed     |",
                "*{date}*".format(date=datetime.utcnow().strftime("%x %X")),
            ),
            divider_block(),
            text_section_block(
                f"""
Extract failed after {duration:.2f}s :fire:

```
{indent(traceback.format_exc(), " " * 12, lambda line: (not line.startswith("Traceback")))}
```

Please check the logs for more information.
"""
            ),
            button_action_block("View in Harness", url="https://app.harness.io"),
            context_block(f"*Python Version:* {sys.version}"),
        )
        .slack_message,
    )
    resp.raise_for_status()


def send_extract_success_message(
    incoming_hook: str, source: str, run_id: str, duration: float
) -> None:
    """Sends a Slack message for the success of an extract"""
    resp = requests.post(
        incoming_hook,
        json=SlackMessageComposer()
        .add_primary_blocks(
            header_block(f"{SlackAlertIcon.OK} Extract Succeeded (id: {run_id})"),
            context_block(
                "*Source:* {source}     |".format(source=source),
                "*Status:* Extraction Succeeded     |",
                "*{date}*".format(date=datetime.utcnow().strftime("%x %X")),
            ),
            divider_block(),
            text_section_block(
                f"""
Extract succeeded after {duration:.2f}s :tada:

Please check the logs for more information.
"""
            ),
            button_action_block("View in Harness", url="https://app.harness.io"),
            context_block(f"*Python Version:* {sys.version}"),
        )
        .slack_message,
    )
    resp.raise_for_status()


def send_normalize_start_slack_message(
    incoming_hook: str,
    source: str,
    blob_name: str,
    run_id: str,
    environment: str,
) -> None:
    """Sends a Slack message for the start of an extract"""
    _ = environment
    resp = requests.post(
        incoming_hook,
        json=SlackMessageComposer()
        .add_primary_blocks(
            header_block(f"{SlackAlertIcon.START} Normalizing (id: {run_id})"),
            context_block(
                "*Source:* {source}     |".format(source=source),
                "*Status:* Starting Normalization     |",
                "*{date}*".format(date=datetime.utcnow().strftime("%x %X")),
            ),
            divider_block(),
            text_section_block(
                f"""
Pending load package discovered in stage :package:

Starting normalization for: :file_folder:

`{blob_name}` 
"""
            ),
            context_block(f"*Python Version:* {sys.version}"),
        )
        .slack_message,
    )
    resp.raise_for_status()


def send_normalize_failure_message(
    incoming_hook: str,
    source: str,
    blob_name: str,
    run_id: str,
    duration: float,
    error: Exception,
) -> None:
    """Sends a Slack message for the failure of an normalization"""
    # trace = "\n".join(f"> {line}" for line in traceback.format_exc().splitlines())
    resp = requests.post(
        incoming_hook,
        json=SlackMessageComposer()
        .add_primary_blocks(
            header_block(f"{SlackAlertIcon.ERROR} Normalization Failed (id: {run_id})"),
            context_block(
                "*Source:* {source}     |".format(source=source),
                "*Status:* Normalization Failed     |",
                "*{date}*".format(date=datetime.utcnow().strftime("%x %X")),
            ),
            divider_block(),
            text_section_block(
                f"""
Normalization failed after {duration:.2f}s :fire:

```
{indent(traceback.format_exc(), " " * 12, lambda line: (not line.startswith("Traceback")))}
```

Please check the pod logs for more information.
"""
            ),
            context_block(f"*Python Version:* {sys.version}"),
        )
        .slack_message,
    )
    resp.raise_for_status()


def send_normalization_success_message(
    incoming_hook: str, source: str, blob_name: str, run_id: str, duration: float
) -> None:
    """Sends a Slack message for the success of an normalization"""
    resp = requests.post(
        incoming_hook,
        json=SlackMessageComposer()
        .add_primary_blocks(
            header_block(f"{SlackAlertIcon.OK} Normalization Succeeded (id: {run_id})"),
            context_block(
                "*Source:* {source}     |".format(source=source),
                "*Status:* Normalization Succeeded     |",
                "*{date}*".format(date=datetime.utcnow().strftime("%x %X")),
            ),
            divider_block(),
            text_section_block(
                f"""
Normalization took {duration:.2f}s :tada:

The package was normalized successfully: :file_folder:

`{blob_name}`

This package is now prepared for loading.
"""
            ),
        )
        .slack_message,
    )
    resp.raise_for_status()


def send_load_start_slack_message(
    incoming_hook: str,
    source: str,
    destination: str,
    dataset: str,
    run_id: str,
) -> None:
    """Sends a Slack message for the start of a load"""
    resp = requests.post(
        incoming_hook,
        json=SlackMessageComposer()
        .add_primary_blocks(
            header_block(f"{SlackAlertIcon.START} Loading (id: {run_id})"),
            context_block(
                "*Source:* {source}     |".format(source=source),
                "*Status:* Starting Load     |",
                "*{date}*".format(date=datetime.utcnow().strftime("%x %X")),
            ),
            divider_block(),
            *compacted_sections_blocks(
                ("*Destination*", destination),
                ("*Dataset*", dataset),
            ),
            context_block(f"*Python Version:* {sys.version}"),
        )
        .slack_message,
    )
    resp.raise_for_status()


def send_load_failure_message(
    incoming_hook: str,
    source: str,
    destination: str,
    dataset: str,
    run_id: str,
) -> None:
    """Sends a Slack message for the failure of an load"""
    # trace = "\n".join(f"> {line}" for line in traceback.format_exc().splitlines())
    resp = requests.post(
        incoming_hook,
        json=SlackMessageComposer()
        .add_primary_blocks(
            header_block(f"{SlackAlertIcon.ERROR} Load Failed (id: {run_id})"),
            context_block(
                "*Source:* {source}     |".format(source=source),
                "*Status:* Normalization Failed     |",
                "*{date}*".format(date=datetime.utcnow().strftime("%x %X")),
            ),
            divider_block(),
            text_section_block(
                f"""
Load to {destination} dataset named {dataset} failed :fire:

```
{indent(traceback.format_exc(), " " * 12, lambda line: (not line.startswith("Traceback")))}
```

Please check the pod logs for more information.
"""
            ),
            context_block(f"*Python Version:* {sys.version}"),
        )
        .slack_message,
    )
    resp.raise_for_status()


def send_load_success_message(
    incoming_hook: str,
    source: str,
    destination: str,
    dataset: str,
    run_id: str,
    payload: str,
) -> None:
    """Sends a Slack message for the success of an normalization"""
    resp = requests.post(
        incoming_hook,
        json=SlackMessageComposer()
        .add_primary_blocks(
            header_block(f"{SlackAlertIcon.OK} Load Succeeded (id: {run_id})"),
            context_block(
                "*Source:* {source}     |".format(source=source),
                "*Status:* Loading Succeeded     |",
                "*{date}*".format(date=datetime.utcnow().strftime("%x %X")),
            ),
            divider_block(),
            *compacted_sections_blocks(
                ("*Destination*", destination),
                ("*Dataset*", dataset),
            ),
            divider_block(),
            text_section_block(
                f"""
The package was loaded successfully: :file_folder:

```
{payload}
```
"""
            ),
            context_block(f"*Python Version:* {sys.version}"),
        )
        .slack_message,
    )
    resp.raise_for_status()
