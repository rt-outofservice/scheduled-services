#!/usr/bin/env python3
"""Slack summary service — dump channels via slackdump, summarize with AI, send via Telegram.

Usage:
    uv run python services/slack-summary/slack_summary.py [--tests]

Config (config.yaml):
    hostname: myhost
    channels:
      - C01ABCDEF
      - D04GHIJKL
    timeframe: "14h"
    user_id: U08L2JF6RB5
"""

import json
import logging
import re
import shutil
import subprocess
import sys
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

# Add common helpers to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common" / "helpers"))
from ai import AIError, call_ai
from log import setup_logging
from telegram import send_telegram

DUMP_DIR = Path("/tmp/slack-summary")
AUTH_TEST_DIR = Path("/tmp/slack-auth-test")
AUTH_RETRY_INTERVAL = 60  # seconds
AUTH_MAX_RETRIES = 10
CHANNEL_DUMP_DELAY = 5  # seconds between channel dumps
USER_RESOLVE_DELAY = 1  # seconds between user resolution API calls


def load_config(config_path: Path) -> dict:
    """Load and validate config.yaml."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    if not config or "channels" not in config:
        raise ValueError("Config must contain 'channels' key")
    if not config["channels"]:
        raise ValueError("Config 'channels' list must not be empty")
    if "timeframe" not in config:
        raise ValueError("Config must contain 'timeframe' key")
    return config


def parse_timeframe(timeframe: str) -> datetime:
    """Convert timeframe string (e.g. '14h', '30m', '1d') to absolute UTC timestamp.

    Returns the datetime that is `timeframe` ago from now.
    """
    match = re.match(r"^(\d+)([hmd])$", timeframe.strip())
    if not match:
        raise ValueError(f"Invalid timeframe format: {timeframe!r} (expected e.g. '14h', '30m', '1d')")
    value = int(match.group(1))
    unit = match.group(2)
    now = datetime.now(UTC)
    if unit == "h":
        return now - timedelta(hours=value)
    elif unit == "m":
        return now - timedelta(minutes=value)
    elif unit == "d":
        return now - timedelta(days=value)
    raise ValueError(f"Unknown timeframe unit: {unit}")


def format_timestamp_iso(dt: datetime) -> str:
    """Format datetime as ISO 8601 string for slackdump -time-from."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def validate_slackdump_auth(first_channel: str, logger: logging.Logger) -> bool:
    """Test slackdump auth by doing a zero-result dump.

    Returns True if auth is valid.
    """
    AUTH_TEST_DIR.mkdir(parents=True, exist_ok=True)
    try:
        now_ts = format_timestamp_iso(datetime.now(UTC))
        result = subprocess.run(
            ["slackdump", "dump", "-time-from", now_ts, "-o", str(AUTH_TEST_DIR), first_channel],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0:
            return True
        logger.warning(f"slackdump auth test failed: {result.stderr.strip()}")
        return False
    except subprocess.TimeoutExpired:
        logger.warning("slackdump auth test timed out")
        return False
    except FileNotFoundError:
        logger.error("slackdump binary not found")
        return False
    finally:
        shutil.rmtree(AUTH_TEST_DIR, ignore_errors=True)


def validate_auth_with_retries(
    first_channel: str,
    hostname: str,
    logger: logging.Logger,
    sleep_fn=time.sleep,
) -> bool:
    """Validate slackdump auth, retrying up to AUTH_MAX_RETRIES times.

    Sends telegram notifications on failure and recovery.
    Returns True if auth succeeds, False after exhausting retries.
    """
    if validate_slackdump_auth(first_channel, logger):
        return True

    # First failure — notify user
    send_telegram("slack-summary: slackdump auth expired, please re-auth. Retrying...", hostname=hostname)
    logger.warning("slackdump auth failed, starting retries")

    for attempt in range(1, AUTH_MAX_RETRIES + 1):
        logger.info(f"Auth retry {attempt}/{AUTH_MAX_RETRIES}, waiting {AUTH_RETRY_INTERVAL}s...")
        sleep_fn(AUTH_RETRY_INTERVAL)
        if validate_slackdump_auth(first_channel, logger):
            send_telegram("slack-summary: slackdump auth restored", hostname=hostname)
            logger.info("slackdump auth restored")
            return True

    # Exhausted retries
    send_telegram("slack-summary FATAL: slackdump auth failed after 10 retries", hostname=hostname)
    logger.error("slackdump auth permanently failed")
    return False


def dump_channel(channel_id: str, time_from: str, logger: logging.Logger) -> str | None:
    """Dump a single channel via slackdump.

    Returns the path to the JSON file on success, None on failure.
    """
    try:
        result = subprocess.run(
            ["slackdump", "dump", "-time-from", time_from, "-o", str(DUMP_DIR), channel_id],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            logger.warning(f"slackdump dump failed for {channel_id}: {result.stderr.strip()}")
            return None
        # slackdump creates <channel_id>.json in the output dir
        json_path = DUMP_DIR / f"{channel_id}.json"
        if json_path.exists():
            return str(json_path)
        # Some versions may nest differently — look for any json
        for f in DUMP_DIR.glob(f"{channel_id}*.json"):
            return str(f)
        logger.warning(f"No JSON output found for channel {channel_id}")
        return None
    except subprocess.TimeoutExpired:
        logger.warning(f"slackdump dump timed out for {channel_id}")
        return None
    except FileNotFoundError:
        logger.error("slackdump binary not found")
        return None


def dump_all_channels(
    channels: list[str],
    time_from: str,
    logger: logging.Logger,
    sleep_fn=time.sleep,
) -> tuple[dict[str, str], list[str]]:
    """Dump all channels, sleeping between each.

    Returns (channel_files, warnings) where channel_files maps channel_id to json path.
    """
    DUMP_DIR.mkdir(parents=True, exist_ok=True)
    channel_files: dict[str, str] = {}
    warnings: list[str] = []

    for i, channel_id in enumerate(channels):
        if i > 0:
            sleep_fn(CHANNEL_DUMP_DELAY)
        logger.info(f"Dumping channel {channel_id} ({i + 1}/{len(channels)})")
        json_path = dump_channel(channel_id, time_from, logger)
        if json_path:
            channel_files[channel_id] = json_path
        else:
            warnings.append(f"Failed to dump {channel_id}")

    return channel_files, warnings


BOT_SUBTYPES = {"bot_message", "bot_add", "bot_remove"}


def parse_channel_messages(json_path: str, logger: logging.Logger) -> tuple[dict, list[dict]]:
    """Parse a slackdump channel JSON file.

    Returns (channel_info, messages) where:
    - channel_info has 'channel_id' and 'name'
    - messages is a list of {user, text, ts} dicts
    """
    try:
        with open(json_path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Failed to parse {json_path}: {e}")
        return {}, []

    channel_info = {
        "channel_id": data.get("channel_id", ""),
        "name": data.get("name", ""),
    }

    raw_messages = data.get("messages", [])
    parsed = []
    for msg in raw_messages:
        # Filter: must be type "message" with no bot subtype
        if msg.get("type") != "message":
            continue
        if msg.get("subtype") in BOT_SUBTYPES:
            continue
        text = msg.get("text", "").strip()
        if not text:
            continue
        parsed.append(
            {
                "user": msg.get("user", ""),
                "text": text,
                "ts": msg.get("ts", ""),
            }
        )

    return channel_info, parsed


def get_user_cache_path() -> Path:
    """Return the path to the persistent user cache file."""
    cache_dir = Path.home() / ".scheduled-services" / "services" / "slack-summary"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "user-cache.txt"


def load_user_cache(cache_path: Path) -> dict[str, str]:
    """Load user ID -> display name cache from file.

    Format: USERID=DisplayName per line.
    """
    cache: dict[str, str] = {}
    if not cache_path.exists():
        return cache
    try:
        for line in cache_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if "=" in line:
                uid, name = line.split("=", 1)
                cache[uid.strip()] = name.strip()
    except OSError:
        pass
    return cache


def save_user_cache(cache_path: Path, cache: dict[str, str]) -> None:
    """Save user cache to file."""
    lines = [f"{uid}={name}" for uid, name in sorted(cache.items())]
    cache_path.write_text("\n".join(lines) + "\n" if lines else "", encoding="utf-8")


def resolve_users_via_slackdump(
    user_ids: set[str],
    cache: dict[str, str],
    cache_path: Path,
    logger: logging.Logger,
    sleep_fn=time.sleep,
) -> dict[str, str]:
    """Resolve uncached user IDs via slackdump list users.

    Updates and saves the cache. Returns the full cache (including newly resolved).
    """
    uncached = user_ids - set(cache.keys())
    if not uncached:
        return cache

    # Try bulk resolve via slackdump list users
    try:
        result = subprocess.run(
            ["slackdump", "list", "users"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0:
            # Parse user list output — each line typically: ID  Name  DisplayName  Email...
            for line in result.stdout.splitlines():
                parts = line.strip().split("\t")
                if len(parts) >= 3 and parts[0].startswith("U"):
                    uid = parts[0].strip()
                    display_name = parts[2].strip() or parts[1].strip()
                    if uid in uncached and display_name:
                        # Strip markdown special chars from display name
                        display_name = re.sub(r"[*_`\[]", "", display_name)
                        cache[uid] = display_name
            save_user_cache(cache_path, cache)
            logger.info(f"Resolved {len(uncached) - len(uncached - set(cache.keys()))} users via slackdump list")
        else:
            logger.warning(f"slackdump list users failed: {result.stderr.strip()}")
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        logger.warning(f"slackdump list users error: {e}")

    # For any still-uncached users, use their ID as fallback
    still_uncached = user_ids - set(cache.keys())
    for uid in still_uncached:
        logger.info(f"Could not resolve user {uid}, using ID as fallback")

    return cache


def get_display_name(user_id: str, cache: dict[str, str]) -> str:
    """Get display name for a user ID, falling back to @USER_ID."""
    name = cache.get(user_id, "")
    return f"@{name}" if name else f"@{user_id}"


def detect_mentions(messages: list[dict], user_id: str) -> list[dict]:
    """Find messages that mention the given user_id.

    Returns messages containing <@user_id> pattern.
    """
    if not user_id:
        return []
    pattern = f"<@{user_id}>"
    return [msg for msg in messages if pattern in msg.get("text", "")]


def is_dm_channel(channel_id: str) -> bool:
    """Check if a channel ID is a DM (starts with D)."""
    return channel_id.startswith("D")


def build_ai_prompt(
    channels_data: list[dict],
    user_cache: dict[str, str],
    user_id: str,
    timeframe: str,
) -> str:
    """Build the AI prompt for summary generation.

    channels_data: list of {channel_id, name, messages: [{user, text, ts}]}
    """
    # Replace user IDs in message text with display names
    resolved_data = []
    for ch in channels_data:
        resolved_msgs = []
        for msg in ch["messages"]:
            text = msg["text"]
            # Replace <@USERID> patterns with display names
            text = re.sub(
                r"<@(U[A-Z0-9]+)>",
                lambda m: get_display_name(m.group(1), user_cache),
                text,
            )
            resolved_msgs.append(
                {
                    "author": get_display_name(msg["user"], user_cache),
                    "text": text,
                    "ts": msg["ts"],
                }
            )
        ch_type = "DM" if is_dm_channel(ch["channel_id"]) else "channel"
        resolved_data.append(
            {
                "channel": ch["name"] or ch["channel_id"],
                "type": ch_type,
                "messages": resolved_msgs,
            }
        )

    data_json = json.dumps(resolved_data, ensure_ascii=False, indent=2)

    mentions_section = ""
    if user_id:
        mentions_section = (
            "\nIMPORTANT: If any messages mention or are directed at the user (look for "
            f"{get_display_name(user_id, user_cache)} mentions or DM channels), "
            "include a MENTIONS & DMs section at the very top of the summary.\n"
        )

    return (
        "You are a Slack summary assistant. Generate a concise narrative summary of these Slack messages.\n\n"
        f"Timeframe: last {timeframe}\n"
        f"{mentions_section}"
        "\nRules:\n"
        "- Write 2-5 sentence narrative paragraphs per active channel (NOT bullet lists)\n"
        "- Organize by channel, using channel name as header\n"
        "- Attribute statements to people using @DisplayName\n"
        "- Focus on: technical/operational topics, decisions, incidents, deployments, action items\n"
        "- Skip: casual chat, jokes, food discussions, emoji-only reactions, scheduling\n"
        "- If a channel has no substantive messages, omit it entirely\n"
        "- List channels with no activity at the bottom in italic\n"
        "- Max 3800 characters total\n"
        "- Use Telegram Markdown V1 formatting (bold with *, no other markdown)\n\n"
        f"Messages:\n{data_json}"
    )


def truncate_message(message: str, max_len: int = 4000) -> str:
    """Truncate message from the bottom if it exceeds max_len."""
    if len(message) <= max_len:
        return message
    truncated = message[: max_len - 40]
    last_nl = truncated.rfind("\n")
    if last_nl > max_len // 2:
        truncated = truncated[:last_nl]
    return truncated + "\n\n... (truncated)"


def format_warnings(warnings: list[str]) -> str:
    """Format warnings list into a brief section for the telegram message."""
    if not warnings:
        return ""
    return "\n\nNote: " + "; ".join(warnings)


def run_summary(config_path: Path) -> None:
    """Main summary execution flow."""
    logger, log_file = setup_logging("slack-summary")
    logger.info("Starting slack-summary service")

    # Load config
    try:
        config = load_config(config_path)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        send_telegram(f"slack-summary FATAL: {e}")
        return

    hostname = config.get("hostname", "")
    channels = config["channels"]
    timeframe = config["timeframe"]
    user_id = config.get("user_id", "")

    # Parse timeframe
    try:
        time_from_dt = parse_timeframe(timeframe)
        time_from = format_timestamp_iso(time_from_dt)
    except ValueError as e:
        logger.error(f"Timeframe error: {e}")
        send_telegram(f"slack-summary FATAL: {e}", hostname=hostname)
        return

    logger.info(f"Timeframe: {timeframe} -> from {time_from}")
    logger.info(f"Channels: {channels}")

    # Validate slackdump auth
    if not validate_auth_with_retries(channels[0], hostname, logger):
        return

    # Dump channels
    DUMP_DIR.mkdir(parents=True, exist_ok=True)
    channel_files, warnings = dump_all_channels(channels, time_from, logger)

    if not channel_files:
        logger.error("All channel dumps failed")
        send_telegram("slack-summary FATAL: all channel dumps failed", hostname=hostname)
        shutil.rmtree(DUMP_DIR, ignore_errors=True)
        return

    # Parse messages
    all_user_ids: set[str] = set()
    channels_data: list[dict] = []
    for channel_id, json_path in channel_files.items():
        channel_info, messages = parse_channel_messages(json_path, logger)
        if not messages:
            logger.info(f"No messages in channel {channel_id}")
            continue
        # Collect user IDs for resolution
        for msg in messages:
            if msg["user"]:
                all_user_ids.add(msg["user"])
            # Also collect mentioned user IDs
            for match in re.finditer(r"<@(U[A-Z0-9]+)>", msg["text"]):
                all_user_ids.add(match.group(1))
        channels_data.append(
            {
                "channel_id": channel_id,
                "name": channel_info.get("name", ""),
                "messages": messages,
            }
        )

    if not channels_data:
        logger.info("No messages found in any channel")
        send_telegram("slack-summary: no messages found in the configured timeframe", hostname=hostname)
        # Cleanup
        shutil.rmtree(DUMP_DIR, ignore_errors=True)
        return

    # Resolve user IDs
    cache_path = get_user_cache_path()
    user_cache = load_user_cache(cache_path)
    user_cache = resolve_users_via_slackdump(all_user_ids, user_cache, cache_path, logger)

    # Build AI prompt
    prompt = build_ai_prompt(channels_data, user_cache, user_id, timeframe)

    # Call AI
    try:
        summary_text = call_ai(prompt)
    except AIError as e:
        logger.error(f"AI call failed: {e}")
        send_telegram(f"slack-summary FATAL: AI call failed: {e}", hostname=hostname)
        shutil.rmtree(DUMP_DIR, ignore_errors=True)
        return

    # Append warnings
    summary_text += format_warnings(warnings)

    # Truncate if needed
    summary_text = truncate_message(summary_text, max_len=4000)

    # Send via telegram
    try:
        sent_ok = send_telegram(summary_text, hostname=hostname)
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        sent_ok = False

    if sent_ok:
        logger.info("Slack summary sent successfully")
    else:
        logger.warning("Slack summary telegram delivery failed")

    # Cleanup
    shutil.rmtree(DUMP_DIR, ignore_errors=True)
    logger.info("slack-summary service finished")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Slack summary service")
    parser.add_argument("--tests", action="store_true", help="Run tests and exit")
    args = parser.parse_args()

    if args.tests:
        return

    config_path = Path(__file__).resolve().parent / "config.yaml"
    run_summary(config_path)


# ---------- Tests (run with: uv run python services/slack-summary/slack_summary.py --tests) ----------

_mod = sys.modules[__name__]


class TestLoadConfig(unittest.TestCase):
    def test_valid_config(self):
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(
                {
                    "hostname": "test",
                    "channels": ["C01ABC", "D02DEF"],
                    "timeframe": "14h",
                    "user_id": "U123",
                },
                f,
            )
            f.flush()
            config = load_config(Path(f.name))
        self.assertEqual(config["hostname"], "test")
        self.assertEqual(len(config["channels"]), 2)
        Path(f.name).unlink()

    def test_missing_config(self):
        with self.assertRaises(FileNotFoundError):
            load_config(Path("/nonexistent/config.yaml"))

    def test_missing_channels_key(self):
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"hostname": "test", "timeframe": "14h"}, f)
            f.flush()
            with self.assertRaises(ValueError):
                load_config(Path(f.name))
        Path(f.name).unlink()

    def test_empty_channels(self):
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"hostname": "test", "channels": [], "timeframe": "14h"}, f)
            f.flush()
            with self.assertRaises(ValueError):
                load_config(Path(f.name))
        Path(f.name).unlink()

    def test_missing_timeframe(self):
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"hostname": "test", "channels": ["C01ABC"]}, f)
            f.flush()
            with self.assertRaises(ValueError):
                load_config(Path(f.name))
        Path(f.name).unlink()


class TestParseTimeframe(unittest.TestCase):
    def test_hours(self):
        result = parse_timeframe("14h")
        now = datetime.now(UTC)
        expected = now - timedelta(hours=14)
        self.assertAlmostEqual(result.timestamp(), expected.timestamp(), delta=2)

    def test_minutes(self):
        result = parse_timeframe("30m")
        now = datetime.now(UTC)
        expected = now - timedelta(minutes=30)
        self.assertAlmostEqual(result.timestamp(), expected.timestamp(), delta=2)

    def test_days(self):
        result = parse_timeframe("1d")
        now = datetime.now(UTC)
        expected = now - timedelta(days=1)
        self.assertAlmostEqual(result.timestamp(), expected.timestamp(), delta=2)

    def test_invalid_format(self):
        with self.assertRaises(ValueError):
            parse_timeframe("abc")

    def test_invalid_unit(self):
        with self.assertRaises(ValueError):
            parse_timeframe("14x")

    def test_whitespace_stripped(self):
        result = parse_timeframe("  14h  ")
        now = datetime.now(UTC)
        expected = now - timedelta(hours=14)
        self.assertAlmostEqual(result.timestamp(), expected.timestamp(), delta=2)


class TestFormatTimestampIso(unittest.TestCase):
    def test_format(self):
        dt = datetime(2026, 4, 1, 10, 30, 0, tzinfo=UTC)
        self.assertEqual(format_timestamp_iso(dt), "2026-04-01T10:30:00")


class TestValidateSlackdumpAuth(unittest.TestCase):
    @patch("subprocess.run")
    @patch.object(_mod, "AUTH_TEST_DIR", Path("/tmp/test-slack-auth"))
    def test_success(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        logger = MagicMock(spec=logging.Logger)
        result = validate_slackdump_auth("C01ABC", logger)
        self.assertTrue(result)

    @patch("subprocess.run")
    @patch.object(_mod, "AUTH_TEST_DIR", Path("/tmp/test-slack-auth"))
    def test_failure(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="auth expired")
        logger = MagicMock(spec=logging.Logger)
        result = validate_slackdump_auth("C01ABC", logger)
        self.assertFalse(result)

    @patch("subprocess.run")
    @patch.object(_mod, "AUTH_TEST_DIR", Path("/tmp/test-slack-auth"))
    def test_timeout(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="slackdump", timeout=60)
        logger = MagicMock(spec=logging.Logger)
        result = validate_slackdump_auth("C01ABC", logger)
        self.assertFalse(result)

    @patch("subprocess.run")
    @patch.object(_mod, "AUTH_TEST_DIR", Path("/tmp/test-slack-auth"))
    def test_binary_not_found(self, mock_run):
        mock_run.side_effect = FileNotFoundError()
        logger = MagicMock(spec=logging.Logger)
        result = validate_slackdump_auth("C01ABC", logger)
        self.assertFalse(result)


class TestValidateAuthWithRetries(unittest.TestCase):
    @patch.object(_mod, "send_telegram")
    @patch.object(_mod, "validate_slackdump_auth", return_value=True)
    def test_immediate_success(self, mock_auth, mock_tg):
        logger = MagicMock(spec=logging.Logger)
        result = validate_auth_with_retries("C01ABC", "host", logger)
        self.assertTrue(result)
        mock_tg.assert_not_called()

    @patch.object(_mod, "send_telegram")
    @patch.object(_mod, "validate_slackdump_auth", side_effect=[False, False, True])
    def test_succeeds_after_retries(self, mock_auth, mock_tg):
        logger = MagicMock(spec=logging.Logger)
        result = validate_auth_with_retries("C01ABC", "host", logger, sleep_fn=lambda _: None)
        self.assertTrue(result)
        # Should have sent "expired" and "restored" notifications
        self.assertEqual(mock_tg.call_count, 2)

    @patch.object(_mod, "send_telegram")
    @patch.object(_mod, "validate_slackdump_auth", return_value=False)
    def test_exhausted_retries(self, mock_auth, mock_tg):
        logger = MagicMock(spec=logging.Logger)
        result = validate_auth_with_retries("C01ABC", "host", logger, sleep_fn=lambda _: None)
        self.assertFalse(result)
        # Should have sent "expired" and "FATAL" notifications
        self.assertEqual(mock_tg.call_count, 2)
        fatal_calls = [c for c in mock_tg.call_args_list if "FATAL" in str(c)]
        self.assertEqual(len(fatal_calls), 1)


class TestParseChannelMessages(unittest.TestCase):
    def test_normal_messages(self):
        import tempfile

        data = {
            "channel_id": "C01ABC",
            "name": "general",
            "messages": [
                {"type": "message", "user": "U123", "text": "hello world", "ts": "1774272716.060739"},
                {"type": "message", "user": "U456", "text": "hi there", "ts": "1774272717.060739"},
            ],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            f.flush()
            info, msgs = parse_channel_messages(f.name, MagicMock())
        self.assertEqual(info["channel_id"], "C01ABC")
        self.assertEqual(info["name"], "general")
        self.assertEqual(len(msgs), 2)
        self.assertEqual(msgs[0]["user"], "U123")
        self.assertEqual(msgs[0]["text"], "hello world")
        Path(f.name).unlink()

    def test_filters_bot_messages(self):
        import tempfile

        data = {
            "channel_id": "C01ABC",
            "name": "general",
            "messages": [
                {"type": "message", "subtype": "bot_message", "user": "B001", "text": "bot msg", "ts": "1.0"},
                {"type": "message", "user": "U123", "text": "human msg", "ts": "2.0"},
                {"type": "message", "subtype": "bot_add", "user": "B002", "text": "added", "ts": "3.0"},
            ],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            f.flush()
            _, msgs = parse_channel_messages(f.name, MagicMock())
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0]["text"], "human msg")
        Path(f.name).unlink()

    def test_filters_non_message_types(self):
        import tempfile

        data = {
            "channel_id": "C01ABC",
            "name": "general",
            "messages": [
                {"type": "channel_join", "user": "U123", "text": "joined", "ts": "1.0"},
                {"type": "message", "user": "U123", "text": "real msg", "ts": "2.0"},
            ],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            f.flush()
            _, msgs = parse_channel_messages(f.name, MagicMock())
        self.assertEqual(len(msgs), 1)
        Path(f.name).unlink()

    def test_skips_empty_text(self):
        import tempfile

        data = {
            "channel_id": "C01ABC",
            "name": "general",
            "messages": [
                {"type": "message", "user": "U123", "text": "", "ts": "1.0"},
                {"type": "message", "user": "U456", "text": "   ", "ts": "2.0"},
            ],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            f.flush()
            _, msgs = parse_channel_messages(f.name, MagicMock())
        self.assertEqual(len(msgs), 0)
        Path(f.name).unlink()

    def test_invalid_json(self):
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("not valid json")
            f.flush()
            info, msgs = parse_channel_messages(f.name, MagicMock())
        self.assertEqual(info, {})
        self.assertEqual(msgs, [])
        Path(f.name).unlink()


class TestUserCache(unittest.TestCase):
    def test_load_and_save(self):
        import tempfile

        cache_path = Path(tempfile.mktemp(suffix=".txt"))
        cache = {"U123": "Alice", "U456": "Bob"}
        save_user_cache(cache_path, cache)
        loaded = load_user_cache(cache_path)
        self.assertEqual(loaded, cache)
        cache_path.unlink()

    def test_load_empty(self):
        import tempfile

        cache_path = Path(tempfile.mktemp(suffix=".txt"))
        cache_path.write_text("")
        loaded = load_user_cache(cache_path)
        self.assertEqual(loaded, {})
        cache_path.unlink()

    def test_load_nonexistent(self):
        loaded = load_user_cache(Path("/nonexistent/cache.txt"))
        self.assertEqual(loaded, {})

    def test_load_malformed_lines(self):
        import tempfile

        cache_path = Path(tempfile.mktemp(suffix=".txt"))
        cache_path.write_text("U123=Alice\nbadline\nU456=Bob\n")
        loaded = load_user_cache(cache_path)
        self.assertEqual(len(loaded), 2)
        self.assertEqual(loaded["U123"], "Alice")
        cache_path.unlink()


class TestResolveUsers(unittest.TestCase):
    @patch("subprocess.run")
    def test_resolves_from_slackdump(self, mock_run):
        import tempfile

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="U123\tAlice Smith\tAlice\talice@co.com\nU456\tBob Jones\tBob\tbob@co.com\n",
            stderr="",
        )
        cache_path = Path(tempfile.mktemp(suffix=".txt"))
        cache = {}
        result = resolve_users_via_slackdump(
            {"U123", "U456"},
            cache,
            cache_path,
            MagicMock(),
        )
        self.assertEqual(result["U123"], "Alice")
        self.assertEqual(result["U456"], "Bob")
        cache_path.unlink()

    @patch("subprocess.run")
    def test_skips_cached_users(self, mock_run):
        import tempfile

        cache_path = Path(tempfile.mktemp(suffix=".txt"))
        cache = {"U123": "Alice"}
        result = resolve_users_via_slackdump(
            {"U123"},
            cache,
            cache_path,
            MagicMock(),
        )
        self.assertEqual(result["U123"], "Alice")
        mock_run.assert_not_called()
        cache_path.unlink(missing_ok=True)

    @patch("subprocess.run")
    def test_strips_markdown_chars(self, mock_run):
        import tempfile

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="U123\tAlice *Smith*\tAlice_S\talice@co.com\n",
            stderr="",
        )
        cache_path = Path(tempfile.mktemp(suffix=".txt"))
        result = resolve_users_via_slackdump(
            {"U123"},
            {},
            cache_path,
            MagicMock(),
        )
        self.assertNotIn("*", result.get("U123", ""))
        self.assertNotIn("_", result.get("U123", ""))
        cache_path.unlink()


class TestMentionDetection(unittest.TestCase):
    def test_detects_mention(self):
        messages = [
            {"text": "hey <@U123> can you check this?", "user": "U456", "ts": "1.0"},
            {"text": "sure thing", "user": "U123", "ts": "2.0"},
        ]
        mentions = detect_mentions(messages, "U123")
        self.assertEqual(len(mentions), 1)
        self.assertIn("<@U123>", mentions[0]["text"])

    def test_no_mentions(self):
        messages = [{"text": "just chatting", "user": "U456", "ts": "1.0"}]
        mentions = detect_mentions(messages, "U123")
        self.assertEqual(len(mentions), 0)

    def test_empty_user_id(self):
        messages = [{"text": "<@U123> hello", "user": "U456", "ts": "1.0"}]
        mentions = detect_mentions(messages, "")
        self.assertEqual(len(mentions), 0)


class TestIsDmChannel(unittest.TestCase):
    def test_dm_channel(self):
        self.assertTrue(is_dm_channel("D04GHIJKL"))

    def test_public_channel(self):
        self.assertFalse(is_dm_channel("C01ABCDEF"))

    def test_empty(self):
        self.assertFalse(is_dm_channel(""))


class TestBuildAIPrompt(unittest.TestCase):
    def test_includes_channel_data(self):
        channels_data = [
            {
                "channel_id": "C01ABC",
                "name": "general",
                "messages": [{"user": "U123", "text": "hello", "ts": "1.0"}],
            }
        ]
        prompt = build_ai_prompt(channels_data, {"U123": "Alice"}, "", "14h")
        self.assertIn("general", prompt)
        self.assertIn("@Alice", prompt)
        self.assertIn("14h", prompt)

    def test_mentions_section_when_user_id(self):
        channels_data = [
            {
                "channel_id": "C01ABC",
                "name": "general",
                "messages": [{"user": "U123", "text": "hello", "ts": "1.0"}],
            }
        ]
        prompt = build_ai_prompt(channels_data, {"U789": "Me"}, "U789", "14h")
        self.assertIn("MENTIONS", prompt)
        self.assertIn("@Me", prompt)

    def test_no_mentions_section_without_user_id(self):
        channels_data = [
            {
                "channel_id": "C01ABC",
                "name": "general",
                "messages": [{"user": "U123", "text": "hello", "ts": "1.0"}],
            }
        ]
        prompt = build_ai_prompt(channels_data, {}, "", "14h")
        self.assertNotIn("MENTIONS", prompt)

    def test_resolves_user_mentions_in_text(self):
        channels_data = [
            {
                "channel_id": "C01ABC",
                "name": "general",
                "messages": [{"user": "U123", "text": "hey <@U456> check this", "ts": "1.0"}],
            }
        ]
        prompt = build_ai_prompt(channels_data, {"U123": "Alice", "U456": "Bob"}, "", "14h")
        self.assertIn("@Bob", prompt)
        self.assertNotIn("<@U456>", prompt)

    def test_dm_channel_type(self):
        channels_data = [
            {
                "channel_id": "D01ABC",
                "name": "",
                "messages": [{"user": "U123", "text": "private msg", "ts": "1.0"}],
            }
        ]
        prompt = build_ai_prompt(channels_data, {}, "", "14h")
        self.assertIn('"type": "DM"', prompt)


class TestTruncateMessage(unittest.TestCase):
    def test_short_message(self):
        self.assertEqual(truncate_message("hello", 4000), "hello")

    def test_long_message(self):
        msg = "a" * 5000
        result = truncate_message(msg, 4000)
        self.assertLessEqual(len(result), 4000)
        self.assertIn("truncated", result)

    def test_truncates_at_newline(self):
        msg = "line1\n" * 800
        result = truncate_message(msg, 100)
        self.assertLessEqual(len(result), 100)


class TestFormatWarnings(unittest.TestCase):
    def test_no_warnings(self):
        self.assertEqual(format_warnings([]), "")

    def test_single_warning(self):
        result = format_warnings(["dump failed for C01"])
        self.assertIn("Note:", result)
        self.assertIn("dump failed for C01", result)

    def test_multiple_warnings(self):
        result = format_warnings(["dump failed", "parse error"])
        self.assertIn("; ", result)


class TestRunSummary(unittest.TestCase):
    @patch.object(_mod, "send_telegram")
    @patch.object(_mod, "call_ai", return_value="*Slack Summary*\n\nActive discussion in #general.")
    @patch.object(_mod, "resolve_users_via_slackdump", return_value={"U123": "Alice"})
    @patch.object(_mod, "load_user_cache", return_value={})
    @patch.object(_mod, "get_user_cache_path")
    @patch.object(_mod, "parse_channel_messages")
    @patch.object(_mod, "dump_all_channels")
    @patch.object(_mod, "validate_auth_with_retries", return_value=True)
    @patch.object(_mod, "setup_logging")
    def test_full_flow(
        self,
        mock_logging,
        mock_auth,
        mock_dump,
        mock_parse,
        mock_cache_path,
        mock_load_cache,
        mock_resolve,
        mock_ai,
        mock_tg,
    ):
        import tempfile

        # Setup logging mock
        mock_logger = MagicMock(spec=logging.Logger)
        mock_log_file = Path(tempfile.mktemp(suffix=".log"))
        mock_log_file.write_text("")
        mock_logging.return_value = (mock_logger, mock_log_file)

        # Setup cache path mock
        mock_cache_path.return_value = Path(tempfile.mktemp(suffix=".txt"))

        # Write config
        config_dir = tempfile.mkdtemp()
        config_path = Path(config_dir) / "config.yaml"
        config = {
            "hostname": "testhost",
            "channels": ["C01ABC"],
            "timeframe": "14h",
            "user_id": "U789",
        }
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        # Mock dump results
        mock_dump.return_value = ({"C01ABC": "/tmp/C01ABC.json"}, [])

        # Mock parse results
        mock_parse.return_value = (
            {"channel_id": "C01ABC", "name": "general"},
            [{"user": "U123", "text": "hello world", "ts": "1.0"}],
        )

        run_summary(config_path)

        mock_auth.assert_called_once()
        mock_dump.assert_called_once()
        mock_ai.assert_called_once()
        mock_tg.assert_called()

        mock_log_file.unlink(missing_ok=True)
        mock_cache_path.return_value.unlink(missing_ok=True)
        shutil.rmtree(config_dir)

    @patch.object(_mod, "send_telegram")
    @patch.object(_mod, "setup_logging")
    def test_missing_config(self, mock_logging, mock_tg):
        import tempfile

        mock_logger = MagicMock(spec=logging.Logger)
        mock_log_file = Path(tempfile.mktemp(suffix=".log"))
        mock_log_file.write_text("")
        mock_logging.return_value = (mock_logger, mock_log_file)

        run_summary(Path("/nonexistent/config.yaml"))

        mock_logger.error.assert_called()
        mock_tg.assert_called_once()
        self.assertIn("FATAL", mock_tg.call_args[0][0])
        mock_log_file.unlink(missing_ok=True)

    @patch.object(_mod, "send_telegram")
    @patch.object(_mod, "validate_auth_with_retries", return_value=False)
    @patch.object(_mod, "setup_logging")
    def test_auth_failure_stops(self, mock_logging, mock_auth, mock_tg):
        import tempfile

        mock_logger = MagicMock(spec=logging.Logger)
        mock_log_file = Path(tempfile.mktemp(suffix=".log"))
        mock_log_file.write_text("")
        mock_logging.return_value = (mock_logger, mock_log_file)

        config_dir = tempfile.mkdtemp()
        config_path = Path(config_dir) / "config.yaml"
        config = {
            "hostname": "testhost",
            "channels": ["C01ABC"],
            "timeframe": "14h",
        }
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        run_summary(config_path)

        # Auth failed — should not proceed to dump
        mock_auth.assert_called_once()

        mock_log_file.unlink(missing_ok=True)
        shutil.rmtree(config_dir)

    @patch.object(_mod, "send_telegram")
    @patch.object(_mod, "dump_all_channels", return_value=({}, ["Failed to dump C01ABC"]))
    @patch.object(_mod, "validate_auth_with_retries", return_value=True)
    @patch.object(_mod, "setup_logging")
    def test_all_dumps_failed(self, mock_logging, mock_auth, mock_dump, mock_tg):
        import tempfile

        mock_logger = MagicMock(spec=logging.Logger)
        mock_log_file = Path(tempfile.mktemp(suffix=".log"))
        mock_log_file.write_text("")
        mock_logging.return_value = (mock_logger, mock_log_file)

        config_dir = tempfile.mkdtemp()
        config_path = Path(config_dir) / "config.yaml"
        config = {
            "hostname": "testhost",
            "channels": ["C01ABC"],
            "timeframe": "14h",
        }
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        run_summary(config_path)

        # Should send FATAL
        fatal_calls = [c for c in mock_tg.call_args_list if "FATAL" in str(c)]
        self.assertGreater(len(fatal_calls), 0)

        mock_log_file.unlink(missing_ok=True)
        shutil.rmtree(config_dir)


class TestDumpChannel(unittest.TestCase):
    @patch("subprocess.run")
    def test_success(self, mock_run):
        import tempfile

        dump_dir = Path(tempfile.mkdtemp())
        json_file = dump_dir / "C01ABC.json"
        json_file.write_text('{"messages": []}')
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        with patch.object(_mod, "DUMP_DIR", dump_dir):
            result = dump_channel("C01ABC", "2026-04-01T00:00:00", MagicMock())
        self.assertIsNotNone(result)
        shutil.rmtree(dump_dir)

    @patch("subprocess.run")
    def test_failure(self, mock_run):
        import tempfile

        dump_dir = Path(tempfile.mkdtemp())
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")

        with patch.object(_mod, "DUMP_DIR", dump_dir):
            result = dump_channel("C01ABC", "2026-04-01T00:00:00", MagicMock())
        self.assertIsNone(result)
        shutil.rmtree(dump_dir)


class TestDumpAllChannels(unittest.TestCase):
    @patch.object(_mod, "dump_channel")
    def test_dumps_all_with_delay(self, mock_dump):
        import tempfile

        dump_dir = Path(tempfile.mkdtemp())
        mock_dump.return_value = str(dump_dir / "C01.json")
        sleep_calls = []

        with patch.object(_mod, "DUMP_DIR", dump_dir):
            files, warnings = dump_all_channels(
                ["C01", "C02"],
                "2026-04-01T00:00:00",
                MagicMock(),
                sleep_fn=lambda s: sleep_calls.append(s),
            )
        self.assertEqual(len(files), 2)
        self.assertEqual(len(warnings), 0)
        self.assertEqual(len(sleep_calls), 1)  # sleep between channels
        shutil.rmtree(dump_dir)

    @patch.object(_mod, "dump_channel")
    def test_collects_warnings_on_failure(self, mock_dump):
        import tempfile

        dump_dir = Path(tempfile.mkdtemp())
        mock_dump.return_value = None

        with patch.object(_mod, "DUMP_DIR", dump_dir):
            files, warnings = dump_all_channels(
                ["C01"],
                "2026-04-01T00:00:00",
                MagicMock(),
                sleep_fn=lambda _: None,
            )
        self.assertEqual(len(files), 0)
        self.assertEqual(len(warnings), 1)
        shutil.rmtree(dump_dir)


if __name__ == "__main__":
    if "--tests" in sys.argv:
        unittest.main(argv=[sys.argv[0]], exit=True)
    else:
        main()
