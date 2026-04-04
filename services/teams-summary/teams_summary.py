#!/usr/bin/env python3
"""Teams summary service — read Teams JSON dumps, summarize with AI, send via Telegram.

Usage:
    uv run python services/teams-summary/teams_summary.py [--tests]
    uv run python services/teams-summary/teams_summary.py [--timeframe 14h]
    uv run python services/teams-summary/teams_summary.py [--notify-on-match "keyword1,keyword2"]

Config (config.yaml):
    hostname: myhost
    data_dir: /path/to/teams/dumps
    timeframe: "14h"
    channels:
      - general
      - engineering
    ignore_files_smaller_than_bytes: 500
    llm_provider: claude
    llm_model: haiku
    llm_model_effort: ""
"""

import logging
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

# Add common helpers to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common" / "helpers"))
from ai import AIError, call_ai, call_ai_json  # noqa: E402
from log import setup_logging  # noqa: E402
from telegram import send_telegram  # noqa: E402

ET = ZoneInfo("America/New_York")

FILENAME_RE = re.compile(r"^(.+)_(\d+)mins_(\d{4}-\d{2}-\d{2})[_-](\d{4})\.json$")
DATE_FOLDER_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass
class FileInfo:
    chat_name: str
    start_time: datetime
    end_time: datetime
    path: Path


def parse_filename(filename: str, file_path: Path) -> FileInfo | None:
    """Parse a Teams JSON dump filename into a FileInfo.

    Filename format: <chat_name>_<lookback>mins_<yyyy-mm-dd>_<HHMM>.json
    (date-time separator can be _ or -)
    Returns None for non-matching filenames.
    """
    m = FILENAME_RE.match(filename)
    if not m:
        return None
    chat_name = m.group(1)
    lookback_mins = int(m.group(2))
    dt_str = f"{m.group(3)}-{m.group(4)}"
    try:
        file_dt = datetime.strptime(dt_str, "%Y-%m-%d-%H%M").replace(tzinfo=ET)
    except ValueError:
        return None
    start_time = file_dt - timedelta(minutes=lookback_mins)
    return FileInfo(
        chat_name=chat_name,
        start_time=start_time,
        end_time=file_dt,
        path=file_path,
    )


def discover_files(
    data_dir: str,
    window_start: datetime,
    window_end: datetime,
    ignore_files_smaller_than_bytes: int,
    logger: logging.Logger,
) -> tuple[list[FileInfo], list[str]]:
    """Discover Teams JSON dumps that overlap with the requested time window.

    Scans date folders (yyyy-mm-dd) within [start_date - 1 day, today],
    filters by file size, parses filenames, and keeps overlapping files.
    """
    root = Path(data_dir)
    if not root.is_dir():
        raise OSError(f"Data directory not found or not a directory: {data_dir}")

    scan_start = (window_start - timedelta(days=1)).date()
    scan_end = window_end.date()

    results = []
    scan_warnings: list[str] = []
    try:
        entries = sorted(root.iterdir())
    except OSError as e:
        raise OSError(f"Cannot list data directory {root}: {e}") from e
    for entry in entries:
        if not entry.is_dir() or not DATE_FOLDER_RE.match(entry.name):
            continue
        try:
            folder_date = datetime.strptime(entry.name, "%Y-%m-%d").date()
        except ValueError:
            logger.warning(f"Invalid date folder name: {entry.name}")
            continue
        if folder_date < scan_start or folder_date > scan_end:
            continue

        try:
            json_files = list(entry.iterdir())
        except OSError as e:
            msg = f"Cannot list directory {entry}: {e}"
            logger.warning(msg)
            scan_warnings.append(msg)
            continue
        for json_file in json_files:
            if not json_file.name.endswith(".json"):
                continue
            try:
                file_size = json_file.stat().st_size
            except OSError as e:
                msg = f"Cannot stat {json_file}: {e}"
                logger.warning(msg)
                scan_warnings.append(msg)
                continue
            if file_size < ignore_files_smaller_than_bytes:
                logger.debug(f"Skipping small file: {json_file} ({file_size} bytes)")
                continue
            info = parse_filename(json_file.name, json_file)
            if info is None:
                logger.warning(f"Filename does not match expected pattern: {json_file.name}")
                continue
            # Check time window overlap: file [start, end] overlaps [window_start, window_end]
            if info.end_time >= window_start and info.start_time <= window_end:
                results.append(info)
            else:
                logger.debug(f"File outside time window: {json_file.name}")

    logger.info(f"Discovered {len(results)} matching file(s) in {data_dir}")
    return results, scan_warnings


def group_by_channel(
    file_infos: list[FileInfo],
    channels_filter: list[str] | None,
) -> dict[str, list[FileInfo]]:
    """Group file infos by chat_name, apply optional channel filtering, sort chronologically.

    channels_filter: if provided, only include channels whose chat_name contains
    one of the filter strings (case-insensitive substring match).
    """
    grouped: dict[str, list[FileInfo]] = defaultdict(list)
    for info in file_infos:
        if channels_filter:
            name_lower = info.chat_name.lower()
            if not any(f.lower() in name_lower for f in channels_filter):
                continue
        grouped[info.chat_name].append(info)

    # Sort files within each group chronologically by start_time
    for files in grouped.values():
        files.sort(key=lambda f: f.start_time)

    return dict(grouped)


def _escape_md(text: str) -> str:
    """Escape Markdown V1 special characters in text used inside formatting."""
    for char in ("\\", "*", "_", "`", "["):
        text = text.replace(char, f"\\{char}")
    return text


def _coerce_bool(value: object) -> bool:
    """Coerce a value to bool, handling string representations from LLM JSON.

    Only accepts bool, str, and int — other types (lists, dicts) are treated as False
    to avoid Python truthiness coercion misrouting alerts.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    if isinstance(value, int):
        return value == 1
    return False


def _is_valid_bool_value(value: object) -> bool:
    """Check if a value is a valid bool-like value from LLM JSON.

    Rejects None, lists, dicts, floats, and unrecognized strings that _coerce_bool
    would silently map to False — preventing false-negative alert suppression.
    """
    if isinstance(value, bool):
        return True
    if isinstance(value, str):
        return value.strip().lower() in ("true", "false", "1", "0", "yes", "no")
    return isinstance(value, int) and value in (0, 1)


def _build_header(hostname: str, title: str, window_end: datetime, timeframe: str = "") -> str:
    """Build Telegram message header with optional hostname prefix and delta."""
    date_str = window_end.strftime("%B %-d, %Y")
    title_date = f"{date_str} | Δ{timeframe}" if timeframe else date_str
    if hostname:
        return f"\\[{_escape_md(hostname)}] *{title}* ({title_date})"
    return f"*{title}* ({title_date})"


def read_and_summarize_channel(
    channel_name: str,
    file_infos: list[FileInfo],
    config: dict,
    logger: logging.Logger,
) -> tuple[str | None, list[str]]:
    """Read all JSON files for a channel, summarize via AI.

    Concatenates file contents with metadata headers, builds an AI prompt
    matching slack-summary output style, and calls call_ai().

    Returns (summary_text, read_warnings) or (None, read_warnings) if all files
    failed to read or AI call fails.
    """
    parts = []
    read_warnings: list[str] = []
    for info in file_infos:
        try:
            content = info.path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as e:
            msg = f"Failed to read {info.path}: {e}"
            logger.warning(msg)
            read_warnings.append(msg)
            continue
        header = (
            f"--- {info.path.name} "
            f"(covers {info.start_time.strftime('%Y-%m-%d %H:%M')} to "
            f"{info.end_time.strftime('%Y-%m-%d %H:%M')} ET) ---"
        )
        parts.append(f"{header}\n{content}")

    if not parts:
        return None, read_warnings

    combined = "\n\n".join(parts)

    prompt = (
        "You are a Teams chat summary assistant. Generate a concise narrative summary "
        f"of these Microsoft Teams messages from the channel/chat: {channel_name}.\n\n"
        "Rules:\n"
        "- Write 2-5 sentence narrative paragraphs (NOT bullet lists)\n"
        "- Attribute statements to people using @DisplayName\n"
        "- Focus on: technical/operational topics, decisions, incidents, deployments, action items\n"
        "- Skip: casual chat, jokes, emoji-only reactions, scheduling\n"
        "- Messages may overlap between dumps; do not repeat information\n"
        "- If there are no substantive messages, respond with exactly: no activity\n"
        "- Do NOT include a title or header — it will be added separately\n"
        "- Max 3500 characters total\n"
        "- Use Telegram Markdown V1 formatting (bold with *, no other markdown)\n\n"
        f"Messages:\n{combined}"
    )

    provider = config.get("llm_provider", "claude")
    model = config.get("llm_model", "")
    effort = config.get("llm_model_effort", "")

    try:
        return call_ai(prompt, provider=provider, model=model, effort=effort), read_warnings
    except AIError as e:
        logger.error(f"AI summarization failed for {channel_name}: {e}")
        return None, read_warnings


def run_summary(config: dict, args, logger: logging.Logger) -> None:
    """Orchestrate the full summary flow.

    Discovers files, groups by channel, summarizes each, assembles and sends
    the telegram message.
    """
    hostname = config.get("hostname", "")
    timeframe = args.timeframe if args.timeframe else config["timeframe"]

    try:
        window_start, window_end = parse_timeframe(timeframe)
    except ValueError as e:
        logger.error(f"Timeframe error: {e}")
        send_telegram(f"teams-summary FATAL: {_escape_md(str(e))}", hostname=hostname)
        return

    ignore_files_smaller_than_bytes = config.get("ignore_files_smaller_than_bytes", 500)
    try:
        files, scan_warnings = discover_files(config["data_dir"], window_start, window_end, ignore_files_smaller_than_bytes, logger)
    except OSError as e:
        logger.error(f"Data directory error: {e}")
        send_telegram(f"teams-summary FATAL: {_escape_md(str(e))}", hostname=hostname)
        return

    if not files:
        header = _build_header(hostname, "Teams Summary", window_end, timeframe)
        if scan_warnings:
            logger.error(f"No files found with scan errors: {'; '.join(scan_warnings)}")
            escaped = "; ".join(_escape_md(w) for w in scan_warnings)
            msg = f"{header}\n\nWARNING: scan errors may have affected results: {escaped}"
        else:
            logger.info("No matching files found")
            msg = f"{header}\n\nNo activity in the configured timeframe."
        send_telegram(msg)
        return

    channels_filter = config.get("channels")
    grouped = group_by_channel(files, channels_filter)

    if not grouped:
        header = _build_header(hostname, "Teams Summary", window_end, timeframe)
        if scan_warnings:
            logger.error(f"No channels matched but scan had warnings: {'; '.join(scan_warnings)}")
            escaped = "; ".join(_escape_md(w) for w in scan_warnings)
            msg = f"{header}\n\nWARNING: scan errors may have affected results: {escaped}"
        else:
            logger.info("No channels matched after filtering")
            msg = f"{header}\n\nNo activity in the configured timeframe."
        send_telegram(msg)
        return

    summaries = []
    no_activity_count = 0
    warnings = [_escape_md(w) for w in scan_warnings]
    for channel_name, channel_files in sorted(grouped.items()):
        logger.info(f"Summarizing channel: {channel_name} ({len(channel_files)} file(s))")
        summary, read_warnings = read_and_summarize_channel(channel_name, channel_files, config, logger)
        for rw in read_warnings:
            warnings.append(_escape_md(rw))
        if summary is None:
            if len(read_warnings) == len(channel_files):
                warnings.append(f"All files unreadable for {_escape_md(channel_name)}")
            else:
                warnings.append(f"AI failed for {_escape_md(channel_name)}")
            continue
        stripped = summary.strip()
        if stripped.lower() == "no activity":
            logger.info(f"No activity in {channel_name}")
            no_activity_count += 1
            continue
        summaries.append(f"*{_escape_md(channel_name)}*\n{stripped}")

    header = _build_header(hostname, "Teams Summary", window_end, timeframe)

    if not summaries:
        # Distinguish between genuine no-activity and total failure
        has_failures = any("AI failed for" in w or "All files unreadable for" in w for w in warnings)
        if has_failures and no_activity_count == 0:
            body = "All channel summaries failed — activity may have been missed."
        elif has_failures:
            body = "Some channel summaries failed — activity may have been missed."
        else:
            body = "No activity in the configured timeframe."
    else:
        body = "\n\n".join(summaries)

    warning_text = ""
    if warnings:
        warning_text = "\n\nNote: " + "; ".join(warnings)

    full_message = header + "\n\n" + body + warning_text

    try:
        sent_ok = send_telegram(full_message)
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        sent_ok = False

    if sent_ok:
        logger.info("Teams summary sent successfully")
    else:
        logger.warning("Teams summary telegram delivery failed")


def run_notify_on_match(config: dict, args, logger: logging.Logger) -> None:
    """Check conversations for keyword mentions and notify if unresolved.

    Discovers files same as summary mode, reads all relevant JSON content,
    uses AI to check for keyword mentions. Only sends Telegram if keywords
    are mentioned and the topic is unresolved.
    """
    hostname = config.get("hostname", "")
    timeframe = args.timeframe if args.timeframe else config["timeframe"]
    keywords = [k.strip() for k in args.notify_on_match.split(",") if k.strip()]

    if not keywords:
        logger.warning("No keywords provided for notify-on-match mode")
        return

    try:
        window_start, window_end = parse_timeframe(timeframe)
    except ValueError as e:
        logger.error(f"Timeframe error: {e}")
        send_telegram(f"teams-summary FATAL: {_escape_md(str(e))}", hostname=hostname)
        return

    ignore_files_smaller_than_bytes = config.get("ignore_files_smaller_than_bytes", 500)
    try:
        files, scan_warnings = discover_files(config["data_dir"], window_start, window_end, ignore_files_smaller_than_bytes, logger)
    except OSError as e:
        logger.error(f"Data directory error: {e}")
        send_telegram(f"teams-summary notify-on-match FATAL: {_escape_md(str(e))}", hostname=hostname)
        return

    if not files:
        if scan_warnings:
            logger.error(f"No files found with scan errors: {'; '.join(scan_warnings)}")
            escaped = "; ".join(_escape_md(w) for w in scan_warnings)
            send_telegram(
                f"teams-summary notify-on-match WARNING: scan errors may have prevented keyword detection: {escaped}",
                hostname=hostname,
            )
        else:
            logger.info("No matching files found for notify-on-match")
        return

    channels_filter = config.get("channels")
    grouped = group_by_channel(files, channels_filter)

    if not grouped:
        logger.info("No channels matched after filtering for notify-on-match")
        if scan_warnings:
            logger.error(f"No channels matched but scan had warnings: {'; '.join(scan_warnings)}")
            escaped = "; ".join(_escape_md(w) for w in scan_warnings)
            send_telegram(
                f"teams-summary notify-on-match WARNING: scan errors may have prevented keyword detection: {escaped}",
                hostname=hostname,
            )
        return

    # Read all file contents
    all_content_parts = []
    read_warnings: list[str] = []
    for channel_name, channel_files in sorted(grouped.items()):
        for info in channel_files:
            try:
                content = info.path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError) as e:
                msg = f"Failed to read {info.path}: {e}"
                logger.warning(msg)
                read_warnings.append(msg)
                continue
            header = (
                f"--- {channel_name}: {info.path.name} "
                f"(covers {info.start_time.strftime('%Y-%m-%d %H:%M')} to "
                f"{info.end_time.strftime('%Y-%m-%d %H:%M')} ET) ---"
            )
            all_content_parts.append(f"{header}\n{content}")

    all_warnings = list(scan_warnings) + read_warnings

    if not all_content_parts:
        if all_warnings:
            logger.error(f"No readable files with errors: {'; '.join(all_warnings)}")
            escaped = "; ".join(_escape_md(w) for w in all_warnings)
            send_telegram(
                f"teams-summary notify-on-match WARNING: file errors may have prevented keyword detection: {escaped}",
                hostname=hostname,
            )
        else:
            logger.info("No readable files for notify-on-match")
        return

    combined = "\n\n".join(all_content_parts)
    keywords_str = ", ".join(keywords)

    prompt = (
        "You are a Teams chat monitoring assistant. Check if any of the following "
        f"keywords/phrases are discussed in these Microsoft Teams messages: {keywords_str}\n\n"
        "Respond with a JSON object containing exactly these fields:\n"
        '- "mentioned": boolean — true if any of the keywords/phrases appear or are '
        "discussed in the conversations\n"
        '- "resolved": boolean — true if the topic was discussed AND resolved/concluded '
        "(someone provided a fix, answer, or confirmation that no action is needed)\n"
        '- "context": string — brief summary (2-4 sentences) of what was discussed about '
        "the keywords, including who was involved and what was said. If not mentioned, "
        'write "Not mentioned in any conversations."\n\n'
        "Important:\n"
        "- Look for both exact keyword matches and semantic references to the topics\n"
        "- Only set resolved=true if there is clear evidence the issue is handled\n"
        "- Return ONLY the JSON object, no other text\n\n"
        f"Messages:\n{combined}"
    )

    provider = config.get("llm_provider", "claude")
    model = config.get("llm_model", "")
    effort = config.get("llm_model_effort", "")

    try:
        result = call_ai_json(prompt, provider=provider, model=model, effort=effort)
    except AIError as e:
        logger.error(f"AI notify-on-match failed: {e}")
        send_telegram(f"teams-summary notify-on-match FATAL: AI call failed: {_escape_md(str(e))}", hostname=hostname)
        return

    if not isinstance(result, dict):
        logger.error(f"AI returned unexpected JSON type: {type(result).__name__}")
        send_telegram("teams-summary notify-on-match FATAL: unexpected AI response format", hostname=hostname)
        return

    missing_fields = [f for f in ("mentioned", "resolved", "context") if f not in result]
    if missing_fields:
        logger.error(f"AI response missing required fields: {', '.join(missing_fields)}")
        send_telegram(
            f"teams-summary notify-on-match FATAL: AI response missing fields: {', '.join(missing_fields)}",
            hostname=hostname,
        )
        return

    invalid_fields = [f for f in ("mentioned", "resolved") if not _is_valid_bool_value(result[f])]
    if invalid_fields:
        logger.error(f"AI response has invalid bool values for: {', '.join(invalid_fields)}")
        send_telegram(
            f"teams-summary notify-on-match FATAL: invalid bool values for: {', '.join(invalid_fields)}",
            hostname=hostname,
        )
        return

    mentioned = _coerce_bool(result["mentioned"])
    resolved = _coerce_bool(result["resolved"])
    context = str(result["context"])

    if not mentioned:
        logger.info(f"Keywords not mentioned: {keywords_str}")
        if all_warnings:
            escaped = "; ".join(_escape_md(w) for w in all_warnings)
            send_telegram(
                f"teams-summary notify-on-match WARNING: partial scan — some files unreadable: {escaped}",
                hostname=hostname,
            )
        return

    if resolved:
        logger.info(f"Keywords mentioned but resolved: {keywords_str}")
        if all_warnings:
            escaped = "; ".join(_escape_md(w) for w in all_warnings)
            send_telegram(
                f"teams-summary notify-on-match WARNING: partial scan — some files unreadable: {escaped}",
                hostname=hostname,
            )
        return

    # Mentioned and unresolved — send notification
    logger.info(f"Keywords mentioned and UNRESOLVED: {keywords_str}")
    header = _build_header(hostname, "Teams Alert", window_end, timeframe)
    warning_note = ""
    if all_warnings:
        escaped = "; ".join(_escape_md(w) for w in all_warnings)
        warning_note = f"\n\nNote: partial scan — {escaped}"
    message = f"{header}\n\n{_escape_md(context)}{warning_note}"

    try:
        sent_ok = send_telegram(message)
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        sent_ok = False

    if sent_ok:
        logger.info("Teams alert sent successfully")
    else:
        logger.warning("Teams alert telegram delivery failed")


def load_config(config_path: Path) -> dict:
    """Load and validate config.yaml."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    if not config or "data_dir" not in config:
        raise ValueError("Config must contain 'data_dir' key")
    if "timeframe" not in config:
        raise ValueError("Config must contain 'timeframe' key")
    return config


def parse_timeframe(timeframe: str) -> tuple[datetime, datetime]:
    """Convert timeframe string (e.g. '14h', '30m', '1d') to ET-aware datetime pair.

    Returns (window_start, window_end) where window_end is now in ET
    and window_start is timeframe ago from now.
    """
    match = re.match(r"^(\d+)([hmd])$", timeframe.strip())
    if not match:
        raise ValueError(f"Invalid timeframe format: {timeframe!r} (expected e.g. '14h', '30m', '1d')")
    value = int(match.group(1))
    unit = match.group(2)
    now = datetime.now(ET)
    if unit == "h":
        start = now - timedelta(hours=value)
    elif unit == "m":
        start = now - timedelta(minutes=value)
    elif unit == "d":
        start = now - timedelta(days=value)
    else:
        raise ValueError(f"Unknown timeframe unit: {unit}")
    return start, now


def main() -> None:
    import argparse
    import time

    parser = argparse.ArgumentParser(description="Teams summary service")
    parser.add_argument("--tests", action="store_true", help="Run tests and exit")
    parser.add_argument("--timeframe", type=str, help="Override config timeframe (e.g. '14h', '30m', '1d')")
    parser.add_argument("--notify-on-match", type=str, help="Comma-separated keywords for notify-on-match mode")
    args = parser.parse_args()

    if args.tests:
        sys.argv = [sys.argv[0]]
        import unittest

        unittest.main(module="__main__", exit=True)

    t0 = time.monotonic()
    logger, log_file = setup_logging("teams-summary")
    mode = "notify-on-match" if args.notify_on_match else "summary"
    logger.info(f"{'*' * 20} teams-summary ({mode}) starting {'*' * 20}")

    config_path = Path(__file__).resolve().parent / "config.yaml"
    try:
        config = load_config(config_path)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        send_telegram(f"teams-summary FATAL: {_escape_md(str(e))}")
        return

    if args.notify_on_match:
        run_notify_on_match(config, args, logger)
    else:
        run_summary(config, args, logger)

    elapsed = time.monotonic() - t0
    logger.info(f"{'*' * 20} completed, elapsed {elapsed / 60:.1f}m {'*' * 20}")


if __name__ == "__main__":
    if "--tests" in sys.argv:
        import os
        import tempfile
        import unittest
        from unittest.mock import patch

        class TestLoadConfig(unittest.TestCase):
            def test_valid_config(self):
                with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                    yaml.dump(
                        {
                            "hostname": "test",
                            "data_dir": "/tmp/teams",
                            "timeframe": "14h",
                            "channels": ["general"],
                        },
                        f,
                    )
                    f.flush()
                    config = load_config(Path(f.name))
                self.assertEqual(config["hostname"], "test")
                self.assertEqual(config["data_dir"], "/tmp/teams")
                self.assertEqual(config["timeframe"], "14h")
                self.assertEqual(config["channels"], ["general"])
                Path(f.name).unlink()

            def test_missing_config(self):
                with self.assertRaises(FileNotFoundError):
                    load_config(Path("/nonexistent/config.yaml"))

            def test_missing_data_dir(self):
                with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                    yaml.dump({"hostname": "test", "timeframe": "14h"}, f)
                    f.flush()
                    with self.assertRaises(ValueError):
                        load_config(Path(f.name))
                Path(f.name).unlink()

            def test_missing_timeframe(self):
                with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                    yaml.dump({"hostname": "test", "data_dir": "/tmp/teams"}, f)
                    f.flush()
                    with self.assertRaises(ValueError):
                        load_config(Path(f.name))
                Path(f.name).unlink()

            def test_optional_fields_defaults(self):
                with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                    yaml.dump({"data_dir": "/tmp/teams", "timeframe": "14h"}, f)
                    f.flush()
                    config = load_config(Path(f.name))
                self.assertEqual(config.get("hostname"), None)
                self.assertEqual(config.get("channels"), None)
                self.assertEqual(config.get("ignore_files_smaller_than_bytes"), None)
                Path(f.name).unlink()

            def test_empty_config(self):
                with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                    f.write("")
                    f.flush()
                    with self.assertRaises(ValueError):
                        load_config(Path(f.name))
                Path(f.name).unlink()

        class TestParseTimeframe(unittest.TestCase):
            def test_hours(self):
                start, end = parse_timeframe("14h")
                diff = end - start
                self.assertAlmostEqual(diff.total_seconds(), 14 * 3600, delta=2)
                self.assertEqual(str(start.tzinfo), "America/New_York")
                self.assertEqual(str(end.tzinfo), "America/New_York")

            def test_minutes(self):
                start, end = parse_timeframe("30m")
                diff = end - start
                self.assertAlmostEqual(diff.total_seconds(), 30 * 60, delta=2)

            def test_days(self):
                start, end = parse_timeframe("1d")
                diff = end - start
                self.assertAlmostEqual(diff.total_seconds(), 86400, delta=2)

            def test_invalid_format(self):
                with self.assertRaises(ValueError):
                    parse_timeframe("abc")

            def test_invalid_unit(self):
                with self.assertRaises(ValueError):
                    parse_timeframe("14x")

            def test_whitespace_stripped(self):
                start, end = parse_timeframe("  14h  ")
                diff = end - start
                self.assertAlmostEqual(diff.total_seconds(), 14 * 3600, delta=2)

            def test_returns_et_aware(self):
                start, end = parse_timeframe("1h")
                self.assertIsNotNone(start.tzinfo)
                self.assertIsNotNone(end.tzinfo)
                self.assertEqual(str(start.tzinfo), "America/New_York")

            def test_end_is_now(self):
                _, end = parse_timeframe("1h")
                now = datetime.now(ET)
                self.assertAlmostEqual(end.timestamp(), now.timestamp(), delta=2)

        class TestCLITimeframePrecedence(unittest.TestCase):
            def setUp(self):
                self.tmpdir = tempfile.mkdtemp()
                self.logger = logging.getLogger("test_cli_precedence")
                self.config = {
                    "data_dir": self.tmpdir,
                    "timeframe": "14h",
                    "hostname": "testhost",
                    "ignore_files_smaller_than_bytes": 10,
                }

            def tearDown(self):
                import shutil

                shutil.rmtree(self.tmpdir)

            @patch("__main__.send_telegram", return_value=True)
            def test_cli_overrides_config(self, mock_tg):
                """CLI --timeframe should override config timeframe in run_summary."""
                from types import SimpleNamespace

                args = SimpleNamespace(timeframe="1h", notify_on_match=None)
                with patch("__main__.parse_timeframe", wraps=parse_timeframe) as mock_pt:
                    run_summary(self.config, args, self.logger)
                    mock_pt.assert_called_once_with("1h")

            @patch("__main__.send_telegram", return_value=True)
            def test_config_used_when_no_cli(self, mock_tg):
                """Config timeframe should be used when CLI --timeframe is None."""
                from types import SimpleNamespace

                args = SimpleNamespace(timeframe=None, notify_on_match=None)
                with patch("__main__.parse_timeframe", wraps=parse_timeframe) as mock_pt:
                    run_summary(self.config, args, self.logger)
                    mock_pt.assert_called_once_with("14h")

        class TestParseFilename(unittest.TestCase):
            def test_normal_name(self):
                info = parse_filename(
                    "General_60mins_2026-04-02-1430.json",
                    Path("/data/2026-04-02/General_60mins_2026-04-02-1430.json"),
                )
                self.assertIsNotNone(info)
                self.assertEqual(info.chat_name, "General")
                self.assertEqual(
                    info.end_time,
                    datetime(2026, 4, 2, 14, 30, tzinfo=ET),
                )
                self.assertEqual(
                    info.start_time,
                    datetime(2026, 4, 2, 13, 30, tzinfo=ET),
                )
                self.assertEqual(
                    info.path,
                    Path("/data/2026-04-02/General_60mins_2026-04-02-1430.json"),
                )

            def test_name_with_underscores(self):
                info = parse_filename(
                    "My_Team_Chat_30mins_2026-04-02-0900.json",
                    Path("/data/file.json"),
                )
                self.assertIsNotNone(info)
                self.assertEqual(info.chat_name, "My_Team_Chat")
                self.assertEqual(
                    info.end_time,
                    datetime(2026, 4, 2, 9, 0, tzinfo=ET),
                )
                self.assertEqual(
                    info.start_time,
                    datetime(2026, 4, 2, 8, 30, tzinfo=ET),
                )

            def test_name_with_special_chars(self):
                info = parse_filename(
                    "Project-Alpha (v2)_120mins_2026-04-02-1800.json",
                    Path("/data/file.json"),
                )
                self.assertIsNotNone(info)
                self.assertEqual(info.chat_name, "Project-Alpha (v2)")
                expected_end = datetime(2026, 4, 2, 18, 0, tzinfo=ET)
                self.assertEqual(info.end_time, expected_end)
                self.assertEqual(info.start_time, expected_end - timedelta(minutes=120))

            def test_various_lookback_30mins(self):
                info = parse_filename("Chat_30mins_2026-04-02-1000.json", Path("/f"))
                self.assertIsNotNone(info)
                self.assertEqual(
                    info.start_time,
                    datetime(2026, 4, 2, 9, 30, tzinfo=ET),
                )

            def test_various_lookback_120mins(self):
                info = parse_filename("Chat_120mins_2026-04-02-1000.json", Path("/f"))
                self.assertIsNotNone(info)
                self.assertEqual(
                    info.start_time,
                    datetime(2026, 4, 2, 8, 0, tzinfo=ET),
                )

            def test_non_matching_returns_none(self):
                self.assertIsNone(parse_filename("not_a_valid_file.json", Path("/f")))
                self.assertIsNone(parse_filename("chat_60_2026-04-02-1000.json", Path("/f")))
                self.assertIsNone(parse_filename("chat_60mins_bad-date.json", Path("/f")))
                self.assertIsNone(parse_filename("readme.txt", Path("/f")))

            def test_invalid_date_values_returns_none(self):
                # Regex matches but strptime fails on impossible date
                self.assertIsNone(parse_filename("Chat_60mins_2026-99-99-9999.json", Path("/f")))
                self.assertIsNone(parse_filename("Chat_60mins_2026-13-01-1400.json", Path("/f")))

            def test_no_mins_suffix_returns_none(self):
                self.assertIsNone(parse_filename("Chat_60hours_2026-04-02-1000.json", Path("/f")))

        class TestDiscoverFiles(unittest.TestCase):
            def setUp(self):
                self.tmpdir = tempfile.mkdtemp()
                self.logger = logging.getLogger("test_discover")

            def tearDown(self):
                import shutil

                shutil.rmtree(self.tmpdir)

            def _create_file(self, folder_name, filename, size=600):
                folder = Path(self.tmpdir) / folder_name
                folder.mkdir(exist_ok=True)
                fpath = folder / filename
                fpath.write_text("x" * size)
                return fpath

            def test_basic_discovery(self):
                self._create_file("2026-04-02", "General_60mins_2026-04-02-1400.json")
                self._create_file("2026-04-02", "Dev_60mins_2026-04-02-1500.json")
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results, _ = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 2)

            def test_filters_by_size(self):
                self._create_file("2026-04-02", "General_60mins_2026-04-02-1400.json", size=100)
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results, _ = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)

            def test_filters_by_time_window(self):
                # File covers 13:00-14:00 on Apr 2
                self._create_file("2026-04-02", "General_60mins_2026-04-02-1400.json")
                # Window is Apr 2 15:00-16:00 — no overlap
                window_start = datetime(2026, 4, 2, 15, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results, _ = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)

            def test_skips_non_date_folders(self):
                self._create_file("not-a-date", "General_60mins_2026-04-02-1400.json")
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results, _ = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)

            def test_skips_out_of_range_date_folders(self):
                self._create_file("2026-03-01", "General_60mins_2026-03-01-1400.json")
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results, _ = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)

            def test_nonexistent_data_dir(self):
                with self.assertRaises(OSError):
                    discover_files("/nonexistent/dir", datetime.now(ET), datetime.now(ET), 500, self.logger)

            def test_non_json_files_ignored(self):
                self._create_file("2026-04-02", "General_60mins_2026-04-02-1400.txt")
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results, _ = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)

            def test_date_folder_buffer(self):
                # File in 2026-04-01 folder but window starts 2026-04-02
                # Buffer: scan_start = 2026-04-01, so this folder should be scanned
                self._create_file("2026-04-01", "General_60mins_2026-04-01-2330.json")
                window_start = datetime(2026, 4, 2, 0, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 1, 0, tzinfo=ET)
                results, _ = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                # File covers 22:30-23:30 on Apr 1, window is 00:00-01:00 Apr 2 — no overlap
                self.assertEqual(len(results), 0)

            def test_date_folder_buffer_with_overlap(self):
                # File in 2026-04-01 covers 23:00-00:30 (spanning midnight)
                self._create_file("2026-04-01", "General_90mins_2026-04-02-0030.json")
                window_start = datetime(2026, 4, 2, 0, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 1, 0, tzinfo=ET)
                results, _ = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 1)

            def test_no_matching_files(self):
                os.makedirs(Path(self.tmpdir) / "2026-04-02")
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results, _ = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)

            def test_stat_failure_skips_file(self):
                self._create_file("2026-04-02", "General_60mins_2026-04-02-1400.json")
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                original_stat = Path.stat

                def failing_stat(self_path, *args, **kwargs):
                    if self_path.name.endswith(".json"):
                        raise OSError("Permission denied")
                    return original_stat(self_path, *args, **kwargs)

                with patch("pathlib.Path.stat", failing_stat):
                    results, warnings = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)
                self.assertEqual(len(warnings), 1)
                self.assertIn("Permission denied", warnings[0])

            def test_invalid_date_folder_skipped(self):
                # Create a folder that matches the date regex but has invalid date values
                os.makedirs(Path(self.tmpdir) / "2026-99-99")
                (Path(self.tmpdir) / "2026-99-99" / "General_60mins_2026-04-02-1400.json").write_text("x" * 600)
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results, _ = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)

            def test_folder_iterdir_failure_returns_warnings(self):
                """Per-folder iterdir failure returns scan warnings."""
                os.makedirs(Path(self.tmpdir) / "2026-04-02")
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                original_iterdir = Path.iterdir

                def failing_iterdir(self_path, *args, **kwargs):
                    if self_path.name == "2026-04-02":
                        raise OSError("Permission denied")
                    return original_iterdir(self_path, *args, **kwargs)

                with patch("pathlib.Path.iterdir", failing_iterdir):
                    results, warnings = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)
                self.assertEqual(len(warnings), 1)
                self.assertIn("Permission denied", warnings[0])

            def test_root_iterdir_failure_raises(self):
                """Permission failure on root iterdir should raise, not return empty."""
                original_iterdir = Path.iterdir

                def failing_iterdir(self_path, *args, **kwargs):
                    if str(self_path) == self.tmpdir:
                        raise OSError("Permission denied")
                    return original_iterdir(self_path, *args, **kwargs)

                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                with patch("pathlib.Path.iterdir", failing_iterdir), self.assertRaises(OSError):
                    discover_files(self.tmpdir, window_start, window_end, 500, self.logger)

        class TestGroupByChannel(unittest.TestCase):
            def _make_info(self, name, hour):
                return FileInfo(
                    chat_name=name,
                    start_time=datetime(2026, 4, 2, hour, 0, tzinfo=ET),
                    end_time=datetime(2026, 4, 2, hour + 1, 0, tzinfo=ET),
                    path=Path(f"/data/{name}_{hour}.json"),
                )

            def test_basic_grouping(self):
                infos = [
                    self._make_info("General", 10),
                    self._make_info("Dev", 11),
                    self._make_info("General", 12),
                ]
                grouped = group_by_channel(infos, None)
                self.assertEqual(len(grouped), 2)
                self.assertEqual(len(grouped["General"]), 2)
                self.assertEqual(len(grouped["Dev"]), 1)

            def test_chronological_sort(self):
                infos = [
                    self._make_info("General", 14),
                    self._make_info("General", 10),
                    self._make_info("General", 12),
                ]
                grouped = group_by_channel(infos, None)
                hours = [f.start_time.hour for f in grouped["General"]]
                self.assertEqual(hours, [10, 12, 14])

            def test_channel_filter_include(self):
                infos = [
                    self._make_info("General", 10),
                    self._make_info("Dev-Team", 11),
                    self._make_info("Marketing", 12),
                ]
                grouped = group_by_channel(infos, ["dev", "general"])
                self.assertIn("General", grouped)
                self.assertIn("Dev-Team", grouped)
                self.assertNotIn("Marketing", grouped)

            def test_channel_filter_case_insensitive(self):
                infos = [self._make_info("GENERAL", 10)]
                grouped = group_by_channel(infos, ["general"])
                self.assertIn("GENERAL", grouped)

            def test_channel_filter_substring_match(self):
                infos = [self._make_info("Engineering-Team-Alpha", 10)]
                grouped = group_by_channel(infos, ["team"])
                self.assertIn("Engineering-Team-Alpha", grouped)

            def test_channel_filter_empty_list(self):
                infos = [self._make_info("General", 10)]
                grouped = group_by_channel(infos, [])
                # Empty filter list should include all channels
                self.assertIn("General", grouped)

            def test_all_filtered_out(self):
                infos = [self._make_info("General", 10)]
                grouped = group_by_channel(infos, ["nonexistent"])
                self.assertEqual(len(grouped), 0)

            def test_empty_input(self):
                grouped = group_by_channel([], None)
                self.assertEqual(len(grouped), 0)

        class TestReadAndSummarizeChannel(unittest.TestCase):
            def setUp(self):
                self.tmpdir = tempfile.mkdtemp()
                self.logger = logging.getLogger("test_summarize")
                self.config = {
                    "data_dir": self.tmpdir,
                    "timeframe": "14h",
                    "llm_provider": "claude",
                    "llm_model": "haiku",
                }

            def tearDown(self):
                import shutil

                shutil.rmtree(self.tmpdir)

            def _make_file_info(self, filename, content):
                fpath = Path(self.tmpdir) / filename
                fpath.write_text(content, encoding="utf-8")
                return FileInfo(
                    chat_name="General",
                    start_time=datetime(2026, 4, 2, 13, 0, tzinfo=ET),
                    end_time=datetime(2026, 4, 2, 14, 0, tzinfo=ET),
                    path=fpath,
                )

            @patch("__main__.call_ai", return_value="Summary text here")
            def test_builds_prompt_with_dedup_instruction(self, mock_ai):
                info = self._make_file_info("test.json", '{"messages": []}')
                result, warnings = read_and_summarize_channel("General", [info], self.config, self.logger)
                self.assertEqual(result, "Summary text here")
                self.assertEqual(warnings, [])
                prompt = mock_ai.call_args[0][0]
                self.assertIn("do not repeat information", prompt)
                self.assertIn("General", prompt)
                self.assertIn("narrative", prompt)
                self.assertIn("3500", prompt)
                self.assertIn("Telegram Markdown V1", prompt)

            @patch("__main__.call_ai", return_value="Summary text")
            def test_passes_provider_model_effort(self, mock_ai):
                info = self._make_file_info("test.json", '{"messages": []}')
                config = {**self.config, "llm_provider": "codex", "llm_model": "gpt-5.4", "llm_model_effort": "xhigh"}
                read_and_summarize_channel("General", [info], config, self.logger)
                _, kwargs = mock_ai.call_args
                self.assertEqual(kwargs["provider"], "codex")
                self.assertEqual(kwargs["model"], "gpt-5.4")
                self.assertEqual(kwargs["effort"], "xhigh")

            @patch("__main__.call_ai", side_effect=AIError("fail"))
            def test_ai_failure_returns_none(self, mock_ai):
                info = self._make_file_info("test.json", '{"messages": []}')
                result, warnings = read_and_summarize_channel("General", [info], self.config, self.logger)
                self.assertIsNone(result)
                self.assertEqual(warnings, [])

            @patch("__main__.call_ai", return_value="Summary")
            def test_multiple_files_concatenated(self, mock_ai):
                info1 = self._make_file_info("file1.json", '{"msg": "first"}')
                info2 = FileInfo(
                    chat_name="General",
                    start_time=datetime(2026, 4, 2, 14, 0, tzinfo=ET),
                    end_time=datetime(2026, 4, 2, 15, 0, tzinfo=ET),
                    path=Path(self.tmpdir) / "file2.json",
                )
                (Path(self.tmpdir) / "file2.json").write_text('{"msg": "second"}', encoding="utf-8")
                read_and_summarize_channel("General", [info1, info2], self.config, self.logger)
                prompt = mock_ai.call_args[0][0]
                self.assertIn("first", prompt)
                self.assertIn("second", prompt)

            def test_unreadable_files_returns_warnings(self):
                info = FileInfo(
                    chat_name="General",
                    start_time=datetime(2026, 4, 2, 13, 0, tzinfo=ET),
                    end_time=datetime(2026, 4, 2, 14, 0, tzinfo=ET),
                    path=Path("/nonexistent/file.json"),
                )
                result, warnings = read_and_summarize_channel("General", [info], self.config, self.logger)
                self.assertIsNone(result)
                self.assertEqual(len(warnings), 1)
                self.assertIn("/nonexistent/file.json", warnings[0])

            @patch("__main__.call_ai", return_value="Summary")
            def test_prompt_includes_file_time_range(self, mock_ai):
                info = self._make_file_info("test.json", '{"messages": []}')
                read_and_summarize_channel("General", [info], self.config, self.logger)
                prompt = mock_ai.call_args[0][0]
                self.assertIn("2026-04-02 13:00", prompt)
                self.assertIn("2026-04-02 14:00", prompt)

            @patch("__main__.call_ai", return_value="Summary")
            def test_partial_read_failure_returns_warnings(self, mock_ai):
                good_info = self._make_file_info("good.json", '{"messages": []}')
                bad_info = FileInfo(
                    chat_name="General",
                    start_time=datetime(2026, 4, 2, 14, 0, tzinfo=ET),
                    end_time=datetime(2026, 4, 2, 15, 0, tzinfo=ET),
                    path=Path("/nonexistent/bad.json"),
                )
                result, warnings = read_and_summarize_channel(
                    "General", [good_info, bad_info], self.config, self.logger
                )
                self.assertEqual(result, "Summary")
                self.assertEqual(len(warnings), 1)
                self.assertIn("/nonexistent/bad.json", warnings[0])

        class TestRunSummary(unittest.TestCase):
            def setUp(self):
                self.tmpdir = tempfile.mkdtemp()
                self.logger = logging.getLogger("test_run_summary")
                self.config = {
                    "data_dir": self.tmpdir,
                    "timeframe": "14h",
                    "hostname": "testhost",
                    "ignore_files_smaller_than_bytes": 10,
                }

            def tearDown(self):
                import shutil

                shutil.rmtree(self.tmpdir)

            def _make_args(self, timeframe=None, notify_on_match=None):
                from types import SimpleNamespace

                return SimpleNamespace(timeframe=timeframe, notify_on_match=notify_on_match)

            @patch("__main__.send_telegram", return_value=True)
            def test_no_files_sends_no_activity(self, mock_tg):
                run_summary(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("No activity", msg)
                self.assertIn("Teams Summary", msg)

            @patch("__main__.send_telegram", return_value=True)
            def test_header_with_hostname(self, mock_tg):
                run_summary(self.config, self._make_args(), self.logger)
                msg = mock_tg.call_args[0][0]
                self.assertIn("\\[testhost]", msg)
                self.assertIn("*Teams Summary*", msg)

            @patch("__main__.send_telegram", return_value=True)
            def test_header_without_hostname(self, mock_tg):
                config = {**self.config, "hostname": ""}
                run_summary(config, self._make_args(), self.logger)
                msg = mock_tg.call_args[0][0]
                self.assertNotIn("\\[", msg)
                self.assertIn("*Teams Summary*", msg)

            @patch("__main__.send_telegram", return_value=True)
            @patch("__main__.call_ai", return_value="Channel discussion summary")
            def test_full_flow_with_files(self, mock_ai, mock_tg):
                # Create a matching file
                folder = Path(self.tmpdir) / datetime.now(ET).strftime("%Y-%m-%d")
                folder.mkdir()
                now = datetime.now(ET)
                fname = f"General_60mins_{now.strftime('%Y-%m-%d-%H%M')}.json"
                (folder / fname).write_text('{"messages": [{"text": "hello"}]}' * 20)
                run_summary(self.config, self._make_args(), self.logger)
                mock_ai.assert_called_once()
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("*General*", msg)
                self.assertIn("Channel discussion summary", msg)

            @patch("__main__.send_telegram", return_value=True)
            @patch("__main__.call_ai", side_effect=AIError("fail"))
            def test_ai_failure_adds_warning(self, mock_ai, mock_tg):
                folder = Path(self.tmpdir) / datetime.now(ET).strftime("%Y-%m-%d")
                folder.mkdir()
                now = datetime.now(ET)
                fname = f"General_60mins_{now.strftime('%Y-%m-%d-%H%M')}.json"
                (folder / fname).write_text('{"messages": [{"text": "hello"}]}' * 20)
                run_summary(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("AI failed for General", msg)
                self.assertIn("All channel summaries failed", msg)
                self.assertNotIn("No activity in the configured timeframe", msg)

            @patch("__main__.send_telegram", return_value=True)
            @patch("__main__.call_ai", return_value="no activity")
            def test_no_activity_channels_excluded(self, mock_ai, mock_tg):
                folder = Path(self.tmpdir) / datetime.now(ET).strftime("%Y-%m-%d")
                folder.mkdir()
                now = datetime.now(ET)
                fname = f"General_60mins_{now.strftime('%Y-%m-%d-%H%M')}.json"
                (folder / fname).write_text('{"messages": [{"text": "hello"}]}' * 20)
                run_summary(self.config, self._make_args(), self.logger)
                msg = mock_tg.call_args[0][0]
                self.assertNotIn("*General*", msg)
                self.assertIn("No activity", msg)

            @patch("__main__.send_telegram", return_value=True)
            def test_mixed_no_activity_and_failure_says_some_not_all(self, mock_tg):
                """One channel returns 'no activity', another fails — must say 'some' not 'all'."""
                folder = Path(self.tmpdir) / datetime.now(ET).strftime("%Y-%m-%d")
                folder.mkdir()
                now = datetime.now(ET)
                # Channel that will return "no activity"
                fname1 = f"General_60mins_{now.strftime('%Y-%m-%d-%H%M')}.json"
                (folder / fname1).write_text('{"messages": [{"text": "hello"}]}' * 20)
                # Channel that will fail (unreadable file)
                fname2 = f"Engineering_60mins_{now.strftime('%Y-%m-%d-%H%M')}.json"
                (folder / fname2).write_text('{"messages": [{"text": "hello"}]}' * 20)

                def selective_ai(prompt, **kwargs):
                    if "General" in prompt:
                        return "no activity"
                    raise AIError("fail")

                with patch("__main__.call_ai", side_effect=selective_ai):
                    run_summary(self.config, self._make_args(), self.logger)
                msg = mock_tg.call_args[0][0]
                self.assertIn("Some channel summaries failed", msg)
                self.assertNotIn("All channel summaries failed", msg)

            @patch("__main__.send_telegram", return_value=True)
            def test_cli_timeframe_override(self, mock_tg):
                run_summary(self.config, self._make_args(timeframe="1h"), self.logger)
                # Should succeed without error (uses 1h instead of config's 14h)
                mock_tg.assert_called_once()

            @patch("__main__.send_telegram", return_value=True)
            def test_header_hostname_special_chars_escaped(self, mock_tg):
                config = {**self.config, "hostname": "my_server_01"}
                run_summary(config, self._make_args(), self.logger)
                msg = mock_tg.call_args[0][0]
                self.assertIn("\\[my\\_server\\_01]", msg)
                self.assertNotIn("[my_server_01]", msg)

            @patch("__main__.send_telegram", return_value=True)
            def test_data_dir_error_sends_fatal(self, mock_tg):
                config = {**self.config, "data_dir": "/nonexistent/broken/path"}
                run_summary(config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                call_args = mock_tg.call_args
                msg = call_args[0][0]
                self.assertIn("FATAL", msg)

            @patch("__main__.send_telegram", return_value=True)
            def test_scan_warnings_in_no_activity_message(self, mock_tg):
                """Scan warnings should escalate to WARNING when no files found."""
                os.makedirs(Path(self.tmpdir) / "2026-04-02")
                original_iterdir = Path.iterdir

                def failing_iterdir(self_path, *args, **kwargs):
                    if self_path.name == "2026-04-02":
                        raise OSError("Permission denied")
                    return original_iterdir(self_path, *args, **kwargs)

                with patch("pathlib.Path.iterdir", failing_iterdir):
                    run_summary(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("WARNING", msg)
                self.assertIn("scan errors", msg)
                self.assertIn("Permission denied", msg)

            @patch("__main__.send_telegram", return_value=True)
            @patch("__main__.call_ai", return_value="Discussion summary")
            def test_channel_name_with_underscores_escaped(self, mock_ai, mock_tg):
                folder = Path(self.tmpdir) / datetime.now(ET).strftime("%Y-%m-%d")
                folder.mkdir()
                now = datetime.now(ET)
                fname = f"dev_ops_team_60mins_{now.strftime('%Y-%m-%d-%H%M')}.json"
                (folder / fname).write_text('{"messages": [{"text": "hello"}]}' * 20)
                run_summary(self.config, self._make_args(), self.logger)
                msg = mock_tg.call_args[0][0]
                # Underscores in channel name must be escaped to avoid Markdown V1 issues
                self.assertIn("*dev\\_ops\\_team*", msg)
                self.assertNotIn("*dev_ops_team*", msg)

            @patch("__main__.send_telegram", return_value=True)
            def test_all_files_unreadable_says_unreadable_not_ai_failed(self, mock_tg):
                """When all files for a channel fail to read, warn about read failure not AI."""
                folder = Path(self.tmpdir) / datetime.now(ET).strftime("%Y-%m-%d")
                folder.mkdir()
                now = datetime.now(ET)
                fname = f"General_60mins_{now.strftime('%Y-%m-%d-%H%M')}.json"
                (folder / fname).write_text('{"messages": [{"text": "hello"}]}' * 20)

                original_read = Path.read_text

                def failing_read(self_path, *args, **kwargs):
                    if self_path.name.endswith(".json"):
                        raise OSError("Permission denied")
                    return original_read(self_path, *args, **kwargs)

                with patch("pathlib.Path.read_text", failing_read):
                    run_summary(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("All files unreadable for General", msg)
                self.assertNotIn("AI failed", msg)
                self.assertIn("All channel summaries failed", msg)
                self.assertNotIn("No activity in the configured timeframe", msg)

            @patch("__main__.send_telegram", return_value=True)
            @patch("__main__.call_ai", return_value="Summary")
            def test_scan_warnings_propagated_when_files_found(self, mock_ai, mock_tg):
                """Scan warnings should appear in final message even when some files are found."""
                # Create two date folders, one will fail to list
                today = datetime.now(ET).strftime("%Y-%m-%d")
                folder = Path(self.tmpdir) / today
                folder.mkdir()
                now = datetime.now(ET)
                fname = f"General_60mins_{now.strftime('%Y-%m-%d-%H%M')}.json"
                (folder / fname).write_text('{"messages": [{"text": "hello"}]}' * 20)
                # Create another folder that will fail
                yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
                os.makedirs(Path(self.tmpdir) / yesterday)
                original_iterdir = Path.iterdir

                def failing_iterdir(self_path, *args, **kwargs):
                    if self_path.name == yesterday:
                        raise OSError("Permission denied")
                    return original_iterdir(self_path, *args, **kwargs)

                with patch("pathlib.Path.iterdir", failing_iterdir):
                    run_summary(self.config, self._make_args(), self.logger)
                msg = mock_tg.call_args[0][0]
                self.assertIn("Summary", msg)
                self.assertIn("Note:", msg)
                self.assertIn("Permission denied", msg)

        class TestRunNotifyOnMatch(unittest.TestCase):
            def setUp(self):
                self.tmpdir = tempfile.mkdtemp()
                self.logger = logging.getLogger("test_notify")
                self.config = {
                    "data_dir": self.tmpdir,
                    "timeframe": "14h",
                    "hostname": "testhost",
                    "ignore_files_smaller_than_bytes": 10,
                }

            def tearDown(self):
                import shutil

                shutil.rmtree(self.tmpdir)

            def _make_args(self, notify_on_match="keyword1,keyword2", timeframe=None):
                from types import SimpleNamespace

                return SimpleNamespace(timeframe=timeframe, notify_on_match=notify_on_match)

            def _create_test_file(self):
                folder = Path(self.tmpdir) / datetime.now(ET).strftime("%Y-%m-%d")
                folder.mkdir(exist_ok=True)
                now = datetime.now(ET)
                fname = f"General_60mins_{now.strftime('%Y-%m-%d-%H%M')}.json"
                (folder / fname).write_text('{"messages": [{"text": "discussion about keyword1"}]}' * 10)

            @patch(
                "__main__.call_ai_json",
                return_value={"mentioned": False, "resolved": False, "context": "Not mentioned."},
            )
            @patch("__main__.send_telegram", return_value=True)
            def test_not_mentioned_no_telegram(self, mock_tg, mock_ai):
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_ai.assert_called_once()
                mock_tg.assert_not_called()

            @patch(
                "__main__.call_ai_json",
                return_value={"mentioned": True, "resolved": True, "context": "Resolved by @Alice."},
            )
            @patch("__main__.send_telegram", return_value=True)
            def test_mentioned_resolved_no_telegram(self, mock_tg, mock_ai):
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_ai.assert_called_once()
                mock_tg.assert_not_called()

            @patch(
                "__main__.call_ai_json",
                return_value={
                    "mentioned": True,
                    "resolved": False,
                    "context": "@Bob raised keyword1 issue, no response yet.",
                },
            )
            @patch("__main__.send_telegram", return_value=True)
            def test_mentioned_unresolved_sends_telegram(self, mock_tg, mock_ai):
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_ai.assert_called_once()
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("*Teams Alert*", msg)
                self.assertIn("\\[testhost]", msg)
                self.assertIn("@Bob raised keyword1 issue", msg)

            @patch("__main__.call_ai_json")
            @patch("__main__.send_telegram", return_value=True)
            def test_prompt_includes_all_keywords(self, mock_tg, mock_ai):
                mock_ai.return_value = {"mentioned": False, "resolved": False, "context": "Not mentioned."}
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args("deploy,outage,rollback"), self.logger)
                prompt = mock_ai.call_args[0][0]
                self.assertIn("deploy", prompt)
                self.assertIn("outage", prompt)
                self.assertIn("rollback", prompt)

            @patch("__main__.call_ai_json", side_effect=AIError("AI fail"))
            @patch("__main__.send_telegram", return_value=True)
            def test_ai_failure_sends_error_telegram(self, mock_tg, mock_ai):
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("FATAL", msg)

            @patch("__main__.call_ai_json", return_value=["not", "a", "dict"])
            @patch("__main__.send_telegram", return_value=True)
            def test_ai_non_dict_response_sends_error(self, mock_tg, mock_ai):
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("FATAL", msg)
                self.assertIn("unexpected AI response format", msg)

            @patch("__main__.call_ai_json", return_value={})
            @patch("__main__.send_telegram", return_value=True)
            def test_ai_empty_dict_sends_missing_fields_error(self, mock_tg, mock_ai):
                """AI returning {} must be treated as format error, not 'not mentioned'."""
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("FATAL", msg)
                self.assertIn("missing fields", msg)

            @patch("__main__.call_ai_json", return_value={"context": "Discussed outage"})
            @patch("__main__.send_telegram", return_value=True)
            def test_ai_partial_fields_sends_missing_fields_error(self, mock_tg, mock_ai):
                """AI returning only 'context' must not silently default mentioned=False."""
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("FATAL", msg)
                self.assertIn("missing fields", msg)
                self.assertIn("mentioned", msg)
                self.assertIn("resolved", msg)

            @patch(
                "__main__.call_ai_json",
                return_value={"mentioned": None, "resolved": False, "context": "Not mentioned."},
            )
            @patch("__main__.send_telegram", return_value=True)
            def test_null_mentioned_sends_invalid_bool_error(self, mock_tg, mock_ai):
                """AI returning null for mentioned must be a schema error, not silent False."""
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("FATAL", msg)
                self.assertIn("invalid bool values", msg)

            @patch(
                "__main__.call_ai_json",
                return_value={"mentioned": "maybe", "resolved": False, "context": "Unclear."},
            )
            @patch("__main__.send_telegram", return_value=True)
            def test_unrecognized_string_mentioned_sends_invalid_bool_error(self, mock_tg, mock_ai):
                """AI returning unrecognized string for mentioned must not silently become False."""
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("FATAL", msg)
                self.assertIn("invalid bool values", msg)

            @patch(
                "__main__.call_ai_json",
                return_value={"mentioned": [True], "resolved": False, "context": "List."},
            )
            @patch("__main__.send_telegram", return_value=True)
            def test_list_mentioned_sends_invalid_bool_error(self, mock_tg, mock_ai):
                """AI returning list for mentioned must be a schema error."""
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("FATAL", msg)
                self.assertIn("invalid bool values", msg)

            @patch("__main__.send_telegram", return_value=True)
            def test_no_files_no_action(self, mock_tg):
                # Empty data dir — no files
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_not_called()

            @patch("__main__.send_telegram", return_value=True)
            def test_data_dir_error_sends_fatal(self, mock_tg):
                config = {**self.config, "data_dir": "/nonexistent/broken/path"}
                run_notify_on_match(config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("FATAL", msg)

            @patch("__main__.send_telegram", return_value=True)
            def test_empty_keywords_no_action(self, mock_tg):
                run_notify_on_match(self.config, self._make_args(notify_on_match="  , ,  "), self.logger)
                mock_tg.assert_not_called()

            @patch("__main__.send_telegram", return_value=True)
            def test_scan_warnings_send_telegram_when_no_files(self, mock_tg):
                """Scan warnings should trigger telegram when no files found."""
                os.makedirs(Path(self.tmpdir) / "2026-04-02")
                original_iterdir = Path.iterdir

                def failing_iterdir(self_path, *args, **kwargs):
                    if self_path.name == "2026-04-02":
                        raise OSError("Permission denied")
                    return original_iterdir(self_path, *args, **kwargs)

                with patch("pathlib.Path.iterdir", failing_iterdir):
                    run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("WARNING", msg)
                self.assertIn("Permission denied", msg)

            @patch("__main__.send_telegram", return_value=True)
            def test_grouped_empty_with_scan_warnings_sends_warning(self, mock_tg):
                """When channel filter drops all files but scan had warnings, send warning."""
                today = datetime.now(ET).strftime("%Y-%m-%d")
                folder = Path(self.tmpdir) / today
                folder.mkdir()
                now = datetime.now(ET)
                # Create a file for a channel NOT in the filter
                fname = f"OffTopic_60mins_{now.strftime('%Y-%m-%d-%H%M')}.json"
                (folder / fname).write_text('{"messages": [{"text": "hello"}]}' * 10)
                # Create a second date folder that will fail to list (generating scan_warning)
                yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
                os.makedirs(Path(self.tmpdir) / yesterday)
                original_iterdir = Path.iterdir

                def failing_iterdir(self_path, *args, **kwargs):
                    if self_path.name == yesterday:
                        raise OSError("Permission denied")
                    return original_iterdir(self_path, *args, **kwargs)

                config = {**self.config, "channels": ["ImportantChannel"]}
                with patch("pathlib.Path.iterdir", failing_iterdir):
                    run_notify_on_match(config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("WARNING", msg)
                self.assertIn("Permission denied", msg)

            @patch(
                "__main__.call_ai_json",
                return_value={"mentioned": True, "resolved": False, "context": "Issue found."},
            )
            @patch("__main__.send_telegram", return_value=True)
            def test_header_without_hostname(self, mock_tg, mock_ai):
                self._create_test_file()
                config = {**self.config, "hostname": ""}
                run_notify_on_match(config, self._make_args(), self.logger)
                msg = mock_tg.call_args[0][0]
                self.assertNotIn("\\[", msg)
                self.assertIn("*Teams Alert*", msg)

            @patch(
                "__main__.call_ai_json",
                return_value={"mentioned": "false", "resolved": "false", "context": "Not mentioned."},
            )
            @patch("__main__.send_telegram", return_value=True)
            def test_string_false_treated_as_not_mentioned(self, mock_tg, mock_ai):
                """LLM returning string 'false' instead of bool must not trigger alert."""
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_not_called()

            @patch(
                "__main__.call_ai_json",
                return_value={"mentioned": "true", "resolved": "true", "context": "Resolved."},
            )
            @patch("__main__.send_telegram", return_value=True)
            def test_string_true_mentioned_resolved_no_alert(self, mock_tg, mock_ai):
                """LLM returning string 'true' for resolved must not trigger alert."""
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_not_called()

            @patch(
                "__main__.call_ai_json",
                return_value={"mentioned": "true", "resolved": "false", "context": "Needs attention."},
            )
            @patch("__main__.send_telegram", return_value=True)
            def test_string_true_mentioned_unresolved_sends_alert(self, mock_tg, mock_ai):
                """LLM returning string 'true'/'false' for mentioned/resolved must send alert."""
                self._create_test_file()
                run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("Needs attention.", msg)

            @patch(
                "__main__.call_ai_json",
                return_value={"mentioned": False, "resolved": False, "context": "Not mentioned."},
            )
            @patch("__main__.send_telegram", return_value=True)
            def test_not_mentioned_with_scan_warnings_sends_warning(self, mock_tg, mock_ai):
                """When AI says not mentioned but scan had warnings, send partial-scan warning."""
                today = datetime.now(ET).strftime("%Y-%m-%d")
                folder = Path(self.tmpdir) / today
                folder.mkdir()
                now = datetime.now(ET)
                fname = f"General_60mins_{now.strftime('%Y-%m-%d-%H%M')}.json"
                (folder / fname).write_text('{"messages": [{"text": "hello"}]}' * 10)
                # Also create a folder that will fail to list
                yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
                os.makedirs(Path(self.tmpdir) / yesterday)
                original_iterdir = Path.iterdir

                def failing_iterdir(self_path, *args, **kwargs):
                    if self_path.name == yesterday:
                        raise OSError("Permission denied")
                    return original_iterdir(self_path, *args, **kwargs)

                with patch("pathlib.Path.iterdir", failing_iterdir):
                    run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("WARNING", msg)
                self.assertIn("partial scan", msg)
                self.assertIn("Permission denied", msg)

            @patch("__main__.send_telegram", return_value=True)
            def test_all_files_unreadable_sends_warning(self, mock_tg):
                """When all discovered files fail to read, send a warning."""
                self._create_test_file()
                original_read = Path.read_text

                def failing_read(self_path, *args, **kwargs):
                    if self_path.name.endswith(".json"):
                        raise OSError("Permission denied")
                    return original_read(self_path, *args, **kwargs)

                with patch("pathlib.Path.read_text", failing_read):
                    run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("WARNING", msg)
                self.assertIn("file errors", msg)

            @patch(
                "__main__.call_ai_json",
                return_value={"mentioned": True, "resolved": False, "context": "Issue found."},
            )
            @patch("__main__.send_telegram", return_value=True)
            def test_alert_includes_scan_warnings(self, mock_tg, mock_ai):
                """When sending alert, include scan warnings in the message."""
                today = datetime.now(ET).strftime("%Y-%m-%d")
                folder = Path(self.tmpdir) / today
                folder.mkdir()
                now = datetime.now(ET)
                fname = f"General_60mins_{now.strftime('%Y-%m-%d-%H%M')}.json"
                (folder / fname).write_text('{"messages": [{"text": "hello"}]}' * 10)
                yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
                os.makedirs(Path(self.tmpdir) / yesterday)
                original_iterdir = Path.iterdir

                def failing_iterdir(self_path, *args, **kwargs):
                    if self_path.name == yesterday:
                        raise OSError("Permission denied")
                    return original_iterdir(self_path, *args, **kwargs)

                with patch("pathlib.Path.iterdir", failing_iterdir):
                    run_notify_on_match(self.config, self._make_args(), self.logger)
                mock_tg.assert_called_once()
                msg = mock_tg.call_args[0][0]
                self.assertIn("*Teams Alert*", msg)
                self.assertIn("Issue found.", msg)
                self.assertIn("Note: partial scan", msg)
                self.assertIn("Permission denied", msg)

        class TestCoerceBool(unittest.TestCase):
            def test_bool_true(self):
                self.assertTrue(_coerce_bool(True))

            def test_bool_false(self):
                self.assertFalse(_coerce_bool(False))

            def test_string_true(self):
                self.assertTrue(_coerce_bool("true"))

            def test_string_false(self):
                self.assertFalse(_coerce_bool("false"))

            def test_string_true_capitalized(self):
                self.assertTrue(_coerce_bool("True"))

            def test_string_false_capitalized(self):
                self.assertFalse(_coerce_bool("False"))

            def test_string_yes(self):
                self.assertTrue(_coerce_bool("yes"))

            def test_string_no(self):
                self.assertFalse(_coerce_bool("no"))

            def test_string_1(self):
                self.assertTrue(_coerce_bool("1"))

            def test_string_0(self):
                self.assertFalse(_coerce_bool("0"))

            def test_empty_string(self):
                self.assertFalse(_coerce_bool(""))

            def test_none(self):
                self.assertFalse(_coerce_bool(None))

            def test_int_1(self):
                self.assertTrue(_coerce_bool(1))

            def test_int_0(self):
                self.assertFalse(_coerce_bool(0))

            def test_string_true_with_whitespace(self):
                self.assertTrue(_coerce_bool(" true "))

            def test_string_false_with_whitespace(self):
                self.assertFalse(_coerce_bool(" false "))

            def test_list_treated_as_false(self):
                self.assertFalse(_coerce_bool([True]))

            def test_dict_treated_as_false(self):
                self.assertFalse(_coerce_bool({"key": "value"}))

            def test_float_treated_as_false(self):
                self.assertFalse(_coerce_bool(1.0))

        class TestIsValidBoolValue(unittest.TestCase):
            def test_bool_true_valid(self):
                self.assertTrue(_is_valid_bool_value(True))

            def test_bool_false_valid(self):
                self.assertTrue(_is_valid_bool_value(False))

            def test_string_true_valid(self):
                self.assertTrue(_is_valid_bool_value("true"))

            def test_string_false_valid(self):
                self.assertTrue(_is_valid_bool_value("false"))

            def test_string_yes_valid(self):
                self.assertTrue(_is_valid_bool_value("yes"))

            def test_string_no_valid(self):
                self.assertTrue(_is_valid_bool_value("no"))

            def test_string_1_valid(self):
                self.assertTrue(_is_valid_bool_value("1"))

            def test_string_0_valid(self):
                self.assertTrue(_is_valid_bool_value("0"))

            def test_int_1_valid(self):
                self.assertTrue(_is_valid_bool_value(1))

            def test_int_0_valid(self):
                self.assertTrue(_is_valid_bool_value(0))

            def test_int_2_invalid(self):
                self.assertFalse(_is_valid_bool_value(2))

            def test_int_negative_invalid(self):
                self.assertFalse(_is_valid_bool_value(-1))

            def test_none_invalid(self):
                self.assertFalse(_is_valid_bool_value(None))

            def test_string_maybe_invalid(self):
                self.assertFalse(_is_valid_bool_value("maybe"))

            def test_empty_string_invalid(self):
                self.assertFalse(_is_valid_bool_value(""))

            def test_list_invalid(self):
                self.assertFalse(_is_valid_bool_value([True]))

            def test_dict_invalid(self):
                self.assertFalse(_is_valid_bool_value({"key": "value"}))

            def test_float_invalid(self):
                self.assertFalse(_is_valid_bool_value(1.0))

        class TestEscapeMd(unittest.TestCase):
            def test_escapes_underscores(self):
                self.assertEqual(_escape_md("dev_ops"), "dev\\_ops")

            def test_escapes_asterisks(self):
                self.assertEqual(_escape_md("*bold*"), "\\*bold\\*")

            def test_escapes_brackets(self):
                self.assertEqual(_escape_md("[alerts]"), "\\[alerts]")

            def test_escapes_backticks(self):
                self.assertEqual(_escape_md("`code`"), "\\`code\\`")

            def test_no_change_plain_text(self):
                self.assertEqual(_escape_md("General"), "General")

            def test_escapes_backslash(self):
                self.assertEqual(_escape_md("a\\b"), "a\\\\b")

        class TestEnvExampleConfig(unittest.TestCase):
            """Verify the example config in env.example-main.yml is valid and complete."""

            def setUp(self):
                self.env_path = Path(__file__).resolve().parents[2] / "env.example-main.yml"

            def test_example_config_is_valid_yaml(self):
                env = yaml.safe_load(self.env_path.read_text())
                config_str = env["vars"]["TEAMS_SUMMARY_SERVICE_CONFIG"]
                config = yaml.safe_load(config_str)
                self.assertIsInstance(config, dict)

            def test_example_config_has_required_keys(self):
                env = yaml.safe_load(self.env_path.read_text())
                config_str = env["vars"]["TEAMS_SUMMARY_SERVICE_CONFIG"]
                config = yaml.safe_load(config_str)
                self.assertIn("data_dir", config)
                self.assertIn("timeframe", config)

            def test_example_config_loads_via_load_config(self):
                env = yaml.safe_load(self.env_path.read_text())
                config_str = env["vars"]["TEAMS_SUMMARY_SERVICE_CONFIG"]
                tmp = Path(tempfile.mkdtemp()) / "config.yaml"
                tmp.write_text(config_str)
                try:
                    config = load_config(tmp)
                    self.assertEqual(config["data_dir"], "/path/to/teams/json-dumps")
                    self.assertEqual(config["timeframe"], "14h")
                finally:
                    tmp.unlink()
                    tmp.parent.rmdir()

        unittest.main(argv=[sys.argv[0]], exit=True)
    else:
        main()
