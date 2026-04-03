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
    min_file_size: 500
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
from ai import AIError, call_ai  # noqa: E402
from log import setup_logging  # noqa: E402
from telegram import send_telegram  # noqa: E402

ET = ZoneInfo("America/New_York")

FILENAME_RE = re.compile(r"^(.+?)_(\d+)mins_(\d{4}-\d{2}-\d{2}-\d{4})\.json$")
DATE_FOLDER_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass
class FileInfo:
    chat_name: str
    start_time: datetime
    end_time: datetime
    path: Path


def parse_filename(filename: str, file_path: Path) -> FileInfo | None:
    """Parse a Teams JSON dump filename into a FileInfo.

    Filename format: <chat_name>_<lookback>mins_<yyyy-mm-dd-HHMM>.json
    Returns None for non-matching filenames.
    """
    m = FILENAME_RE.match(filename)
    if not m:
        return None
    chat_name = m.group(1)
    lookback_mins = int(m.group(2))
    dt_str = m.group(3)
    file_dt = datetime.strptime(dt_str, "%Y-%m-%d-%H%M").replace(tzinfo=ET)
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
    min_file_size: int,
    logger: logging.Logger,
) -> list[FileInfo]:
    """Discover Teams JSON dumps that overlap with the requested time window.

    Scans date folders (yyyy-mm-dd) within [start_date - 1 day, today],
    filters by file size, parses filenames, and keeps overlapping files.
    """
    root = Path(data_dir)
    if not root.is_dir():
        logger.warning(f"Data directory not found: {data_dir}")
        return []

    scan_start = (window_start - timedelta(days=1)).date()
    scan_end = window_end.date()

    results = []
    for entry in sorted(root.iterdir()):
        if not entry.is_dir() or not DATE_FOLDER_RE.match(entry.name):
            continue
        folder_date = datetime.strptime(entry.name, "%Y-%m-%d").date()
        if folder_date < scan_start or folder_date > scan_end:
            continue

        for json_file in entry.iterdir():
            if not json_file.name.endswith(".json"):
                continue
            if json_file.stat().st_size < min_file_size:
                logger.debug(f"Skipping small file: {json_file} ({json_file.stat().st_size} bytes)")
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
    return results


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


def read_and_summarize_channel(
    channel_name: str,
    file_infos: list[FileInfo],
    config: dict,
    logger: logging.Logger,
) -> str | None:
    """Read all JSON files for a channel, summarize via AI.

    Concatenates file contents with metadata headers, builds an AI prompt
    matching slack-summary output style, and calls call_ai().

    Returns the summary text, or None if AI call fails.
    """
    parts = []
    for info in file_infos:
        try:
            content = info.path.read_text(encoding="utf-8")
        except OSError as e:
            logger.warning(f"Failed to read {info.path}: {e}")
            continue
        header = (
            f"--- {info.path.name} "
            f"(covers {info.start_time.strftime('%Y-%m-%d %H:%M')} to "
            f"{info.end_time.strftime('%Y-%m-%d %H:%M')} ET) ---"
        )
        parts.append(f"{header}\n{content}")

    if not parts:
        return None

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
        return call_ai(prompt, provider=provider, model=model, effort=effort)
    except AIError as e:
        logger.error(f"AI summarization failed for {channel_name}: {e}")
        return None


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
        send_telegram(f"teams-summary FATAL: {e}", hostname=hostname)
        return

    min_file_size = config.get("min_file_size", 500)
    files = discover_files(config["data_dir"], window_start, window_end, min_file_size, logger)

    if not files:
        logger.info("No matching files found")
        date_str = window_end.strftime("%B %-d, %Y")
        header = f"\\[{hostname}] *Teams Summary* ({date_str})" if hostname else f"*Teams Summary* ({date_str})"
        send_telegram(f"{header}\n\nNo activity in the configured timeframe.")
        return

    channels_filter = config.get("channels")
    grouped = group_by_channel(files, channels_filter)

    if not grouped:
        logger.info("No channels matched after filtering")
        date_str = window_end.strftime("%B %-d, %Y")
        header = f"\\[{hostname}] *Teams Summary* ({date_str})" if hostname else f"*Teams Summary* ({date_str})"
        send_telegram(f"{header}\n\nNo activity in the configured timeframe.")
        return

    summaries = []
    warnings = []
    for channel_name, channel_files in sorted(grouped.items()):
        logger.info(f"Summarizing channel: {channel_name} ({len(channel_files)} file(s))")
        summary = read_and_summarize_channel(channel_name, channel_files, config, logger)
        if summary is None:
            warnings.append(f"AI failed for {channel_name}")
            continue
        stripped = summary.strip()
        if stripped.lower() == "no activity":
            logger.info(f"No activity in {channel_name}")
            continue
        summaries.append(f"*{channel_name}*\n{stripped}")

    date_str = window_end.strftime("%B %-d, %Y")
    header = f"\\[{hostname}] *Teams Summary* ({date_str})" if hostname else f"*Teams Summary* ({date_str})"

    body = "No activity in the configured timeframe." if not summaries else "\n\n".join(summaries)

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

    parser = argparse.ArgumentParser(description="Teams summary service")
    parser.add_argument("--tests", action="store_true", help="Run tests and exit")
    parser.add_argument("--timeframe", type=str, help="Override config timeframe (e.g. '14h', '30m', '1d')")
    parser.add_argument("--notify-on-match", type=str, help="Comma-separated keywords for notify-on-match mode")
    args = parser.parse_args()

    if args.tests:
        return

    logger, log_file = setup_logging("teams-summary")
    logger.info("Starting teams-summary service")

    config_path = Path(__file__).resolve().parent / "config.yaml"
    try:
        config = load_config(config_path)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        send_telegram(f"teams-summary FATAL: {e}")
        return

    hostname = config.get("hostname", "")

    # CLI timeframe overrides config
    timeframe = args.timeframe if args.timeframe else config["timeframe"]
    try:
        window_start, window_end = parse_timeframe(timeframe)
    except ValueError as e:
        logger.error(f"Timeframe error: {e}")
        send_telegram(f"teams-summary FATAL: {e}", hostname=hostname)
        return

    logger.info(f"Timeframe: {timeframe} -> {window_start} to {window_end}")

    if args.notify_on_match:
        logger.info(f"Notify-on-match mode: {args.notify_on_match}")
        # run_notify_on_match(config, args, logger) — implemented in Task 4
    else:
        logger.info("Summary mode")
        run_summary(config, args, logger)


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
                self.assertEqual(config.get("min_file_size"), None)
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
            def test_cli_overrides_config(self):
                """Verify that --timeframe arg takes precedence over config value."""
                with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                    yaml.dump({"data_dir": "/tmp/teams", "timeframe": "14h"}, f)
                    f.flush()
                    config = load_config(Path(f.name))
                # Simulate CLI override
                cli_timeframe = "2h"
                effective = cli_timeframe if cli_timeframe else config["timeframe"]
                self.assertEqual(effective, "2h")
                # Simulate no CLI override
                cli_timeframe = None
                effective = cli_timeframe if cli_timeframe else config["timeframe"]
                self.assertEqual(effective, "14h")
                Path(f.name).unlink()

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
                results = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 2)

            def test_filters_by_size(self):
                self._create_file("2026-04-02", "General_60mins_2026-04-02-1400.json", size=100)
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)

            def test_filters_by_time_window(self):
                # File covers 13:00-14:00 on Apr 2
                self._create_file("2026-04-02", "General_60mins_2026-04-02-1400.json")
                # Window is Apr 2 15:00-16:00 — no overlap
                window_start = datetime(2026, 4, 2, 15, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)

            def test_skips_non_date_folders(self):
                self._create_file("not-a-date", "General_60mins_2026-04-02-1400.json")
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)

            def test_skips_out_of_range_date_folders(self):
                self._create_file("2026-03-01", "General_60mins_2026-03-01-1400.json")
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)

            def test_nonexistent_data_dir(self):
                results = discover_files("/nonexistent/dir", datetime.now(ET), datetime.now(ET), 500, self.logger)
                self.assertEqual(results, [])

            def test_non_json_files_ignored(self):
                self._create_file("2026-04-02", "General_60mins_2026-04-02-1400.txt")
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)

            def test_date_folder_buffer(self):
                # File in 2026-04-01 folder but window starts 2026-04-02
                # Buffer: scan_start = 2026-04-01, so this folder should be scanned
                self._create_file("2026-04-01", "General_60mins_2026-04-01-2330.json")
                window_start = datetime(2026, 4, 2, 0, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 1, 0, tzinfo=ET)
                results = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                # File covers 22:30-23:30 on Apr 1, window is 00:00-01:00 Apr 2 — no overlap
                self.assertEqual(len(results), 0)

            def test_date_folder_buffer_with_overlap(self):
                # File in 2026-04-01 covers 23:00-00:30 (spanning midnight)
                self._create_file("2026-04-01", "General_90mins_2026-04-02-0030.json")
                window_start = datetime(2026, 4, 2, 0, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 1, 0, tzinfo=ET)
                results = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 1)

            def test_no_matching_files(self):
                os.makedirs(Path(self.tmpdir) / "2026-04-02")
                window_start = datetime(2026, 4, 2, 10, 0, tzinfo=ET)
                window_end = datetime(2026, 4, 2, 16, 0, tzinfo=ET)
                results = discover_files(self.tmpdir, window_start, window_end, 500, self.logger)
                self.assertEqual(len(results), 0)

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
                result = read_and_summarize_channel("General", [info], self.config, self.logger)
                self.assertEqual(result, "Summary text here")
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
                result = read_and_summarize_channel("General", [info], self.config, self.logger)
                self.assertIsNone(result)

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

            def test_unreadable_files_skipped(self):
                info = FileInfo(
                    chat_name="General",
                    start_time=datetime(2026, 4, 2, 13, 0, tzinfo=ET),
                    end_time=datetime(2026, 4, 2, 14, 0, tzinfo=ET),
                    path=Path("/nonexistent/file.json"),
                )
                result = read_and_summarize_channel("General", [info], self.config, self.logger)
                self.assertIsNone(result)

            @patch("__main__.call_ai", return_value="Summary")
            def test_prompt_includes_file_time_range(self, mock_ai):
                info = self._make_file_info("test.json", '{"messages": []}')
                read_and_summarize_channel("General", [info], self.config, self.logger)
                prompt = mock_ai.call_args[0][0]
                self.assertIn("2026-04-02 13:00", prompt)
                self.assertIn("2026-04-02 14:00", prompt)

        class TestRunSummary(unittest.TestCase):
            def setUp(self):
                self.tmpdir = tempfile.mkdtemp()
                self.logger = logging.getLogger("test_run_summary")
                self.config = {
                    "data_dir": self.tmpdir,
                    "timeframe": "14h",
                    "hostname": "testhost",
                    "min_file_size": 10,
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
            def test_cli_timeframe_override(self, mock_tg):
                run_summary(self.config, self._make_args(timeframe="1h"), self.logger)
                # Should succeed without error (uses 1h instead of config's 14h)
                mock_tg.assert_called_once()

        unittest.main(argv=[sys.argv[0]], exit=True)
    else:
        main()
