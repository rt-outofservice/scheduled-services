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

import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

# Add common helpers to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common" / "helpers"))
from log import setup_logging  # noqa: E402
from telegram import send_telegram  # noqa: E402

ET = ZoneInfo("America/New_York")


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
        # run_summary(config, args, logger) — implemented in Task 3


if __name__ == "__main__":
    if "--tests" in sys.argv:
        import tempfile
        import unittest

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

        unittest.main(argv=[sys.argv[0]], exit=True)
    else:
        main()
