#!/usr/bin/env python3
"""News digest service — fetch RSS feeds, analyze with AI, send via Telegram.

Usage:
    uv run python services/news-digest/news_digest.py [--digest-names tech,security] [--tests]

Config (config.yaml):
    hostname: myhost
    feeds:
      tech:
        lang: en
        mode: detailed
        hours: 24
        feeds:
          - {name: "HN", url: "https://news.ycombinator.com/rss"}
        subcategories:
          "Cloud": ["HN"]
"""

import json
import logging
import re
import sys
import time
import unicodedata
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import yaml

# Add common helpers to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common" / "helpers"))
from ai import AIError, call_ai
from log import setup_logging
from telegram import send_telegram

# Import fetch_feeds from same directory
sys.path.insert(0, str(Path(__file__).resolve().parent))
import fetch_feeds

SPOO_ME_URL = "https://spoo.me/"
SPOO_ME_RATE_LIMIT = 5  # max requests per second


def load_config(config_path: Path) -> dict:
    """Load and validate config.yaml."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    if not config or "feeds" not in config:
        raise ValueError("Config must contain 'feeds' key")
    return config


def filter_groups_by_names(config: dict, digest_names: list[str] | None) -> dict:
    """Filter feed groups to only those matching --digest-names.

    Returns a dict of {group_name: group_config} for the selected groups.
    """
    all_groups = config.get("feeds", {})
    if not digest_names:
        return all_groups
    selected = {}
    for name in digest_names:
        name = name.strip()
        if name in all_groups:
            selected[name] = all_groups[name]
    return selected


def normalize_headline(text: str) -> str:
    """Normalize a headline for dedup comparison.

    Lowercases, strips accents, removes non-alphanumeric chars.
    """
    text = text.lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^\w ]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_sent_headlines(log_file: Path) -> set[str]:
    """Extract previously sent headlines from today's log file for dedup.

    Looks for lines containing SENT_HEADLINE: markers in the log.
    """
    sent = set()
    if not log_file.exists():
        return sent
    try:
        content = log_file.read_text(encoding="utf-8")
        for line in content.splitlines():
            if "SENT_HEADLINE:" in line:
                # Extract the headline after the marker
                idx = line.index("SENT_HEADLINE:") + len("SENT_HEADLINE:")
                headline = line[idx:].strip()
                sent.add(normalize_headline(headline))
    except OSError:
        pass
    return sent


def dedup_items(items: list[dict], sent_headlines: set[str]) -> list[dict]:
    """Remove items whose normalized title matches a previously sent headline."""
    deduped = []
    for item in items:
        normalized = normalize_headline(item.get("title", ""))
        if normalized and normalized not in sent_headlines:
            deduped.append(item)
    return deduped


def shorten_url(url: str) -> tuple[str, str | None]:
    """Shorten a URL via spoo.me POST API.

    Returns (shortened_url, error). Falls back to original URL on failure.
    """
    try:
        data = urlencode({"url": url}).encode()
        req = Request(
            SPOO_ME_URL,
            data=data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            method="POST",
        )
        with urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
            short = body.get("short_url", "")
            if short:
                return short, None
            return url, "no short_url in response"
    except Exception as e:
        return url, str(e)


def shorten_urls_in_items(items: list[dict], warnings: list[str]) -> list[dict]:
    """Shorten URLs in items via spoo.me with rate limiting.

    Modifies items in place, adding 'short_link' key.
    Appends warnings for failures.
    """
    fail_count = 0
    for i, item in enumerate(items):
        link = item.get("link", "")
        if not link:
            item["short_link"] = ""
            continue

        # Rate limit: max SPOO_ME_RATE_LIMIT per second
        if i > 0 and i % SPOO_ME_RATE_LIMIT == 0:
            time.sleep(1.0)

        short, err = shorten_url(link)
        item["short_link"] = short
        if err:
            fail_count += 1
            logging.getLogger("scheduled-services.news-digest").warning(f"URL shortener failed for {link}: {err}")

    if fail_count:
        warnings.append(f"URL shortener failed for {fail_count} links")

    return items


def build_ai_prompt(group_name: str, group_config: dict, feeds_data: dict) -> str:
    """Build the AI prompt for a feed group."""
    lang = group_config.get("lang", "en")
    mode = group_config.get("mode", "summary")
    subcategories = group_config.get("subcategories", {})

    # Collect all items across feeds
    all_items = []
    for feed_name, feed_data in feeds_data.get("feeds", {}).items():
        if feed_data.get("error"):
            continue
        for item in feed_data.get("items", []):
            item_copy = dict(item)
            item_copy["source"] = feed_name
            all_items.append(item_copy)

    if not all_items:
        return ""

    items_json = json.dumps(all_items, ensure_ascii=False, indent=2)

    if mode == "detailed":
        return (
            f"You are a news digest assistant. Analyze these news items and create a digest.\n\n"
            f"Language: {lang}\n"
            f"Group: {group_name}\n"
            f"Format: DETAILED — list individual stories with headlines and short summaries (2-3 sentences).\n"
            f"For each story include the source name and the shortened URL (use 'short_link' field if available, "
            f"otherwise 'link').\n"
            f"Cross-reference: if multiple sources cover the same story, merge them into one entry "
            f"listing all sources.\n"
            f"Max 20 stories. Max 3800 characters total.\n"
            f"Use Telegram Markdown V1 formatting (bold with *, no other markdown).\n\n"
            f"News items:\n{items_json}"
        )
    else:
        subcat_info = ""
        if subcategories:
            subcat_json = json.dumps(subcategories, ensure_ascii=False)
            subcat_info = f"\nSubcategories (organize output by these):\n{subcat_json}\n"

        return (
            f"You are a news digest assistant. Analyze these news items and create a narrative digest.\n\n"
            f"Language: {lang}\n"
            f"Group: {group_name}\n"
            f"Format: SUMMARY — write 3-5 sentence narrative paragraphs per subcategory/topic.\n"
            f"No per-story URLs needed. Attribute information to source names where relevant.\n"
            f"Max 3800 characters total.\n"
            f"Use Telegram Markdown V1 formatting (bold with *, no other markdown).\n"
            f"{subcat_info}\n"
            f"News items:\n{items_json}"
        )


def truncate_message(message: str, max_len: int = 4000) -> str:
    """Truncate message from the bottom if it exceeds max_len."""
    if len(message) <= max_len:
        return message
    # Leave room for truncation notice
    truncated = message[: max_len - 40]
    # Try to cut at last newline
    last_nl = truncated.rfind("\n")
    if last_nl > max_len // 2:
        truncated = truncated[:last_nl]
    return truncated + "\n\n... (truncated)"


def format_warnings(warnings: list[str]) -> str:
    """Format warnings list into a brief section for the telegram message."""
    if not warnings:
        return ""
    lines = ["", "Note: " + "; ".join(warnings)]
    return "\n".join(lines)


def run_digest(config_path: Path, digest_names: list[str] | None = None) -> None:
    """Main digest execution flow."""
    logger, log_file = setup_logging("news-digest")
    logger.info("Starting news-digest service")

    # Load config
    try:
        config = load_config(config_path)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        send_telegram(f"news-digest FATAL: {e}")
        return

    hostname = config.get("hostname", "")

    # Filter groups
    groups = filter_groups_by_names(config, digest_names)
    if not groups:
        logger.warning("No matching feed groups found")
        send_telegram("news-digest: no matching feed groups found", hostname=hostname)
        return

    logger.info(f"Processing groups: {list(groups.keys())}")

    # Extract previously sent headlines for dedup
    sent_headlines = extract_sent_headlines(log_file)
    logger.info(f"Found {len(sent_headlines)} previously sent headlines for dedup")

    for group_name, group_config in groups.items():
        warnings: list[str] = []
        logger.info(f"Processing group: {group_name}")

        # Build feeds list for fetch_all
        feeds_list = group_config.get("feeds", [])
        hours = group_config.get("hours", 24)

        # Fetch feeds
        try:
            feeds_result = fetch_feeds.fetch_all({group_name: feeds_list}, hours=hours)
        except Exception as e:
            logger.error(f"Feed fetch failed for {group_name}: {e}")
            send_telegram(f"news-digest FATAL: feed fetch failed for {group_name}: {e}", hostname=hostname)
            continue

        group_feeds = feeds_result.get(group_name, {"feeds": {}})

        # Collect feed errors as warnings
        for feed_name, feed_data in group_feeds.get("feeds", {}).items():
            if feed_data.get("error"):
                warnings.append(f"{feed_name} unavailable")
                logger.warning(f"Feed error for {feed_name}: {feed_data['error']}")

        # Check if all feeds failed
        total_items = sum(
            len(fd.get("items", [])) for fd in group_feeds.get("feeds", {}).values() if not fd.get("error")
        )
        if total_items == 0:
            all_errored = all(fd.get("error") for fd in group_feeds.get("feeds", {}).values())
            if all_errored and group_feeds.get("feeds"):
                logger.error(f"All feeds failed for group {group_name}")
                send_telegram(
                    f"news-digest FATAL: all feeds failed for group {group_name}",
                    hostname=hostname,
                )
            else:
                logger.info(f"No new items for group {group_name}")
            continue

        # Dedup items across all feeds in the group
        for feed_name, feed_data in group_feeds.get("feeds", {}).items():
            if feed_data.get("error"):
                continue
            original_count = len(feed_data["items"])
            feed_data["items"] = dedup_items(feed_data["items"], sent_headlines)
            deduped_count = original_count - len(feed_data["items"])
            if deduped_count:
                logger.info(f"Deduped {deduped_count} items from {feed_name}")

        # Recount after dedup
        total_items = sum(
            len(fd.get("items", [])) for fd in group_feeds.get("feeds", {}).values() if not fd.get("error")
        )
        if total_items == 0:
            logger.info(f"No new items after dedup for group {group_name}")
            continue

        # URL shortening for detailed mode
        mode = group_config.get("mode", "summary")
        if mode == "detailed":
            for _feed_name, feed_data in group_feeds.get("feeds", {}).items():
                if not feed_data.get("error"):
                    shorten_urls_in_items(feed_data["items"], warnings)

        # Build AI prompt
        prompt = build_ai_prompt(group_name, group_config, group_feeds)
        if not prompt:
            logger.info(f"No content for AI prompt for group {group_name}")
            continue

        # Call AI
        try:
            digest_text = call_ai(prompt)
        except AIError as e:
            logger.error(f"AI call failed for group {group_name}: {e}")
            send_telegram(f"news-digest FATAL: AI call failed for group {group_name}: {e}", hostname=hostname)
            continue

        # Truncate AI text first, then append warnings so warnings survive
        # Account for hostname prefix added by send_telegram
        warning_text = format_warnings(warnings)
        prefix_len = len(f"*{hostname}* — ") if hostname else 0
        digest_text = truncate_message(digest_text, max_len=4000 - len(warning_text) - prefix_len)
        digest_text += warning_text

        # Send via telegram
        try:
            sent_ok = send_telegram(digest_text, hostname=hostname)
        except Exception as e:
            logger.error(f"Telegram send failed for group {group_name}: {e}")
            sent_ok = False

        # Log sent headlines for future dedup (only if actually delivered)
        if sent_ok:
            for _feed_name, feed_data in group_feeds.get("feeds", {}).items():
                if feed_data.get("error"):
                    continue
                for item in feed_data.get("items", []):
                    title = item.get("title", "")
                    if title:
                        logger.info(f"SENT_HEADLINE: {title}")
        else:
            logger.warning(f"Skipping SENT_HEADLINE markers for group {group_name} — will retry next run")

        logger.info(f"Completed group: {group_name}")

    logger.info("News-digest service finished")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="News digest service")
    parser.add_argument(
        "--digest-names",
        type=str,
        default=None,
        help="Comma-separated list of digest group names to process (default: all)",
    )
    parser.add_argument("--tests", action="store_true", help="Run tests and exit")
    args = parser.parse_args()

    if args.tests:
        # Run tests (handled in __main__ block below)
        return

    config_path = Path(__file__).resolve().parent / "config.yaml"
    digest_names = [n.strip() for n in args.digest_names.split(",")] if args.digest_names else None
    run_digest(config_path, digest_names)


if __name__ == "__main__":
    if "--tests" in sys.argv:
        import unittest
        from unittest.mock import MagicMock, patch

        _mod = sys.modules[__name__]

        class TestLoadConfig(unittest.TestCase):
            def test_valid_config(self):
                import tempfile

                with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                    yaml.dump({"hostname": "test", "feeds": {"g1": {"lang": "en"}}}, f)
                    f.flush()
                    config = load_config(Path(f.name))
                self.assertEqual(config["hostname"], "test")
                self.assertIn("g1", config["feeds"])
                Path(f.name).unlink()

            def test_missing_config(self):
                with self.assertRaises(FileNotFoundError):
                    load_config(Path("/nonexistent/config.yaml"))

            def test_missing_feeds_key(self):
                import tempfile

                with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                    yaml.dump({"hostname": "test"}, f)
                    f.flush()
                    with self.assertRaises(ValueError):
                        load_config(Path(f.name))
                Path(f.name).unlink()

        class TestFilterGroupsByNames(unittest.TestCase):
            def setUp(self):
                self.config = {
                    "feeds": {
                        "tech": {"lang": "en"},
                        "security": {"lang": "en"},
                        "finance": {"lang": "ru"},
                    }
                }

            def test_no_filter_returns_all(self):
                result = filter_groups_by_names(self.config, None)
                self.assertEqual(len(result), 3)

            def test_filter_single(self):
                result = filter_groups_by_names(self.config, ["tech"])
                self.assertEqual(len(result), 1)
                self.assertIn("tech", result)

            def test_filter_multiple(self):
                result = filter_groups_by_names(self.config, ["tech", "security"])
                self.assertEqual(len(result), 2)

            def test_filter_nonexistent(self):
                result = filter_groups_by_names(self.config, ["nonexistent"])
                self.assertEqual(len(result), 0)

            def test_filter_with_spaces(self):
                result = filter_groups_by_names(self.config, [" tech "])
                self.assertEqual(len(result), 1)

        class TestNormalizeHeadline(unittest.TestCase):
            def test_basic(self):
                self.assertEqual(normalize_headline("Hello World"), "hello world")

            def test_special_chars(self):
                self.assertEqual(normalize_headline("Hello, World!"), "hello world")

            def test_accents(self):
                self.assertEqual(normalize_headline("cafe\u0301"), "cafe")

            def test_extra_spaces(self):
                self.assertEqual(normalize_headline("  hello   world  "), "hello world")

            def test_cyrillic(self):
                self.assertEqual(normalize_headline("Новости дня"), "новости дня")

            def test_cyrillic_dedup_not_empty(self):
                # Non-Latin headlines must not normalize to empty string
                self.assertTrue(len(normalize_headline("Путин подписал указ")) > 0)

            def test_empty(self):
                self.assertEqual(normalize_headline(""), "")

        class TestExtractSentHeadlines(unittest.TestCase):
            def test_extracts_headlines(self):
                import tempfile

                with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
                    f.write("2026-04-01 10:00:00 [INFO] SENT_HEADLINE: Big News Today\n")
                    f.write("2026-04-01 10:00:01 [INFO] SENT_HEADLINE: Another Story\n")
                    f.write("2026-04-01 10:00:02 [INFO] Processing group: tech\n")
                    f.flush()
                    result = extract_sent_headlines(Path(f.name))
                self.assertEqual(len(result), 2)
                self.assertIn(normalize_headline("Big News Today"), result)
                self.assertIn(normalize_headline("Another Story"), result)
                Path(f.name).unlink()

            def test_missing_file(self):
                result = extract_sent_headlines(Path("/nonexistent.log"))
                self.assertEqual(len(result), 0)

        class TestDedupItems(unittest.TestCase):
            def test_removes_duplicates(self):
                items = [
                    {"title": "Big News Today"},
                    {"title": "Something New"},
                ]
                sent = {normalize_headline("Big News Today")}
                result = dedup_items(items, sent)
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0]["title"], "Something New")

            def test_no_duplicates(self):
                items = [{"title": "Fresh Story"}]
                sent = {normalize_headline("Old Story")}
                result = dedup_items(items, sent)
                self.assertEqual(len(result), 1)

            def test_empty_title_excluded(self):
                items = [{"title": ""}]
                result = dedup_items(items, set())
                self.assertEqual(len(result), 0)

        class TestShortenUrl(unittest.TestCase):
            @patch.object(_mod, "urlopen")
            def test_success(self, mock_urlopen):
                mock_resp = MagicMock()
                mock_resp.read.return_value = json.dumps({"short_url": "https://spoo.me/abc"}).encode()
                mock_resp.__enter__ = lambda s: s
                mock_resp.__exit__ = MagicMock(return_value=False)
                mock_urlopen.return_value = mock_resp

                short, err = shorten_url("https://example.com/long")
                self.assertEqual(short, "https://spoo.me/abc")
                self.assertIsNone(err)

            @patch.object(_mod, "urlopen")
            def test_api_error(self, mock_urlopen):
                mock_urlopen.side_effect = URLError("timeout")
                short, err = shorten_url("https://example.com/long")
                self.assertEqual(short, "https://example.com/long")
                self.assertIsNotNone(err)

            @patch.object(_mod, "urlopen")
            def test_no_short_url_in_response(self, mock_urlopen):
                mock_resp = MagicMock()
                mock_resp.read.return_value = json.dumps({}).encode()
                mock_resp.__enter__ = lambda s: s
                mock_resp.__exit__ = MagicMock(return_value=False)
                mock_urlopen.return_value = mock_resp

                short, err = shorten_url("https://example.com/long")
                self.assertEqual(short, "https://example.com/long")
                self.assertIn("no short_url", err)

        class TestShortenUrlsInItems(unittest.TestCase):
            @patch.object(_mod, "shorten_url")
            def test_shortens_all(self, mock_shorten):
                mock_shorten.return_value = ("https://short.url/x", None)
                items = [{"link": "https://example.com/1"}, {"link": "https://example.com/2"}]
                warnings = []
                shorten_urls_in_items(items, warnings)
                self.assertEqual(items[0]["short_link"], "https://short.url/x")
                self.assertEqual(items[1]["short_link"], "https://short.url/x")
                self.assertEqual(len(warnings), 0)

            @patch.object(_mod, "shorten_url")
            def test_collects_warnings(self, mock_shorten):
                mock_shorten.return_value = ("https://example.com/1", "timeout")
                items = [{"link": "https://example.com/1"}]
                warnings = []
                shorten_urls_in_items(items, warnings)
                self.assertEqual(items[0]["short_link"], "https://example.com/1")
                self.assertEqual(len(warnings), 1)

            @patch.object(_mod, "shorten_url")
            def test_empty_link_skipped(self, mock_shorten):
                items = [{"link": ""}]
                warnings = []
                shorten_urls_in_items(items, warnings)
                self.assertEqual(items[0]["short_link"], "")
                mock_shorten.assert_not_called()

        class TestBuildAIPrompt(unittest.TestCase):
            def test_detailed_mode(self):
                group_config = {"lang": "en", "mode": "detailed"}
                feeds_data = {
                    "feeds": {
                        "HN": {
                            "items": [{"title": "Story", "link": "https://x.com", "short_link": "https://s.co/x"}],
                            "error": None,
                        }
                    }
                }
                prompt = build_ai_prompt("tech", group_config, feeds_data)
                self.assertIn("DETAILED", prompt)
                self.assertIn("Language: en", prompt)
                self.assertIn("Story", prompt)

            def test_summary_mode_with_subcategories(self):
                group_config = {
                    "lang": "ru",
                    "mode": "summary",
                    "subcategories": {"Cloud": ["HN"]},
                }
                feeds_data = {
                    "feeds": {
                        "HN": {
                            "items": [{"title": "Story", "link": "https://x.com"}],
                            "error": None,
                        }
                    }
                }
                prompt = build_ai_prompt("tech", group_config, feeds_data)
                self.assertIn("SUMMARY", prompt)
                self.assertIn("Language: ru", prompt)
                self.assertIn("Cloud", prompt)

            def test_empty_items_returns_empty(self):
                group_config = {"lang": "en", "mode": "detailed"}
                feeds_data = {"feeds": {"HN": {"items": [], "error": None}}}
                prompt = build_ai_prompt("tech", group_config, feeds_data)
                self.assertEqual(prompt, "")

            def test_errored_feeds_skipped(self):
                group_config = {"lang": "en", "mode": "detailed"}
                feeds_data = {"feeds": {"HN": {"items": [], "error": "fetch error"}}}
                prompt = build_ai_prompt("tech", group_config, feeds_data)
                self.assertEqual(prompt, "")

        class TestTruncateMessage(unittest.TestCase):
            def test_short_message(self):
                self.assertEqual(truncate_message("hello", 4000), "hello")

            def test_long_message_truncated(self):
                msg = "a" * 5000
                result = truncate_message(msg, 4000)
                self.assertLessEqual(len(result), 4000)
                self.assertIn("truncated", result)

            def test_truncates_at_newline(self):
                msg = "line1\n" * 800
                result = truncate_message(msg, 100)
                self.assertLessEqual(len(result), 100)
                self.assertIn("truncated", result)

        class TestFormatWarnings(unittest.TestCase):
            def test_no_warnings(self):
                self.assertEqual(format_warnings([]), "")

            def test_single_warning(self):
                result = format_warnings(["2 feeds unavailable"])
                self.assertIn("Note:", result)
                self.assertIn("2 feeds unavailable", result)

            def test_multiple_warnings(self):
                result = format_warnings(["2 feeds unavailable", "URL shortener failed for 3 links"])
                self.assertIn("; ", result)

        class TestRunDigest(unittest.TestCase):
            """Integration tests for the main run_digest flow."""

            @patch.object(_mod, "send_telegram")
            @patch.object(_mod, "call_ai", return_value="*Tech Digest*\n\nStory about testing.")
            @patch("fetch_feeds.fetch_all")
            @patch.object(_mod, "setup_logging")
            def test_full_flow(self, mock_logging, mock_fetch, mock_ai, mock_telegram):
                import tempfile

                # Set up mock logging
                mock_logger = MagicMock(spec=logging.Logger)
                mock_log_file = Path(tempfile.mktemp(suffix=".log"))
                mock_log_file.write_text("")
                mock_logging.return_value = (mock_logger, mock_log_file)

                # Write config
                config_dir = tempfile.mkdtemp()
                config_path = Path(config_dir) / "config.yaml"
                config = {
                    "hostname": "testhost",
                    "feeds": {
                        "tech": {
                            "lang": "en",
                            "mode": "summary",
                            "hours": 24,
                            "feeds": [{"name": "HN", "url": "https://example.com/rss"}],
                        }
                    },
                }
                with open(config_path, "w") as f:
                    yaml.dump(config, f)

                # Mock fetch_all result
                mock_fetch.return_value = {
                    "tech": {
                        "feeds": {
                            "HN": {
                                "items": [
                                    {"title": "Test Story", "link": "https://x.com", "date": "2026-04-01T10:00:00Z"}
                                ],
                                "error": None,
                            }
                        }
                    }
                }

                run_digest(config_path)

                mock_ai.assert_called_once()
                mock_telegram.assert_called()
                # Check headline was logged
                mock_logger.info.assert_any_call("SENT_HEADLINE: Test Story")

                mock_log_file.unlink(missing_ok=True)
                import shutil

                shutil.rmtree(config_dir)

            @patch.object(_mod, "send_telegram")
            @patch.object(_mod, "setup_logging")
            def test_missing_config(self, mock_logging, mock_telegram):
                import tempfile

                mock_logger = MagicMock(spec=logging.Logger)
                mock_log_file = Path(tempfile.mktemp(suffix=".log"))
                mock_log_file.write_text("")
                mock_logging.return_value = (mock_logger, mock_log_file)

                run_digest(Path("/nonexistent/config.yaml"))

                mock_logger.error.assert_called()
                mock_telegram.assert_called_once()
                self.assertIn("FATAL", mock_telegram.call_args[0][0])
                mock_log_file.unlink(missing_ok=True)

            @patch.object(_mod, "send_telegram")
            @patch.object(_mod, "call_ai", return_value="Digest text")
            @patch("fetch_feeds.fetch_all")
            @patch.object(_mod, "setup_logging")
            def test_digest_names_filtering(self, mock_logging, mock_fetch, mock_ai, mock_telegram):
                import tempfile

                mock_logger = MagicMock(spec=logging.Logger)
                mock_log_file = Path(tempfile.mktemp(suffix=".log"))
                mock_log_file.write_text("")
                mock_logging.return_value = (mock_logger, mock_log_file)

                config_dir = tempfile.mkdtemp()
                config_path = Path(config_dir) / "config.yaml"
                config = {
                    "hostname": "testhost",
                    "feeds": {
                        "tech": {
                            "lang": "en",
                            "mode": "summary",
                            "hours": 24,
                            "feeds": [{"name": "HN", "url": "https://example.com/rss"}],
                        },
                        "security": {
                            "lang": "en",
                            "mode": "detailed",
                            "hours": 12,
                            "feeds": [{"name": "Sec", "url": "https://example.com/sec"}],
                        },
                    },
                }
                with open(config_path, "w") as f:
                    yaml.dump(config, f)

                mock_fetch.return_value = {
                    "tech": {
                        "feeds": {
                            "HN": {
                                "items": [
                                    {"title": "Tech Story", "link": "https://x.com", "date": "2026-04-01T10:00:00Z"}
                                ],
                                "error": None,
                            }
                        }
                    }
                }

                run_digest(config_path, digest_names=["tech"])

                # Only tech group processed
                mock_fetch.assert_called_once()
                call_args = mock_fetch.call_args[0][0]
                self.assertIn("tech", call_args)
                self.assertNotIn("security", call_args)

                mock_log_file.unlink(missing_ok=True)
                import shutil

                shutil.rmtree(config_dir)

            @patch.object(_mod, "send_telegram")
            @patch("fetch_feeds.fetch_all")
            @patch.object(_mod, "setup_logging")
            def test_all_feeds_failed_sends_fatal(self, mock_logging, mock_fetch, mock_telegram):
                import tempfile

                mock_logger = MagicMock(spec=logging.Logger)
                mock_log_file = Path(tempfile.mktemp(suffix=".log"))
                mock_log_file.write_text("")
                mock_logging.return_value = (mock_logger, mock_log_file)

                config_dir = tempfile.mkdtemp()
                config_path = Path(config_dir) / "config.yaml"
                config = {
                    "hostname": "testhost",
                    "feeds": {
                        "tech": {
                            "lang": "en",
                            "mode": "summary",
                            "hours": 24,
                            "feeds": [{"name": "HN", "url": "https://example.com/rss"}],
                        }
                    },
                }
                with open(config_path, "w") as f:
                    yaml.dump(config, f)

                mock_fetch.return_value = {"tech": {"feeds": {"HN": {"items": [], "error": "connection refused"}}}}

                run_digest(config_path)

                # Should send FATAL telegram
                fatal_calls = [c for c in mock_telegram.call_args_list if "FATAL" in str(c)]
                self.assertGreater(len(fatal_calls), 0)

                mock_log_file.unlink(missing_ok=True)
                import shutil

                shutil.rmtree(config_dir)

        unittest.main(argv=[sys.argv[0]], exit=True)
    else:
        main()
