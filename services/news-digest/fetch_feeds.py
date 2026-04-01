#!/usr/bin/env python3
"""Fetch and pre-process RSS/Atom feeds for the news-digest service.

Importable as a module:
    from fetch_feeds import fetch_all
    result = fetch_all(groups_dict, hours=24)

Also usable as CLI — reads JSON from stdin, outputs JSON to stdout:
    echo '{"tech": [{"name": "HN", "url": "https://..."}]}' | python3 fetch_feeds.py --hours 24

Output format per group:
    {"group_name": {"feeds": {"Feed Name": {"items": [...], "error": null}}}}
Each item has: title, description, link, date (ISO 8601).
"""

import json
import subprocess
import sys
import textwrap
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

# Namespace prefixes commonly found in RSS/Atom feeds
ATOM_NS = "http://www.w3.org/2005/Atom"
DC_NS = "http://purl.org/dc/elements/1.1/"

USER_AGENT = "fetch-feeds/1.0 (news-digest service)"
FETCH_TIMEOUT = 30
MAX_RESPONSE_BYTES = 10 * 1024 * 1024  # 10 MB


def parse_date(date_str: str | None) -> datetime | None:
    """Parse a date string from RSS/Atom into a timezone-aware datetime."""
    if not date_str:
        return None
    date_str = date_str.strip()

    # RFC 2822 (RSS 2.0 pubDate)
    try:
        return parsedate_to_datetime(date_str)
    except Exception:
        pass

    # ISO 8601 / RFC 3339 (Atom updated/published)
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt
        except ValueError:
            continue

    return None


def text_of(element: ET.Element | None) -> str:
    """Extract text content from an XML element, or empty string."""
    if element is None:
        return ""
    return "".join(element.itertext()).strip()


def parse_rss_items(root: ET.Element) -> list[dict]:
    """Parse RSS 2.0 <item> elements."""
    items = []
    for item in root.iter("item"):
        title = text_of(item.find("title"))
        description = text_of(item.find("description"))
        link = text_of(item.find("link"))
        date_str = text_of(item.find("pubDate")) or text_of(item.find(f"{{{DC_NS}}}date"))
        items.append(
            {
                "title": title,
                "description": description[:500] if description else "",
                "link": link,
                "date": date_str,
            }
        )
    return items


def parse_atom_entries(root: ET.Element) -> list[dict]:
    """Parse Atom <entry> elements."""
    items = []
    for entry in root.iter(f"{{{ATOM_NS}}}entry"):
        title = text_of(entry.find(f"{{{ATOM_NS}}}title"))

        # Atom content or summary
        content_el = entry.find(f"{{{ATOM_NS}}}content")
        summary_el = entry.find(f"{{{ATOM_NS}}}summary")
        description = text_of(content_el) or text_of(summary_el)

        # Atom link: <link href="..." />
        link_el = entry.find(f"{{{ATOM_NS}}}link")
        link = link_el.get("href", "") if link_el is not None else ""

        # Atom dates
        date_str = text_of(entry.find(f"{{{ATOM_NS}}}updated")) or text_of(entry.find(f"{{{ATOM_NS}}}published"))

        items.append(
            {
                "title": title,
                "description": description[:500] if description else "",
                "link": link,
                "date": date_str,
            }
        )
    return items


def parse_feed(xml_bytes: bytes) -> list[dict]:
    """Parse an RSS or Atom feed from raw XML bytes into item dicts."""
    # Reject XML with DTD declarations to prevent XML bomb / XXE attacks
    upper = xml_bytes.upper()
    if b"<!DOCTYPE" in upper or b"<!ENTITY" in upper:
        raise ET.ParseError("DTD/entity declarations are not allowed in feeds")
    root = ET.fromstring(xml_bytes)
    tag = root.tag.lower()

    # Atom feed
    if "feed" in tag:
        return parse_atom_entries(root)

    # RSS 2.0 — root is <rss>, items are under <channel>
    if "rss" in tag or root.find("channel") is not None:
        return parse_rss_items(root)

    # Try both parsers as fallback
    items = parse_rss_items(root)
    if not items:
        items = parse_atom_entries(root)

    return items


def fetch_feed(url: str) -> tuple[list[dict], str | None]:
    """Fetch a single feed URL and return (items, error)."""
    if not url.startswith(("http://", "https://")):
        return [], f"unsupported URL scheme: {url}"
    try:
        req = Request(url, headers={"User-Agent": USER_AGENT})
        with urlopen(req, timeout=FETCH_TIMEOUT) as resp:
            data = resp.read(MAX_RESPONSE_BYTES + 1)
            if len(data) > MAX_RESPONSE_BYTES:
                return [], f"response too large (>{MAX_RESPONSE_BYTES // 1024 // 1024} MB)"

        items = parse_feed(data)
        return items, None
    except URLError as e:
        return [], f"fetch error: {e.reason}"
    except ET.ParseError as e:
        return [], f"XML parse error: {e}"
    except Exception as e:
        return [], f"error: {e}"


def filter_by_hours(items: list[dict], hours: int) -> list[dict]:
    """Filter items to those published within the last N hours."""
    cutoff = datetime.now(UTC) - timedelta(hours=hours)
    filtered = []
    for item in items:
        dt = parse_date(item["date"])
        if dt is None:
            # Include items with unparseable dates — better to over-include
            filtered.append(item)
            continue
        # Normalize to UTC for comparison
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        if dt >= cutoff:
            filtered.append(item)
    return filtered


def fetch_all(groups: dict, hours: int = 24) -> dict:
    """Fetch and filter feeds for all groups.

    Args:
        groups: Dict mapping group names to lists of {"name": str, "url": str, ...} feed defs.
                Each feed def may also have "feeds" key (subcategory list) — those are flattened.
        hours: Filter items to last N hours.

    Returns:
        Dict: {group_name: {"feeds": {feed_name: {"items": [...], "error": str|None}}}}
    """
    result = {}
    for group_name, feeds in groups.items():
        group_result: dict[str, dict] = {}
        for feed_info in feeds:
            name = feed_info.get("name", "unknown")
            url = feed_info.get("url", "")
            if not url:
                group_result[name] = {"items": [], "error": "no URL provided"}
                continue

            items, error = fetch_feed(url)
            if not error:
                items = filter_by_hours(items, hours)
                # Normalize dates to ISO 8601
                for item in items:
                    dt = parse_date(item["date"])
                    if dt:
                        item["date"] = dt.isoformat()
            group_result[name] = {"items": items, "error": error}

        result[group_name] = {"feeds": group_result}

    return result


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Fetch and parse RSS feeds")
    parser.add_argument("--hours", type=int, default=24, help="Filter items to last N hours (default: 24)")
    args = parser.parse_args()

    # Read feed groups from stdin
    try:
        raw = sys.stdin.read()
        groups = json.loads(raw)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"invalid JSON input: {e}"}), file=sys.stdout)
        sys.exit(1)

    result = fetch_all(groups, hours=args.hours)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    print()  # trailing newline


if __name__ == "__main__":
    if "--tests" in sys.argv:
        import unittest
        from unittest.mock import MagicMock, patch

        _mod = sys.modules[__name__]

        class TestParseDate(unittest.TestCase):
            def test_rfc2822(self):
                dt = parse_date("Mon, 10 Mar 2025 14:30:00 +0000")
                self.assertIsNotNone(dt)
                self.assertEqual(dt.year, 2025)
                self.assertEqual(dt.month, 3)
                self.assertEqual(dt.day, 10)

            def test_iso8601_with_tz(self):
                dt = parse_date("2025-03-10T14:30:00+00:00")
                self.assertIsNotNone(dt)
                self.assertEqual(dt.year, 2025)

            def test_iso8601_with_z(self):
                dt = parse_date("2025-03-10T14:30:00Z")
                self.assertIsNotNone(dt)
                self.assertEqual(dt.tzinfo, UTC)

            def test_none_input(self):
                self.assertIsNone(parse_date(None))

            def test_empty_string(self):
                self.assertIsNone(parse_date(""))

            def test_garbage(self):
                self.assertIsNone(parse_date("not a date"))

            def test_whitespace_stripped(self):
                dt = parse_date("  2025-03-10T14:30:00Z  ")
                self.assertIsNotNone(dt)

        class TestParseRSSItems(unittest.TestCase):
            def test_basic_rss(self):
                xml = textwrap.dedent("""\
                    <rss version="2.0">
                      <channel>
                        <title>Test Feed</title>
                        <item>
                          <title>Story One</title>
                          <description>Description one</description>
                          <link>https://example.com/1</link>
                          <pubDate>Mon, 10 Mar 2025 14:30:00 +0000</pubDate>
                        </item>
                        <item>
                          <title>Story Two</title>
                          <description>Description two</description>
                          <link>https://example.com/2</link>
                          <pubDate>Tue, 11 Mar 2025 10:00:00 +0000</pubDate>
                        </item>
                      </channel>
                    </rss>
                """)
                root = ET.fromstring(xml)
                items = parse_rss_items(root)
                self.assertEqual(len(items), 2)
                self.assertEqual(items[0]["title"], "Story One")
                self.assertEqual(items[0]["link"], "https://example.com/1")
                self.assertEqual(items[1]["title"], "Story Two")

            def test_missing_fields(self):
                xml = textwrap.dedent("""\
                    <rss version="2.0">
                      <channel>
                        <item>
                          <title>No link or date</title>
                        </item>
                      </channel>
                    </rss>
                """)
                root = ET.fromstring(xml)
                items = parse_rss_items(root)
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["title"], "No link or date")
                self.assertEqual(items[0]["link"], "")
                self.assertEqual(items[0]["date"], "")

        class TestParseAtomEntries(unittest.TestCase):
            def test_basic_atom(self):
                xml = textwrap.dedent("""\
                    <feed xmlns="http://www.w3.org/2005/Atom">
                      <title>Test Atom Feed</title>
                      <entry>
                        <title>Atom Story</title>
                        <summary>Summary text</summary>
                        <link href="https://example.com/atom/1" />
                        <updated>2025-03-10T14:30:00Z</updated>
                      </entry>
                    </feed>
                """)
                root = ET.fromstring(xml)
                items = parse_atom_entries(root)
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["title"], "Atom Story")
                self.assertEqual(items[0]["description"], "Summary text")
                self.assertEqual(items[0]["link"], "https://example.com/atom/1")

            def test_content_preferred_over_summary(self):
                xml = textwrap.dedent("""\
                    <feed xmlns="http://www.w3.org/2005/Atom">
                      <entry>
                        <title>Story</title>
                        <content>Full content here</content>
                        <summary>Short summary</summary>
                        <link href="https://example.com/1" />
                        <published>2025-03-10T14:30:00Z</published>
                      </entry>
                    </feed>
                """)
                root = ET.fromstring(xml)
                items = parse_atom_entries(root)
                self.assertEqual(items[0]["description"], "Full content here")

        class TestParseFeed(unittest.TestCase):
            def test_detects_rss(self):
                xml = textwrap.dedent("""\
                    <rss version="2.0">
                      <channel>
                        <item>
                          <title>RSS Item</title>
                          <link>https://example.com</link>
                          <pubDate>Mon, 10 Mar 2025 14:30:00 +0000</pubDate>
                        </item>
                      </channel>
                    </rss>
                """)
                items = parse_feed(xml.encode())
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["title"], "RSS Item")

            def test_detects_atom(self):
                xml = textwrap.dedent("""\
                    <feed xmlns="http://www.w3.org/2005/Atom">
                      <entry>
                        <title>Atom Item</title>
                        <link href="https://example.com" />
                        <updated>2025-03-10T14:30:00Z</updated>
                      </entry>
                    </feed>
                """)
                items = parse_feed(xml.encode())
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["title"], "Atom Item")

        class TestFilterByHours(unittest.TestCase):
            def test_recent_items_kept(self):
                now = datetime.now(UTC)
                items = [
                    {"title": "recent", "date": (now - timedelta(hours=1)).isoformat()},
                    {"title": "old", "date": (now - timedelta(hours=48)).isoformat()},
                ]
                filtered = filter_by_hours(items, 24)
                self.assertEqual(len(filtered), 1)
                self.assertEqual(filtered[0]["title"], "recent")

            def test_no_date_included(self):
                items = [{"title": "no date", "date": ""}]
                filtered = filter_by_hours(items, 24)
                self.assertEqual(len(filtered), 1)

            def test_unparseable_date_included(self):
                items = [{"title": "bad date", "date": "not-a-date"}]
                filtered = filter_by_hours(items, 24)
                self.assertEqual(len(filtered), 1)

        class TestFetchFeed(unittest.TestCase):
            def test_success(self):
                xml = b"""<rss version="2.0"><channel>
                    <item><title>Test</title><link>https://x.com</link>
                    <pubDate>Mon, 10 Mar 2025 14:30:00 +0000</pubDate></item>
                    </channel></rss>"""
                mock_resp = MagicMock()
                mock_resp.read.return_value = xml
                mock_resp.__enter__ = lambda s: s
                mock_resp.__exit__ = MagicMock(return_value=False)

                with patch.object(_mod, "urlopen", return_value=mock_resp):
                    items, error = fetch_feed("https://example.com/rss")
                self.assertIsNone(error)
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["title"], "Test")

            def test_url_error(self):
                with patch.object(_mod, "urlopen", side_effect=URLError("connection refused")):
                    items, error = fetch_feed("https://bad.example.com")
                self.assertEqual(items, [])
                self.assertIn("fetch error", error)

            def test_xml_parse_error(self):
                mock_resp = MagicMock()
                mock_resp.read.return_value = b"not xml at all"
                mock_resp.__enter__ = lambda s: s
                mock_resp.__exit__ = MagicMock(return_value=False)

                with patch.object(_mod, "urlopen", return_value=mock_resp):
                    items, error = fetch_feed("https://example.com/bad")
                self.assertEqual(items, [])
                self.assertIn("xml parse error", error.lower())

            def test_unsupported_scheme(self):
                items, error = fetch_feed("ftp://example.com/feed")
                self.assertEqual(items, [])
                self.assertIn("unsupported URL scheme", error)

        class TestDescriptionTruncation(unittest.TestCase):
            def test_long_description_truncated(self):
                long_desc = "A" * 1000
                xml = f"""<rss version="2.0"><channel>
                    <item><title>T</title><description>{long_desc}</description>
                    <link>https://x.com</link></item></channel></rss>"""
                root = ET.fromstring(xml)
                items = parse_rss_items(root)
                self.assertEqual(len(items[0]["description"]), 500)

        class TestFetchAll(unittest.TestCase):
            """Test the fetch_all() module-level API."""

            def test_empty_groups(self):
                result = fetch_all({}, hours=24)
                self.assertEqual(result, {})

            def test_empty_feeds_list(self):
                result = fetch_all({"test_group": []}, hours=24)
                self.assertEqual(result, {"test_group": {"feeds": {}}})

            def test_feed_with_no_url(self):
                result = fetch_all({"g": [{"name": "bad", "url": ""}]}, hours=24)
                self.assertEqual(result["g"]["feeds"]["bad"]["error"], "no URL provided")

            def test_successful_fetch(self):
                xml = b"""<rss version="2.0"><channel>
                    <item><title>Hello</title><link>https://x.com</link>
                    <pubDate>Mon, 10 Mar 2025 14:30:00 +0000</pubDate></item>
                    </channel></rss>"""
                mock_resp = MagicMock()
                mock_resp.read.return_value = xml
                mock_resp.__enter__ = lambda s: s
                mock_resp.__exit__ = MagicMock(return_value=False)

                with patch.object(_mod, "urlopen", return_value=mock_resp):
                    result = fetch_all({"tech": [{"name": "Test", "url": "https://example.com/rss"}]}, hours=999999)
                self.assertIsNone(result["tech"]["feeds"]["Test"]["error"])
                self.assertEqual(len(result["tech"]["feeds"]["Test"]["items"]), 1)
                self.assertEqual(result["tech"]["feeds"]["Test"]["items"][0]["title"], "Hello")

            def test_multiple_groups(self):
                result = fetch_all(
                    {"g1": [{"name": "a", "url": ""}], "g2": [{"name": "b", "url": ""}]},
                    hours=24,
                )
                self.assertIn("g1", result)
                self.assertIn("g2", result)

        class TestCLIIntegration(unittest.TestCase):
            """Test the script as a subprocess."""

            def test_valid_input(self):
                script = str(Path(__file__).resolve())
                input_data = json.dumps({"test_group": []})
                result = subprocess.run(
                    [sys.executable, script, "--hours", "24"],
                    input=input_data,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(result.returncode, 0)
                output = json.loads(result.stdout)
                self.assertIn("test_group", output)
                self.assertEqual(output["test_group"]["feeds"], {})

            def test_invalid_json(self):
                script = str(Path(__file__).resolve())
                result = subprocess.run(
                    [sys.executable, script],
                    input="not json",
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(result.returncode, 1)
                output = json.loads(result.stdout)
                self.assertIn("error", output)

            def test_feed_with_no_url(self):
                script = str(Path(__file__).resolve())
                input_data = json.dumps({"g": [{"name": "bad", "url": ""}]})
                result = subprocess.run(
                    [sys.executable, script, "--hours", "24"],
                    input=input_data,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(result.returncode, 0)
                output = json.loads(result.stdout)
                self.assertEqual(output["g"]["feeds"]["bad"]["error"], "no URL provided")

        unittest.main(argv=[sys.argv[0]], exit=True)
    else:
        main()
