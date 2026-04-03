#!/usr/bin/env python3
"""Install crontab entries for scheduled services.

Replaces the inline bash block in the configure-crontab playbook task.
Manages tagged blocks (# BEGIN/END managed:scheduled-*) in the user's
crontab, with PATH header and cron syntax validation.

Usage:
    install_crontab.py [--tests] svc:PREFIX [svc:PREFIX ...]
"""

import argparse
import os
import re
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch

# ── Crontab reading and block stripping ────────────────────────────

MANAGED_BEGIN_RE = re.compile(r"^# BEGIN managed:scheduled-")
MANAGED_END_RE = re.compile(r"^# END managed:scheduled-")

PATH_LINE = "PATH={home}/.local/bin:{home}/.bin:/opt/homebrew/bin:/snap/bin:/usr/local/bin:/usr/bin:/bin"

CRON_FIELD_RE = re.compile(r"^[0-9*/,-]+\s+[0-9*/,-]+\s+[0-9*/,-]+\s+[0-9*/,-]+\s+[0-9*/,-]+\s+")


def read_crontab():
    """Read the current user crontab. Returns empty string if none."""
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if result.returncode != 0:
        return ""
    return result.stdout


def strip_managed_blocks(text):
    """Remove all # BEGIN/END managed:scheduled-* blocks from crontab text."""
    lines = text.splitlines()
    out = []
    skip = False
    for line in lines:
        if MANAGED_BEGIN_RE.match(line):
            skip = True
            continue
        if MANAGED_END_RE.match(line):
            skip = False
            continue
        if not skip:
            out.append(line)
    return "\n".join(out)


# ── Managed block building ─────────────────────────────────────────


def build_managed_block(svc, cron_content):
    """Build a managed crontab block for a service.

    Returns a string with BEGIN/END markers, PATH header, and schedule lines.
    """
    home = os.path.expanduser("~")
    lines = [f"# BEGIN managed:scheduled-{svc}"]
    lines.append(PATH_LINE.format(home=home))
    for line in cron_content.splitlines():
        if line.strip():
            lines.append(line)
    lines.append(f"# END managed:scheduled-{svc}")
    return "\n".join(lines)


# ── Cron syntax validation ─────────────────────────────────────────


def validate_cron_syntax_subprocess(crontab_text):
    """Try validating crontab via crontab -n or -T. Returns True if validated."""
    for flag in ["-n", "-T"]:
        result = subprocess.run(
            ["crontab", flag, "-"],
            input=crontab_text + "\n",
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return True
        stderr = result.stderr.lower()
        if any(
            phrase in stderr for phrase in ["invalid option", "unknown option", "unrecognized option", "illegal option"]
        ):
            continue
        raise ValueError(f"crontab syntax validation failed: {result.stderr.strip()}")
    return False


def validate_cron_syntax_regex(crontab_text):
    """Validate crontab lines using regex fallback."""
    for line in crontab_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Skip variable assignments (e.g. PATH=...)
        before_space = line.split()[0] if line.split() else ""
        if "=" in before_space:
            continue
        if not CRON_FIELD_RE.match(line):
            raise ValueError(f"invalid cron syntax: {line}")


def validate_cron_syntax(crontab_text):
    """Validate crontab syntax using subprocess, falling back to regex."""
    if not validate_cron_syntax_subprocess(crontab_text):
        validate_cron_syntax_regex(crontab_text)


# ── Crontab installation ──────────────────────────────────────────


def install_crontab(crontab_text):
    """Install the given text as the user's crontab."""
    subprocess.run(
        ["crontab", "-"],
        input=crontab_text + "\n",
        check=True,
        capture_output=True,
        text=True,
    )


def clear_crontab():
    """Remove the user's crontab entirely."""
    subprocess.run(["crontab", "-r"], capture_output=True)


# ── Main workflow ──────────────────────────────────────────────────


def update_crontab(services):
    """Full crontab update workflow.

    services: list of (svc_name, env_prefix) tuples
    """
    current = read_crontab()
    cleaned = strip_managed_blocks(current)

    new_blocks = []
    for svc, prefix in services:
        enabled = os.environ.get(f"{prefix}_ENABLED", "false")
        if enabled != "true":
            continue
        cron_content = os.environ.get(f"{prefix}_CRON_SCHEDULE", "")
        if not cron_content:
            continue
        block = build_managed_block(svc, cron_content)
        new_blocks.append(block)
        print(f"{svc}: cron schedule added")

    # Combine cleaned existing + new blocks
    final = cleaned
    if new_blocks:
        new_text = "\n".join(new_blocks)
        final = final.rstrip("\n") + "\n" + new_text if final.strip() else new_text

    # Trim trailing newlines
    final = final.rstrip("\n")

    if not final.strip():
        clear_crontab()
        print("Crontab cleared (no cron services enabled)")
    else:
        validate_cron_syntax(final)
        install_crontab(final)
        print("Crontab updated")


# ── CLI ────────────────────────────────────────────────────────────


def parse_svc_args(svc_args):
    """Parse svc:PREFIX arguments into (svc, prefix) tuples."""
    pairs = []
    for arg in svc_args:
        if ":" not in arg:
            raise ValueError(f"invalid service argument '{arg}', expected svc:PREFIX")
        svc, prefix = arg.split(":", 1)
        pairs.append((svc, prefix))
    return pairs


def main():
    parser = argparse.ArgumentParser(description="Install crontab entries")
    parser.add_argument("--tests", action="store_true", help="Run embedded tests")
    parser.add_argument("services", nargs="*", help="svc:PREFIX pairs")
    args = parser.parse_args()

    if args.tests:
        sys.argv = [sys.argv[0]]
        unittest.main(module=__name__, exit=True)

    if not args.services:
        parser.error("at least one svc:PREFIX argument is required")

    pairs = parse_svc_args(args.services)
    update_crontab(pairs)


# ── Tests ──────────────────────────────────────────────────────────


class TestStripManagedBlocks(unittest.TestCase):
    def test_strips_single_block(self):
        text = (
            "# existing entry\n"
            "0 * * * * /usr/bin/something\n"
            "# BEGIN managed:scheduled-news-digest\n"
            "PATH=/usr/bin:/bin\n"
            "30 9 * * 1 /some/command\n"
            "# END managed:scheduled-news-digest\n"
            "# another entry"
        )
        result = strip_managed_blocks(text)
        self.assertNotIn("managed:scheduled", result)
        self.assertNotIn("30 9 * * 1", result)
        self.assertIn("0 * * * * /usr/bin/something", result)
        self.assertIn("# another entry", result)

    def test_strips_multiple_blocks(self):
        text = (
            "# BEGIN managed:scheduled-svc1\n"
            "line1\n"
            "# END managed:scheduled-svc1\n"
            "keep this\n"
            "# BEGIN managed:scheduled-svc2\n"
            "line2\n"
            "# END managed:scheduled-svc2"
        )
        result = strip_managed_blocks(text)
        self.assertEqual(result.strip(), "keep this")

    def test_no_blocks(self):
        text = "0 * * * * /usr/bin/something\n# comment"
        result = strip_managed_blocks(text)
        self.assertEqual(result, text)

    def test_empty_input(self):
        self.assertEqual(strip_managed_blocks(""), "")


class TestBuildManagedBlock(unittest.TestCase):
    @patch.dict(os.environ, {"HOME": "/home/testuser"})
    def test_basic_block(self):
        cron = "30 9 * * 1-5 cd ~/.scheduled-services && uv run python news_digest.py"
        block = build_managed_block("news-digest", cron)
        lines = block.splitlines()
        self.assertEqual(lines[0], "# BEGIN managed:scheduled-news-digest")
        self.assertEqual(lines[-1], "# END managed:scheduled-news-digest")
        self.assertTrue(any("PATH=" in line for line in lines))
        self.assertTrue(any("30 9" in line for line in lines))

    @patch.dict(os.environ, {"HOME": "/home/testuser"})
    def test_skips_empty_lines(self):
        block = build_managed_block("svc", "line1\n\nline2\n\n")
        lines = block.splitlines()
        content_lines = [x for x in lines if not x.startswith("#") and "PATH=" not in x]
        self.assertEqual(len(content_lines), 2)

    @patch.dict(os.environ, {"HOME": "/home/testuser"})
    def test_path_uses_home(self):
        block = build_managed_block("svc", "* * * * * cmd")
        self.assertIn("/home/testuser/.local/bin", block)


class TestValidateCronSyntaxRegex(unittest.TestCase):
    def test_valid_five_field(self):
        validate_cron_syntax_regex("30 9 * * 1-5 /usr/bin/cmd")

    def test_valid_with_ranges(self):
        validate_cron_syntax_regex("*/15 0-23 1,15 * * /cmd")

    def test_skips_comments(self):
        validate_cron_syntax_regex("# this is a comment")

    def test_skips_empty(self):
        validate_cron_syntax_regex("")

    def test_skips_variable_assignments(self):
        validate_cron_syntax_regex("PATH=/usr/bin:/bin")

    def test_invalid_line_raises(self):
        with self.assertRaises(ValueError) as ctx:
            validate_cron_syntax_regex("not a valid cron line")
        self.assertIn("invalid cron syntax", str(ctx.exception))

    def test_multiline_mixed(self):
        text = "# comment\nPATH=/usr/bin\n30 9 * * 1-5 /cmd\n0 */2 * * * /other"
        validate_cron_syntax_regex(text)

    def test_multiline_with_invalid(self):
        text = "30 9 * * 1-5 /cmd\nbadline"
        with self.assertRaises(ValueError):
            validate_cron_syntax_regex(text)


class TestMergeWorkflow(unittest.TestCase):
    """Test the full update_crontab workflow with mocked subprocess."""

    @patch("__main__.clear_crontab")
    @patch("__main__.install_crontab")
    @patch("__main__.validate_cron_syntax")
    @patch("__main__.read_crontab")
    @patch.dict(
        os.environ,
        {
            "NEWS_DIGEST_ENABLED": "true",
            "NEWS_DIGEST_CRON_SCHEDULE": "30 9 * * 1-5 cd ~/.sched-svc && uv run python news_digest.py",
        },
    )
    def test_adds_new_block(self, mock_read, mock_validate, mock_install, mock_clear):
        mock_read.return_value = "# existing\n0 * * * * /usr/bin/something"
        update_crontab([("news-digest", "NEWS_DIGEST")])
        mock_install.assert_called_once()
        installed = mock_install.call_args[0][0]
        self.assertIn("# BEGIN managed:scheduled-news-digest", installed)
        self.assertIn("0 * * * * /usr/bin/something", installed)
        mock_clear.assert_not_called()

    @patch("__main__.clear_crontab")
    @patch("__main__.install_crontab")
    @patch("__main__.validate_cron_syntax")
    @patch("__main__.read_crontab")
    @patch.dict(
        os.environ,
        {
            "NEWS_DIGEST_ENABLED": "true",
            "NEWS_DIGEST_CRON_SCHEDULE": "30 9 * * 1-5 /cmd",
        },
    )
    def test_replaces_existing_block(self, mock_read, mock_validate, mock_install, mock_clear):
        mock_read.return_value = (
            "# BEGIN managed:scheduled-news-digest\n0 0 * * * /old/cmd\n# END managed:scheduled-news-digest"
        )
        update_crontab([("news-digest", "NEWS_DIGEST")])
        installed = mock_install.call_args[0][0]
        self.assertNotIn("/old/cmd", installed)
        self.assertIn("30 9 * * 1-5 /cmd", installed)

    @patch("__main__.clear_crontab")
    @patch("__main__.install_crontab")
    @patch("__main__.read_crontab")
    @patch.dict(os.environ, {"NEWS_DIGEST_ENABLED": "false"})
    def test_disabled_service_clears_if_empty(self, mock_read, mock_install, mock_clear):
        mock_read.return_value = (
            "# BEGIN managed:scheduled-news-digest\n0 0 * * * /cmd\n# END managed:scheduled-news-digest"
        )
        update_crontab([("news-digest", "NEWS_DIGEST")])
        mock_clear.assert_called_once()
        mock_install.assert_not_called()

    @patch("__main__.clear_crontab")
    @patch("__main__.install_crontab")
    @patch("__main__.validate_cron_syntax")
    @patch("__main__.read_crontab")
    @patch.dict(os.environ, {"NEWS_DIGEST_ENABLED": "false"})
    def test_preserves_unmanaged_lines(self, mock_read, mock_validate, mock_install, mock_clear):
        mock_read.return_value = (
            "0 * * * * /usr/bin/keep\n"
            "# BEGIN managed:scheduled-news-digest\n"
            "0 0 * * * /cmd\n"
            "# END managed:scheduled-news-digest"
        )
        update_crontab([("news-digest", "NEWS_DIGEST")])
        mock_install.assert_called_once()
        installed = mock_install.call_args[0][0]
        self.assertIn("/usr/bin/keep", installed)
        self.assertNotIn("managed:scheduled", installed)

    @patch("__main__.clear_crontab")
    @patch("__main__.install_crontab")
    @patch("__main__.read_crontab")
    def test_empty_crontab_no_services(self, mock_read, mock_install, mock_clear):
        mock_read.return_value = ""
        update_crontab([])
        mock_clear.assert_called_once()
        mock_install.assert_not_called()


class TestEmptyCrontabEdgeCase(unittest.TestCase):
    @patch("__main__.clear_crontab")
    @patch("__main__.install_crontab")
    @patch("__main__.read_crontab")
    @patch.dict(os.environ, {"SVC_ENABLED": "false"})
    def test_no_existing_no_enabled_clears(self, mock_read, mock_install, mock_clear):
        mock_read.return_value = ""
        update_crontab([("svc", "SVC")])
        mock_clear.assert_called_once()
        mock_install.assert_not_called()


class TestValidateCronSyntaxSubprocess(unittest.TestCase):
    @patch("subprocess.run")
    def test_success_with_n_flag(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        result = validate_cron_syntax_subprocess("0 * * * * /cmd")
        self.assertTrue(result)

    @patch("subprocess.run")
    def test_fallthrough_on_invalid_option(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stderr="crontab: invalid option -- 'n'\n")
        result = validate_cron_syntax_subprocess("0 * * * * /cmd")
        self.assertFalse(result)

    @patch("subprocess.run")
    def test_raises_on_real_error(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stderr="bad syntax at line 1")
        with self.assertRaises(ValueError):
            validate_cron_syntax_subprocess("bad line")


class TestParseSvcArgs(unittest.TestCase):
    def test_valid_args(self):
        result = parse_svc_args(["news-digest:NEWS_DIGEST", "svc:PREFIX"])
        self.assertEqual(result, [("news-digest", "NEWS_DIGEST"), ("svc", "PREFIX")])

    def test_invalid_arg_raises(self):
        with self.assertRaises(ValueError):
            parse_svc_args(["no-colon"])


if __name__ == "__main__":
    main()
