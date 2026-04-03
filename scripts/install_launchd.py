#!/usr/bin/env python3
"""Install launchd agents for scheduled services.

Replaces the inline bash block in the configure-launchd playbook task.
Reads YAML schedule definitions from environment variables and generates
LaunchAgent plist files from a template.

Usage:
    install_launchd.py --template PATH --agents-dir PATH --prefix-tag TAG \\
        svc:PREFIX [svc:PREFIX ...]
    install_launchd.py --tests
"""

import argparse
import glob
import os
import subprocess
import sys
import unittest
from itertools import product
from pathlib import Path
from unittest.mock import patch

import yaml

# ── Schedule expansion ──────────────────────────────────────────────

CALENDAR_KEYS = ["Minute", "Hour", "Weekday", "Day", "Month"]


def expand_schedule(entries):
    """Expand schedule entries with array values into cartesian-product dicts.

    Each entry is a dict like {"Minute": 0, "Hour": 9, "Weekday": [1,2,3,4,5]}.
    Array values are expanded into individual entries via cartesian product.
    Returns a flat list of dicts with only scalar values.
    """
    results = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise RuntimeError(f"schedule entry is not a mapping: {entry!r}")
        arrays = {}
        scalars = {}
        for k in CALENDAR_KEYS:
            if k not in entry:
                continue
            v = entry[k]
            if isinstance(v, list):
                arrays[k] = v
            else:
                scalars[k] = v

        if not arrays:
            results.append(scalars)
            continue

        keys = list(arrays.keys())
        for combo in product(*(arrays[k] for k in keys)):
            d = dict(scalars)
            for k, v in zip(keys, combo, strict=True):
                d[k] = v
            results.append(d)
    return results


def schedule_to_xml(entries):
    """Convert expanded schedule entries to StartCalendarInterval XML."""
    expanded = expand_schedule(entries)
    lines = ["    <array>"]
    for entry in expanded:
        lines.append("        <dict>")
        for k in CALENDAR_KEYS:
            if k in entry:
                lines.append(f"            <key>{k}</key>")
                lines.append(f"            <integer>{entry[k]}</integer>")
        lines.append("        </dict>")
    lines.append("    </array>")
    return "\n".join(lines)


# ── Plist generation ────────────────────────────────────────────────

PATH_EXPORT = 'export PATH="$HOME/.local/bin:$HOME/.bin:/opt/homebrew/bin:/snap/bin:/usr/local/bin:/usr/bin:/bin:$PATH"'


def xml_escape(text):
    """Escape text for XML content."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def render_plist(template, label, command, schedule_xml, working_dir, log_dir):
    """Render a plist from template with placeholder substitution."""
    full_command = f"{PATH_EXPORT}; {command}"
    plist = template.replace("{{LABEL}}", label)
    plist = plist.replace("{{WORKING_DIR}}", working_dir)
    plist = plist.replace("{{LOG_DIR}}", log_dir)
    plist = plist.replace("{{COMMAND}}", xml_escape(full_command))

    # Replace SCHEDULE placeholder (may have leading whitespace on its line)
    result_lines = []
    for line in plist.splitlines():
        stripped = line.strip()
        if stripped == "{{SCHEDULE}}":
            result_lines.append(schedule_xml)
        else:
            result_lines.append(line)
    plist = "\n".join(result_lines)
    return plist


# ── Agent lifecycle ─────────────────────────────────────────────────


def remove_existing_agents(svc, prefix_tag, agents_dir):
    """Remove all existing launchd agents for a service."""
    uid = os.getuid()
    pattern = os.path.join(agents_dir, f"{prefix_tag}.{svc}*.plist")
    for plist_path in glob.glob(pattern):
        label = Path(plist_path).stem
        subprocess.run(
            ["launchctl", "bootout", f"gui/{uid}/{label}"],
            capture_output=True,
        )
        os.remove(plist_path)


def validate_plist(plist_path, label):
    """Validate a plist file with plutil. Raises on failure."""
    result = subprocess.run(["plutil", "-lint", plist_path], capture_output=True, text=True)
    if result.returncode != 0:
        os.remove(plist_path)
        raise RuntimeError(f"generated plist {label} is invalid: {result.stderr}")


def bootstrap_agent(plist_path, label):
    """Bootstrap a launchd agent, with bootout-retry on conflict."""
    uid = os.getuid()
    domain = f"gui/{uid}"
    result = subprocess.run(["launchctl", "bootstrap", domain, plist_path], capture_output=True, text=True)
    if result.returncode != 0:
        subprocess.run(["launchctl", "bootout", f"{domain}/{label}"], capture_output=True)
        result = subprocess.run(
            ["launchctl", "bootstrap", domain, plist_path],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"failed to bootstrap agent {label}: {result.stderr}")


def install_plist(plist_content, label, agents_dir):
    """Write plist, validate, and bootstrap the agent."""
    plist_path = os.path.join(agents_dir, f"{label}.plist")
    Path(plist_path).write_text(plist_content)
    validate_plist(plist_path, label)
    bootstrap_agent(plist_path, label)


# ── Service installer ──────────────────────────────────────────────


def install_service(svc, prefix, template, prefix_tag, agents_dir):
    """Install or remove launchd agents for one service."""
    enabled = os.environ.get(f"{prefix}_ENABLED", "false")
    home = os.path.expanduser("~")
    working_dir = os.path.join(home, ".scheduled-services")

    remove_existing_agents(svc, prefix_tag, agents_dir)

    if enabled != "true":
        return

    schedule_raw = os.environ.get(f"{prefix}_LAUNCHD_SCHEDULE", "")
    if not schedule_raw:
        print(f"{svc}: no LAUNCHD_SCHEDULE — skipping")
        return

    jobs = yaml.safe_load(schedule_raw)
    if not isinstance(jobs, list) or len(jobs) == 0:
        raise RuntimeError(f"{svc} LAUNCHD_SCHEDULE has no jobs")

    log_dir = os.path.join(home, ".logs", "scheduled-services", svc)
    os.makedirs(log_dir, exist_ok=True)

    for i, job in enumerate(jobs):
        if not isinstance(job, dict):
            raise RuntimeError(f"{svc} job {i} is not a mapping (got {type(job).__name__})")
        command = job.get("command")
        if not command:
            raise RuntimeError(f"{svc} job {i} missing 'command' field")

        schedule = job.get("schedule")
        if not schedule or len(schedule) == 0:
            raise RuntimeError(f"{svc} job {i} has no schedule entries")

        schedule_xml = schedule_to_xml(schedule)

        # Label: no suffix for single-job services, numbered for multi-job
        label = f"{prefix_tag}.{svc}"
        if len(jobs) > 1:
            label = f"{label}.{i + 1}"

        plist_content = render_plist(template, label, command, schedule_xml, working_dir, log_dir)
        install_plist(plist_content, label, agents_dir)
        print(f"{svc}: agent {label} installed ({len(schedule)} schedule entries)")


# ── CLI ─────────────────────────────────────────────────────────────


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
    parser = argparse.ArgumentParser(description="Install launchd agents")
    parser.add_argument("--tests", action="store_true", help="Run embedded tests")
    parser.add_argument("--template", help="Path to plist template")
    parser.add_argument("--agents-dir", help="Path to LaunchAgents directory")
    parser.add_argument("--prefix-tag", help="Plist label prefix")
    parser.add_argument("services", nargs="*", help="svc:PREFIX pairs")
    args = parser.parse_args()

    if args.tests:
        sys.argv = [sys.argv[0]]
        unittest.main(module=__name__, exit=True)

    if not args.template or not args.agents_dir or not args.prefix_tag:
        parser.error("--template, --agents-dir, and --prefix-tag are required")
    if not args.services:
        parser.error("at least one svc:PREFIX argument is required")

    template = Path(args.template).read_text()
    os.makedirs(args.agents_dir, exist_ok=True)

    pairs = parse_svc_args(args.services)
    for svc, prefix in pairs:
        install_service(svc, prefix, template, args.prefix_tag, args.agents_dir)

    print("LaunchAgent configuration complete")


# ── Tests ───────────────────────────────────────────────────────────


class TestScheduleExpansion(unittest.TestCase):
    def test_scalar_only(self):
        entries = [{"Minute": 0, "Hour": 9}]
        result = expand_schedule(entries)
        self.assertEqual(result, [{"Minute": 0, "Hour": 9}])

    def test_array_expansion(self):
        entries = [{"Minute": 0, "Hour": 9, "Weekday": [1, 2, 3]}]
        result = expand_schedule(entries)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], {"Minute": 0, "Hour": 9, "Weekday": 1})
        self.assertEqual(result[1], {"Minute": 0, "Hour": 9, "Weekday": 2})
        self.assertEqual(result[2], {"Minute": 0, "Hour": 9, "Weekday": 3})

    def test_mixed_scalar_and_array(self):
        entries = [
            {"Minute": 30, "Hour": [8, 17], "Weekday": [1, 5]},
        ]
        result = expand_schedule(entries)
        self.assertEqual(len(result), 4)
        expected = [
            {"Minute": 30, "Hour": 8, "Weekday": 1},
            {"Minute": 30, "Hour": 8, "Weekday": 5},
            {"Minute": 30, "Hour": 17, "Weekday": 1},
            {"Minute": 30, "Hour": 17, "Weekday": 5},
        ]
        self.assertEqual(result, expected)

    def test_multiple_entries(self):
        entries = [
            {"Minute": 0, "Hour": 9},
            {"Minute": 30, "Hour": 14},
        ]
        result = expand_schedule(entries)
        self.assertEqual(len(result), 2)

    def test_empty_entries(self):
        self.assertEqual(expand_schedule([]), [])

    def test_ignores_unknown_keys(self):
        entries = [{"Minute": 0, "Hour": 9, "Bogus": 42}]
        result = expand_schedule(entries)
        self.assertEqual(result, [{"Minute": 0, "Hour": 9}])

    def test_non_dict_entry_raises(self):
        with self.assertRaises(RuntimeError) as ctx:
            expand_schedule(["0 9 * * *"])
        self.assertIn("not a mapping", str(ctx.exception))


class TestScheduleToXml(unittest.TestCase):
    def test_basic_xml(self):
        entries = [{"Minute": 0, "Hour": 9}]
        xml = schedule_to_xml(entries)
        self.assertIn("<array>", xml)
        self.assertIn("</array>", xml)
        self.assertIn("<key>Minute</key>", xml)
        self.assertIn("<integer>0</integer>", xml)
        self.assertIn("<key>Hour</key>", xml)
        self.assertIn("<integer>9</integer>", xml)

    def test_key_ordering(self):
        entries = [{"Hour": 9, "Minute": 0, "Weekday": 1}]
        xml = schedule_to_xml(entries)
        minute_pos = xml.index("Minute")
        hour_pos = xml.index("Hour")
        weekday_pos = xml.index("Weekday")
        self.assertLess(minute_pos, hour_pos)
        self.assertLess(hour_pos, weekday_pos)

    def test_array_expansion_in_xml(self):
        entries = [{"Minute": 0, "Weekday": [1, 2]}]
        xml = schedule_to_xml(entries)
        self.assertEqual(xml.count("<dict>"), 2)
        self.assertEqual(xml.count("</dict>"), 2)


class TestXmlEscape(unittest.TestCase):
    def test_ampersand(self):
        self.assertEqual(xml_escape("a & b"), "a &amp; b")

    def test_angle_brackets(self):
        self.assertEqual(xml_escape("a < b > c"), "a &lt; b &gt; c")

    def test_no_special_chars(self):
        self.assertEqual(xml_escape("hello"), "hello")

    def test_combined(self):
        self.assertEqual(xml_escape("a & <b>"), "a &amp; &lt;b&gt;")


class TestRenderPlist(unittest.TestCase):
    TEMPLATE = (
        '<?xml version="1.0"?>\n'
        "<dict>\n"
        "    <key>Label</key>\n"
        "    <string>{{LABEL}}</string>\n"
        "    <key>ProgramArguments</key>\n"
        "    <array>\n"
        "        <string>/bin/bash</string>\n"
        "        <string>-c</string>\n"
        "        <string>{{COMMAND}}</string>\n"
        "    </array>\n"
        "    <key>WorkingDirectory</key>\n"
        "    <string>{{WORKING_DIR}}</string>\n"
        "    <key>StartCalendarInterval</key>\n"
        "    {{SCHEDULE}}\n"
        "    <key>StandardOutPath</key>\n"
        "    <string>{{LOG_DIR}}/stdout.log</string>\n"
        "</dict>"
    )

    def test_placeholder_substitution(self):
        xml = "    <array><dict></dict></array>"
        result = render_plist(
            self.TEMPLATE,
            "com.test.svc",
            "echo hello",
            xml,
            "/home/user/.scheduled-services",
            "/home/user/.logs/svc",
        )
        self.assertIn("<string>com.test.svc</string>", result)
        self.assertIn("/home/user/.scheduled-services", result)
        self.assertIn("/home/user/.logs/svc/stdout.log", result)

    def test_command_xml_escaped(self):
        result = render_plist(
            self.TEMPLATE,
            "label",
            'echo "a & b"',
            "    <array/>",
            "/w",
            "/l",
        )
        self.assertIn("&amp;", result)
        self.assertNotIn("{{COMMAND}}", result)

    def test_schedule_replaced(self):
        sched_xml = "    <array>\n        <dict></dict>\n    </array>"
        result = render_plist(self.TEMPLATE, "l", "cmd", sched_xml, "/w", "/l")
        self.assertNotIn("{{SCHEDULE}}", result)
        self.assertIn("<array>", result)

    def test_path_export_prepended(self):
        result = render_plist(self.TEMPLATE, "l", "run.sh", "    <array/>", "/w", "/l")
        self.assertIn("export PATH=", result)
        self.assertIn("; run.sh", result)


class TestLabelNaming(unittest.TestCase):
    """Verify single-job vs multi-job label naming via install_service."""

    @patch("__main__.install_plist")
    @patch("__main__.remove_existing_agents")
    @patch.dict(
        os.environ,
        {
            "TEST_ENABLED": "true",
            "TEST_LAUNCHD_SCHEDULE": yaml.dump([{"command": "cmd1", "schedule": [{"Minute": 0}]}]),
        },
    )
    def test_single_job_no_suffix(self, mock_remove, mock_install):
        tpl = "{{LABEL}} {{COMMAND}} {{WORKING_DIR}} {{LOG_DIR}}\n    {{SCHEDULE}}"
        install_service("mysvc", "TEST", tpl, "com.test", "/tmp/agents")
        label = mock_install.call_args[0][1]
        self.assertEqual(label, "com.test.mysvc")

    @patch("__main__.install_plist")
    @patch("__main__.remove_existing_agents")
    @patch.dict(
        os.environ,
        {
            "TEST_ENABLED": "true",
            "TEST_LAUNCHD_SCHEDULE": yaml.dump(
                [
                    {"command": "cmd1", "schedule": [{"Minute": 0}]},
                    {"command": "cmd2", "schedule": [{"Minute": 30}]},
                ]
            ),
        },
    )
    def test_multi_job_numbered(self, mock_remove, mock_install):
        tpl = "{{LABEL}} {{COMMAND}} {{WORKING_DIR}} {{LOG_DIR}}\n    {{SCHEDULE}}"
        install_service("mysvc", "TEST", tpl, "com.test", "/tmp/agents")
        labels = [c[0][1] for c in mock_install.call_args_list]
        self.assertEqual(labels, ["com.test.mysvc.1", "com.test.mysvc.2"])


class TestErrorCases(unittest.TestCase):
    @patch("__main__.remove_existing_agents")
    @patch.dict(
        os.environ,
        {
            "TEST_ENABLED": "true",
            "TEST_LAUNCHD_SCHEDULE": yaml.dump([{"schedule": [{"Minute": 0}]}]),
        },
    )
    def test_missing_command_raises(self, mock_remove):
        with self.assertRaises(RuntimeError) as ctx:
            install_service("svc", "TEST", "tpl", "tag", "/tmp")
        self.assertIn("missing 'command'", str(ctx.exception))

    @patch("__main__.remove_existing_agents")
    @patch.dict(
        os.environ,
        {
            "TEST_ENABLED": "true",
            "TEST_LAUNCHD_SCHEDULE": yaml.dump([{"command": "c", "schedule": []}]),
        },
    )
    def test_empty_schedule_raises(self, mock_remove):
        with self.assertRaises(RuntimeError) as ctx:
            install_service("svc", "TEST", "tpl", "tag", "/tmp")
        self.assertIn("no schedule entries", str(ctx.exception))

    @patch("__main__.remove_existing_agents")
    @patch.dict(
        os.environ,
        {"TEST_ENABLED": "true", "TEST_LAUNCHD_SCHEDULE": "not-a-list"},
    )
    def test_invalid_yaml_structure_raises(self, mock_remove):
        with self.assertRaises(RuntimeError) as ctx:
            install_service("svc", "TEST", "tpl", "tag", "/tmp")
        self.assertIn("no jobs", str(ctx.exception))

    @patch("__main__.remove_existing_agents")
    @patch.dict(
        os.environ,
        {"TEST_ENABLED": "true", "TEST_LAUNCHD_SCHEDULE": ""},
    )
    def test_empty_schedule_var_skips(self, mock_remove):
        # Should not raise, just print skip message
        install_service("svc", "TEST", "tpl", "tag", "/tmp")

    @patch("__main__.remove_existing_agents")
    @patch.dict(os.environ, {"TEST_ENABLED": "false"})
    def test_disabled_service_only_removes(self, mock_remove):
        install_service("svc", "TEST", "tpl", "tag", "/tmp/agents")
        mock_remove.assert_called_once()

    @patch("__main__.remove_existing_agents")
    @patch.dict(
        os.environ,
        {
            "TEST_ENABLED": "true",
            "TEST_LAUNCHD_SCHEDULE": yaml.dump([42, "string"]),
        },
    )
    def test_non_dict_job_raises(self, mock_remove):
        with self.assertRaises(RuntimeError) as ctx:
            install_service("svc", "TEST", "tpl", "tag", "/tmp")
        self.assertIn("not a mapping", str(ctx.exception))


class TestParseSvcArgs(unittest.TestCase):
    def test_valid_args(self):
        result = parse_svc_args(["slack-summary:SLACK_SUMMARY", "news:NEWS"])
        self.assertEqual(result, [("slack-summary", "SLACK_SUMMARY"), ("news", "NEWS")])

    def test_invalid_arg_raises(self):
        with self.assertRaises(ValueError):
            parse_svc_args(["no-colon"])


if __name__ == "__main__":
    main()
