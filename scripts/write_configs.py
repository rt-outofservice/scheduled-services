#!/usr/bin/env python3
"""Write validated config.yaml files for scheduled services.

Replaces the inline bash block in the generate-configs playbook task.
Reads service config from environment variables, validates YAML, and
writes config files with mode 600.

Usage:
    write_configs.py [--tests] --base-dir PATH svc:PREFIX [svc:PREFIX ...]
"""

import argparse
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

# ── Config writing ────────────────────────────────────────────────


def write_config(svc, prefix, base_dir):
    """Write config.yaml for a service from its environment variable.

    Reads {prefix}_SERVICE_CONFIG, validates as YAML, writes to
    {base_dir}/services/{svc}/config.yaml with mode 600.

    Returns True if config was written, False if skipped.
    Raises ValueError on invalid YAML.
    """
    config_var = f"{prefix}_SERVICE_CONFIG"
    config_content = os.environ.get(config_var, "")

    if not config_content:
        print(f"{svc}: no SERVICE_CONFIG — skipping")
        return False

    # Decode escape sequences (env vars come through printf '%b')
    decoded = config_content.encode().decode("unicode_escape")

    # Validate YAML before writing
    try:
        yaml.safe_load(decoded)
    except yaml.YAMLError as e:
        raise ValueError(f"{svc} config.yaml is not valid YAML: {e}") from e

    config_path = Path(base_dir) / "services" / svc / "config.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(decoded)
    config_path.chmod(0o600)

    print(f"{svc}: config.yaml written and validated")
    return True


# ── CLI ───────────────────────────────────────────────────────────


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
    parser = argparse.ArgumentParser(description="Write service config files")
    parser.add_argument("--tests", action="store_true", help="Run embedded tests")
    parser.add_argument("--base-dir", help="Base directory for services")
    parser.add_argument("services", nargs="*", help="svc:PREFIX pairs")
    args = parser.parse_args()

    if args.tests:
        sys.argv = [sys.argv[0]]
        unittest.main(module=__name__, exit=True)

    if not args.base_dir:
        parser.error("--base-dir is required")
    if not args.services:
        parser.error("at least one svc:PREFIX argument is required")

    pairs = parse_svc_args(args.services)
    for svc, prefix in pairs:
        enabled = os.environ.get(f"{prefix}_ENABLED", "false")
        if enabled != "true":
            continue
        write_config(svc, prefix, args.base_dir)

    print("Config generation complete")


# ── Tests ─────────────────────────────────────────────────────────


class TestWriteConfig(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil

        shutil.rmtree(self.tmpdir)

    @patch.dict(os.environ, {"TEST_SERVICE_CONFIG": "hostname: myhost\\nport: 8080"})
    def test_writes_valid_yaml(self):
        result = write_config("test-svc", "TEST", self.tmpdir)
        self.assertTrue(result)
        config_path = Path(self.tmpdir) / "services" / "test-svc" / "config.yaml"
        self.assertTrue(config_path.exists())
        data = yaml.safe_load(config_path.read_text())
        self.assertEqual(data["hostname"], "myhost")
        self.assertEqual(data["port"], 8080)

    @patch.dict(os.environ, {"TEST_SERVICE_CONFIG": "hostname: myhost\\nport: 8080"})
    def test_sets_permissions_600(self):
        write_config("test-svc", "TEST", self.tmpdir)
        config_path = Path(self.tmpdir) / "services" / "test-svc" / "config.yaml"
        mode = stat.S_IMODE(config_path.stat().st_mode)
        self.assertEqual(mode, 0o600)

    @patch.dict(os.environ, {"TEST_SERVICE_CONFIG": ""})
    def test_skips_empty_config(self):
        result = write_config("test-svc", "TEST", self.tmpdir)
        self.assertFalse(result)
        config_path = Path(self.tmpdir) / "services" / "test-svc" / "config.yaml"
        self.assertFalse(config_path.exists())

    def test_skips_missing_env_var(self):
        result = write_config("test-svc", "NONEXISTENT", self.tmpdir)
        self.assertFalse(result)

    @patch.dict(os.environ, {"TEST_SERVICE_CONFIG": "key: value"})
    def test_creates_parent_dirs(self):
        deep_base = os.path.join(self.tmpdir, "a", "b")
        write_config("test-svc", "TEST", deep_base)
        config_path = Path(deep_base) / "services" / "test-svc" / "config.yaml"
        self.assertTrue(config_path.exists())

    @patch.dict(os.environ, {"TEST_SERVICE_CONFIG": "valid: true\\nlist:\\n  - one\\n  - two"})
    def test_multiline_yaml(self):
        write_config("test-svc", "TEST", self.tmpdir)
        config_path = Path(self.tmpdir) / "services" / "test-svc" / "config.yaml"
        data = yaml.safe_load(config_path.read_text())
        self.assertTrue(data["valid"])
        self.assertEqual(data["list"], ["one", "two"])

    @patch.dict(os.environ, {"TEST_SERVICE_CONFIG": "{{invalid yaml: [unterminated"})
    def test_invalid_yaml_raises(self):
        with self.assertRaises(ValueError) as ctx:
            write_config("test-svc", "TEST", self.tmpdir)
        self.assertIn("not valid YAML", str(ctx.exception))
        # Ensure no file was written
        config_path = Path(self.tmpdir) / "services" / "test-svc" / "config.yaml"
        self.assertFalse(config_path.exists())

    @patch.dict(os.environ, {"TEST_SERVICE_CONFIG": "key: value"})
    def test_overwrites_existing_config(self):
        config_path = Path(self.tmpdir) / "services" / "test-svc" / "config.yaml"
        config_path.parent.mkdir(parents=True)
        config_path.write_text("old: data\n")
        write_config("test-svc", "TEST", self.tmpdir)
        data = yaml.safe_load(config_path.read_text())
        self.assertEqual(data, {"key": "value"})
        self.assertNotIn("old", data)


class TestMainWorkflow(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil

        shutil.rmtree(self.tmpdir)

    @patch.dict(
        os.environ,
        {
            "SVC_ENABLED": "true",
            "SVC_SERVICE_CONFIG": "key: val",
            "DISABLED_ENABLED": "false",
            "DISABLED_SERVICE_CONFIG": "key: val",
        },
    )
    def test_skips_disabled_services(self):
        # Simulate the main loop logic
        pairs = parse_svc_args(["svc:SVC", "other:DISABLED"])
        for svc, prefix in pairs:
            enabled = os.environ.get(f"{prefix}_ENABLED", "false")
            if enabled != "true":
                continue
            write_config(svc, prefix, self.tmpdir)

        svc_path = Path(self.tmpdir) / "services" / "svc" / "config.yaml"
        other_path = Path(self.tmpdir) / "services" / "other" / "config.yaml"
        self.assertTrue(svc_path.exists())
        self.assertFalse(other_path.exists())


class TestParseSvcArgs(unittest.TestCase):
    def test_valid_args(self):
        result = parse_svc_args(["news-digest:NEWS_DIGEST", "svc:PREFIX"])
        self.assertEqual(result, [("news-digest", "NEWS_DIGEST"), ("svc", "PREFIX")])

    def test_invalid_arg_raises(self):
        with self.assertRaises(ValueError):
            parse_svc_args(["no-colon"])


if __name__ == "__main__":
    main()
