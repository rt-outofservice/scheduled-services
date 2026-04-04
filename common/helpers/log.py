"""Logging helper — daily log files with automatic cleanup."""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path


def setup_logging(service_name: str) -> tuple[logging.Logger, Path]:
    """Configure daily log file at ~/.logs/scheduled-services/<service_name>/.

    Creates a log file named <service_name>_MM-DD-YYYY.log, keeps 30 most recent
    files per service (deletes older), and redirects stdout/stderr to the log file.

    Returns (logger, log_file_path).
    """
    log_dir = Path.home() / ".logs" / "scheduled-services" / service_name
    log_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%m-%d-%Y")
    log_file = log_dir / f"{service_name}_{today}.log"

    logger = logging.getLogger(f"scheduled-services.{service_name}")
    logger.setLevel(logging.DEBUG)
    for h in logger.handlers:
        h.close()
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(file_handler)

    # Redirect stdout/stderr to the same stream used by FileHandler
    sys.stdout = file_handler.stream
    sys.stderr = file_handler.stream

    # Cleanup old logs — keep 30 most recent
    _cleanup_old_logs(log_dir, keep=30)

    return logger, log_file


def _cleanup_old_logs(log_dir: Path, keep: int = 30) -> None:
    """Delete all but the most recent `keep` log files in log_dir."""
    log_files = sorted(log_dir.glob("*.log"), key=lambda f: f.stat().st_mtime, reverse=True)
    for old_file in log_files[keep:]:
        old_file.unlink()


# --- Embedded tests ---
if __name__ == "__main__":
    if "--tests" in sys.argv:
        # Restore stdout/stderr for test output
        _real_stdout = sys.__stdout__
        _real_stderr = sys.__stderr__

        import tempfile
        import unittest

        class TestSetupLogging(unittest.TestCase):
            def setUp(self):
                self.tmpdir = tempfile.mkdtemp()
                self._orig_home = os.environ.get("HOME")
                os.environ["HOME"] = self.tmpdir
                # Reset stdout/stderr before each test
                sys.stdout = _real_stdout
                sys.stderr = _real_stderr

            def tearDown(self):
                if self._orig_home:
                    os.environ["HOME"] = self._orig_home
                else:
                    os.environ.pop("HOME", None)
                sys.stdout = _real_stdout
                sys.stderr = _real_stderr
                import shutil

                shutil.rmtree(self.tmpdir, ignore_errors=True)

            def test_creates_log_dir_and_file(self):
                logger, log_file = setup_logging("test-svc")
                self.assertTrue(log_file.exists())
                self.assertIn("test-svc", log_file.name)
                self.assertIn(datetime.now().strftime("%m-%d-%Y"), log_file.name)
                # Restore for assertions
                sys.stdout = _real_stdout
                sys.stderr = _real_stderr

            def test_logger_writes_to_file(self):
                logger, log_file = setup_logging("test-svc")
                sys.stdout = _real_stdout
                sys.stderr = _real_stderr
                logger.info("test message 123")
                logger.handlers[0].flush()
                content = log_file.read_text()
                self.assertIn("test message 123", content)

            def test_redirects_stdout_stderr(self):
                _, log_file = setup_logging("test-svc")
                print("stdout capture test")
                sys.stdout.flush()
                sys.stdout = _real_stdout
                sys.stderr = _real_stderr
                content = log_file.read_text()
                self.assertIn("stdout capture test", content)

            def test_cleanup_old_logs(self):
                log_dir = Path(self.tmpdir) / ".logs" / "scheduled-services" / "cleanup-test"
                log_dir.mkdir(parents=True)
                # Create 35 fake log files with different mtimes
                import time

                for i in range(35):
                    f = log_dir / f"cleanup-test_{i:02d}-01-2026.log"
                    f.write_text(f"log {i}")
                    os.utime(f, (time.time() - (35 - i) * 86400, time.time() - (35 - i) * 86400))
                _cleanup_old_logs(log_dir, keep=30)
                remaining = list(log_dir.glob("*.log"))
                self.assertEqual(len(remaining), 30)

            def test_returns_logger_and_path(self):
                logger, log_file = setup_logging("test-svc")
                sys.stdout = _real_stdout
                sys.stderr = _real_stderr
                self.assertIsInstance(logger, logging.Logger)
                self.assertIsInstance(log_file, Path)
                self.assertEqual(logger.name, "scheduled-services.test-svc")

        # Run with real stdout
        sys.stdout = _real_stdout
        sys.stderr = _real_stderr
        unittest.main(argv=["", "-v"], exit=True)
