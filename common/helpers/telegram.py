"""Telegram helper — send messages via tg() shell function."""

import subprocess
import sys


def send_telegram(message: str, hostname: str = "") -> None:
    """Send a message via telegram using tg() from shell helpers.

    Sources tg() from ~/.bash.d/telegram.bash or ~/.zsh.d/telegram.zsh and
    calls it via bash subprocess.

    Args:
        message: The message text to send. Passed through as-is — callers are
                 responsible for escaping Markdown V1 special chars if needed.
        hostname: If set, prepends "*hostname* \u2014 " prefix to the message.
    """
    if not message.strip():
        return

    if hostname:
        escaped_hostname = _escape_markdown_v1(hostname)
        full_message = f"*{escaped_hostname}* \u2014 {message}"
    else:
        full_message = message

    # Auto-split messages exceeding 4000 chars
    chunks = _split_message(full_message, max_len=4000)

    for chunk in chunks:
        _send_chunk(chunk)


def _escape_markdown_v1(text: str) -> str:
    """Escape Markdown V1 special characters: * _ ` ["""
    # Escape backslashes first to avoid double-escaping
    text = text.replace("\\", "\\\\")
    for char in ["*", "_", "`", "["]:
        text = text.replace(char, f"\\{char}")
    return text


def _split_message(message: str, max_len: int = 4000) -> list[str]:
    """Split message into chunks not exceeding max_len chars.

    Tries to split on newlines first, then falls back to hard split.
    """
    if len(message) <= max_len:
        return [message]

    chunks = []
    remaining = message
    while remaining:
        if len(remaining) <= max_len:
            chunks.append(remaining)
            break

        # Try to find a newline to split on
        split_pos = remaining.rfind("\n", 0, max_len)
        if split_pos == -1 or split_pos < max_len // 2:
            # No good newline break, hard split
            split_pos = max_len

        chunks.append(remaining[:split_pos])
        remaining = remaining[split_pos:].lstrip("\n")

    return chunks


def _send_chunk(message: str) -> None:
    """Send a single message chunk via tg() shell function."""
    script = 'source ~/.bash.d/telegram.bash 2>/dev/null || source ~/.zsh.d/telegram.zsh 2>/dev/null; tg "$1"'
    try:
        subprocess.run(
            ["bash", "-c", script, "bash", message],
            check=True,
            capture_output=True,
            timeout=30,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        import logging

        logging.getLogger(__name__).error("Telegram send failed: %s", e)


# --- Embedded tests ---
if __name__ == "__main__":
    if "--tests" in sys.argv:
        import unittest
        from unittest.mock import patch

        class TestEscapeMarkdownV1(unittest.TestCase):
            def test_escapes_star(self):
                self.assertEqual(_escape_markdown_v1("*bold*"), "\\*bold\\*")

            def test_escapes_underscore(self):
                self.assertEqual(_escape_markdown_v1("_italic_"), "\\_italic\\_")

            def test_escapes_backtick(self):
                self.assertEqual(_escape_markdown_v1("`code`"), "\\`code\\`")

            def test_escapes_bracket(self):
                self.assertEqual(_escape_markdown_v1("[link]"), "\\[link]")

            def test_escapes_backslash(self):
                self.assertEqual(_escape_markdown_v1("a\\b"), "a\\\\b")

            def test_no_change_for_plain_text(self):
                self.assertEqual(_escape_markdown_v1("hello world"), "hello world")

            def test_multiple_special_chars(self):
                result = _escape_markdown_v1("*_`[")
                self.assertEqual(result, "\\*\\_\\`\\[")

        class TestSplitMessage(unittest.TestCase):
            def test_short_message_no_split(self):
                result = _split_message("hello", max_len=4000)
                self.assertEqual(result, ["hello"])

            def test_exact_limit(self):
                msg = "a" * 4000
                result = _split_message(msg, max_len=4000)
                self.assertEqual(len(result), 1)

            def test_splits_on_newline(self):
                msg = "a" * 50 + "\n" + "b" * 50
                result = _split_message(msg, max_len=60)
                self.assertEqual(len(result), 2)
                self.assertEqual(result[0], "a" * 50)
                self.assertEqual(result[1], "b" * 50)

            def test_hard_split_when_no_newline(self):
                msg = "a" * 100
                result = _split_message(msg, max_len=40)
                self.assertTrue(all(len(c) <= 40 for c in result))
                self.assertEqual("".join(result), msg)

            def test_empty_message(self):
                result = _split_message("", max_len=4000)
                self.assertEqual(result, [""])

        class TestSendTelegram(unittest.TestCase):
            @patch("__main__._send_chunk")
            def test_prepends_hostname(self, mock_send):
                send_telegram("test msg", hostname="myhost")
                mock_send.assert_called_once()
                sent = mock_send.call_args[0][0]
                self.assertTrue(sent.startswith("*myhost* \u2014 "))

            @patch("__main__._send_chunk")
            def test_no_hostname(self, mock_send):
                send_telegram("test msg")
                mock_send.assert_called_once()
                sent = mock_send.call_args[0][0]
                self.assertNotIn("\u2014", sent)

            @patch("__main__._send_chunk")
            def test_preserves_markdown_in_content(self, mock_send):
                send_telegram("*bold*")
                mock_send.assert_called_once()
                sent = mock_send.call_args[0][0]
                self.assertIn("*bold*", sent)

            @patch("__main__._send_chunk")
            def test_escapes_hostname_special_chars(self, mock_send):
                send_telegram("test msg", hostname="my_server_01")
                mock_send.assert_called_once()
                sent = mock_send.call_args[0][0]
                self.assertTrue(sent.startswith("*my\\_server\\_01* \u2014 "))

            @patch("__main__._send_chunk")
            def test_empty_message_skipped(self, mock_send):
                send_telegram("   ")
                mock_send.assert_not_called()

            @patch("__main__._send_chunk")
            def test_auto_splits_long_message(self, mock_send):
                long_msg = "a" * 5000
                send_telegram(long_msg)
                self.assertGreater(mock_send.call_count, 1)

        class TestSendChunk(unittest.TestCase):
            @patch("subprocess.run")
            def test_calls_bash_with_tg(self, mock_run):
                _send_chunk("hello")
                mock_run.assert_called_once()
                args = mock_run.call_args
                self.assertEqual(args[0][0][0], "bash")
                self.assertEqual(args[0][0][-1], "hello")
                self.assertTrue(args[1]["check"])

            @patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "bash"))
            def test_logs_error_on_failure(self, mock_run):
                # Should not raise - logs instead
                _send_chunk("hello")

            @patch("subprocess.run", side_effect=subprocess.TimeoutExpired("bash", 30))
            def test_logs_error_on_timeout(self, mock_run):
                # Should not raise - logs instead
                _send_chunk("hello")

        unittest.main(argv=["", "-v"], exit=True)
