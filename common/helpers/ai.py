"""AI helper — invoke LLM providers (claude, codex) for content analysis and summarization."""

import json
import subprocess
import sys
import tempfile


class AIError(Exception):
    """Raised when an LLM call fails."""


def call_ai(prompt: str, provider: str = "claude", model: str = "", effort: str = "", timeout: int = 600) -> str:
    """Invoke an LLM provider and return the text response.

    Args:
        prompt: The prompt text to send.
        provider: LLM provider — "claude" or "codex".
        model: Optional model name.
        effort: Optional reasoning effort (codex only, e.g. "high", "xhigh").
        timeout: Subprocess timeout in seconds (default 600).

    Returns:
        The text response from the LLM.

    Raises:
        AIError: If the LLM call fails.
    """
    if provider == "codex":
        return _call_codex(prompt, model=model, effort=effort, timeout=timeout)
    return _call_claude(prompt, model=model, timeout=timeout)


def call_ai_json(
    prompt: str, provider: str = "claude", model: str = "", effort: str = "", timeout: int = 600
) -> dict | list:
    """Invoke an LLM provider and parse the response as JSON.

    Args:
        prompt: The prompt text to send.
        provider: LLM provider — "claude" or "codex".
        model: Optional model name.
        effort: Optional reasoning effort (codex only).
        timeout: Subprocess timeout in seconds.

    Returns:
        Parsed JSON response (dict or list).

    Raises:
        AIError: If the LLM call fails or response is not valid JSON.
    """
    response = call_ai(prompt, provider=provider, model=model, effort=effort, timeout=timeout)
    cleaned = _extract_json(response)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise AIError(f"Failed to parse AI response as JSON: {e}\nResponse: {response[:500]}") from e


def _call_claude(prompt: str, model: str = "", timeout: int = 600) -> str:
    """Invoke claude -p."""
    cmd = ["claude", "-p"]
    if model:
        cmd.extend(["--model", model])
    try:
        result = subprocess.run(cmd, input=prompt, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise AIError(f"claude -p timed out after {timeout}s") from None
    except FileNotFoundError:
        raise AIError("claude binary not found — is it installed and on PATH?") from None
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise AIError(f"claude -p failed (exit {result.returncode}): {detail}")
    return result.stdout.strip()


def _call_codex(prompt: str, model: str = "", effort: str = "", timeout: int = 600) -> str:
    """Invoke codex exec with output to a temp file."""
    cmd = ["codex", "exec", "--ephemeral", "--skip-git-repo-check"]
    if model:
        cmd.extend(["-m", model])
    if effort:
        cmd.extend(["-c", f"reasoning_effort={effort}"])

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as out_file:
        out_path = out_file.name
    cmd.extend(["-o", out_path])

    try:
        result = subprocess.run(cmd, input=prompt, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise AIError(f"codex exec timed out after {timeout}s") from None
    except FileNotFoundError:
        raise AIError("codex binary not found — is it installed and on PATH?") from None
    finally:
        import os

        try:
            with open(out_path) as f:
                output = f.read().strip()
            os.unlink(out_path)
        except OSError:
            output = ""

    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise AIError(f"codex exec failed (exit {result.returncode}): {stderr}")
    if not output:
        raise AIError("codex exec returned empty output")
    return output


def _extract_json(text: str) -> str:
    """Extract JSON from text that may contain markdown code fences."""
    stripped = text.strip()

    # Find code fence anywhere in text (handles preamble before fence)
    fence_start = stripped.find("```")
    if fence_start != -1:
        after_fence = stripped[fence_start:]
        first_newline = after_fence.find("\n")
        if first_newline == -1:
            return stripped
        content_start = first_newline + 1
        closing_fence = after_fence.find("```", content_start)
        if closing_fence != -1:
            return after_fence[content_start:closing_fence].strip()
        return after_fence[content_start:].strip()

    return stripped


# --- Embedded tests ---
if __name__ == "__main__":
    if "--tests" in sys.argv:
        import unittest
        from unittest.mock import MagicMock, patch

        class TestCallClaude(unittest.TestCase):
            @patch("subprocess.run")
            def test_basic_call(self, mock_run):
                mock_run.return_value = MagicMock(returncode=0, stdout="response text\n", stderr="")
                result = call_ai("test prompt")
                self.assertEqual(result, "response text")
                mock_run.assert_called_once()
                args = mock_run.call_args
                self.assertEqual(args[0][0], ["claude", "-p"])
                self.assertEqual(args[1]["input"], "test prompt")

            @patch("subprocess.run")
            def test_with_model(self, mock_run):
                mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
                call_ai("prompt", model="sonnet")
                cmd = mock_run.call_args[0][0]
                self.assertEqual(cmd, ["claude", "-p", "--model", "sonnet"])

            @patch("subprocess.run")
            def test_explicit_claude_provider(self, mock_run):
                mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
                call_ai("prompt", provider="claude", model="haiku")
                cmd = mock_run.call_args[0][0]
                self.assertEqual(cmd, ["claude", "-p", "--model", "haiku"])

            @patch("subprocess.run")
            def test_raises_on_failure(self, mock_run):
                mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error details")
                with self.assertRaises(AIError) as ctx:
                    call_ai("prompt")
                self.assertIn("error details", str(ctx.exception))

            @patch("subprocess.run")
            def test_timeout_passed(self, mock_run):
                mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
                call_ai("prompt", timeout=120)
                self.assertEqual(mock_run.call_args[1]["timeout"], 120)

            @patch("subprocess.run")
            def test_timeout_expired_raises_ai_error(self, mock_run):
                mock_run.side_effect = subprocess.TimeoutExpired(cmd="claude", timeout=600)
                with self.assertRaises(AIError) as ctx:
                    call_ai("prompt")
                self.assertIn("timed out", str(ctx.exception))

            @patch("subprocess.run")
            def test_file_not_found_raises_ai_error(self, mock_run):
                mock_run.side_effect = FileNotFoundError()
                with self.assertRaises(AIError) as ctx:
                    call_ai("prompt")
                self.assertIn("not found", str(ctx.exception))

        class TestCallCodex(unittest.TestCase):
            @patch("subprocess.run")
            def test_basic_codex_call(self, mock_run):
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                # Write response to the output file codex would use
                with patch("builtins.open", create=True) as mock_open:
                    mock_open.return_value.__enter__ = lambda s: s
                    mock_open.return_value.__exit__ = MagicMock(return_value=False)
                    mock_open.return_value.read = MagicMock(return_value="codex response\n")
                    with patch("os.unlink"):
                        result = _call_codex("test prompt", model="gpt-5.4")
                self.assertEqual(result, "codex response")
                cmd = mock_run.call_args[0][0]
                self.assertIn("codex", cmd)
                self.assertIn("exec", cmd)
                self.assertIn("--ephemeral", cmd)
                self.assertIn("-m", cmd)
                self.assertIn("gpt-5.4", cmd)

            @patch("subprocess.run")
            def test_codex_with_effort(self, mock_run):
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                with patch("builtins.open", create=True) as mock_open:
                    mock_open.return_value.__enter__ = lambda s: s
                    mock_open.return_value.__exit__ = MagicMock(return_value=False)
                    mock_open.return_value.read = MagicMock(return_value="ok")
                    with patch("os.unlink"):
                        _call_codex("prompt", model="gpt-5.4", effort="xhigh")
                cmd = mock_run.call_args[0][0]
                self.assertIn("-c", cmd)
                idx = cmd.index("-c")
                self.assertEqual(cmd[idx + 1], "reasoning_effort=xhigh")

            @patch("subprocess.run")
            def test_codex_timeout(self, mock_run):
                mock_run.side_effect = subprocess.TimeoutExpired(cmd="codex", timeout=600)
                with patch("builtins.open", create=True) as mock_open:
                    mock_open.return_value.__enter__ = lambda s: s
                    mock_open.return_value.__exit__ = MagicMock(return_value=False)
                    mock_open.return_value.read = MagicMock(return_value="")
                    with patch("os.unlink"), self.assertRaises(AIError) as ctx:
                        _call_codex("prompt")
                self.assertIn("timed out", str(ctx.exception))

        class TestCallAIJson(unittest.TestCase):
            @patch("__main__.call_ai")
            def test_parses_json(self, mock_ai):
                mock_ai.return_value = '{"key": "value"}'
                result = call_ai_json("prompt")
                self.assertEqual(result, {"key": "value"})

            @patch("__main__.call_ai")
            def test_parses_json_with_fences(self, mock_ai):
                mock_ai.return_value = '```json\n{"key": "value"}\n```'
                result = call_ai_json("prompt")
                self.assertEqual(result, {"key": "value"})

            @patch("__main__.call_ai")
            def test_raises_on_invalid_json(self, mock_ai):
                mock_ai.return_value = "not json at all"
                with self.assertRaises(AIError) as ctx:
                    call_ai_json("prompt")
                self.assertIn("Failed to parse", str(ctx.exception))

            @patch("__main__.call_ai")
            def test_parses_list(self, mock_ai):
                mock_ai.return_value = "[1, 2, 3]"
                result = call_ai_json("prompt")
                self.assertEqual(result, [1, 2, 3])

            @patch("__main__.call_ai")
            def test_passes_provider_and_effort(self, mock_ai):
                mock_ai.return_value = '{"ok": true}'
                call_ai_json("prompt", provider="codex", model="gpt-5.4", effort="xhigh")
                mock_ai.assert_called_once_with(
                    "prompt", provider="codex", model="gpt-5.4", effort="xhigh", timeout=600
                )

        class TestExtractJson(unittest.TestCase):
            def test_plain_json(self):
                self.assertEqual(_extract_json('{"a": 1}'), '{"a": 1}')

            def test_with_json_fence(self):
                self.assertEqual(_extract_json('```json\n{"a": 1}\n```'), '{"a": 1}')

            def test_with_plain_fence(self):
                self.assertEqual(_extract_json('```\n{"a": 1}\n```'), '{"a": 1}')

            def test_strips_whitespace(self):
                self.assertEqual(_extract_json('  {"a": 1}  '), '{"a": 1}')

            def test_with_preamble_before_fence(self):
                text = 'Here is the JSON:\n```json\n{"a": 1}\n```'
                self.assertEqual(_extract_json(text), '{"a": 1}')

            def test_with_preamble_and_plain_fence(self):
                text = "Sure, here you go:\n```\n[1, 2, 3]\n```"
                self.assertEqual(_extract_json(text), "[1, 2, 3]")

            def test_with_trailing_text_after_fence(self):
                text = '```json\n{"a": 1}\n```\nHope this helps!'
                self.assertEqual(_extract_json(text), '{"a": 1}')

            def test_with_preamble_and_trailing_text(self):
                text = 'Here you go:\n```json\n{"a": 1}\n```\nLet me know if you need more.'
                self.assertEqual(_extract_json(text), '{"a": 1}')

        unittest.main(argv=["", "-v"], exit=True)
