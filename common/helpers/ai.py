"""AI helper — invoke claude -p for content analysis and summarization."""

import json
import subprocess
import sys


class AIError(Exception):
    """Raised when claude -p returns a non-zero exit code."""


def call_ai(prompt: str, model: str = "", timeout: int = 600) -> str:
    """Invoke claude -p with the given prompt and return stdout.

    Args:
        prompt: The prompt text to send to claude.
        model: Optional model name (passed via --model flag).
        timeout: Subprocess timeout in seconds (default 600).

    Returns:
        The stdout output from claude -p.

    Raises:
        AIError: If claude -p exits with a non-zero code.
    """
    cmd = ["claude", "-p"]
    if model:
        cmd.extend(["--model", model])

    result = subprocess.run(
        cmd,
        input=prompt,
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise AIError(f"claude -p failed (exit {result.returncode}): {stderr}")

    return result.stdout.strip()


def call_ai_json(prompt: str, model: str = "", timeout: int = 600) -> dict | list:
    """Invoke claude -p and parse the response as JSON.

    Args:
        prompt: The prompt text to send to claude.
        model: Optional model name.
        timeout: Subprocess timeout in seconds.

    Returns:
        Parsed JSON response (dict or list).

    Raises:
        AIError: If claude -p fails or response is not valid JSON.
    """
    response = call_ai(prompt, model=model, timeout=timeout)

    # Try to extract JSON from response (claude may include markdown fences)
    cleaned = _extract_json(response)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise AIError(f"Failed to parse AI response as JSON: {e}\nResponse: {response[:500]}") from e


def _extract_json(text: str) -> str:
    """Extract JSON from text that may contain markdown code fences."""
    # Strip markdown code fences if present
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.split("\n")
        # Remove first line (```json or ```) and last line (```)
        lines = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        stripped = "\n".join(lines).strip()

    return stripped


# --- Embedded tests ---
if __name__ == "__main__":
    if "--tests" in sys.argv:
        import unittest
        from unittest.mock import MagicMock, patch

        class TestCallAI(unittest.TestCase):
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

        class TestExtractJson(unittest.TestCase):
            def test_plain_json(self):
                self.assertEqual(_extract_json('{"a": 1}'), '{"a": 1}')

            def test_with_json_fence(self):
                self.assertEqual(_extract_json('```json\n{"a": 1}\n```'), '{"a": 1}')

            def test_with_plain_fence(self):
                self.assertEqual(_extract_json('```\n{"a": 1}\n```'), '{"a": 1}')

            def test_strips_whitespace(self):
                self.assertEqual(_extract_json('  {"a": 1}  '), '{"a": 1}')

        unittest.main(argv=["", "-v"], exit=True)
