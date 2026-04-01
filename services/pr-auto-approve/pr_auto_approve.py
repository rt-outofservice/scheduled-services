#!/usr/bin/env python3
"""PR auto-approve service — review and approve infrastructure PRs/MRs across GitHub and GitLab.

Usage:
    uv run python services/pr-auto-approve/pr_auto_approve.py [--day-summary-only] [--tests]

Config (config.yaml):
    hostname: myhost
    targets:
      - provider: github
        org: my-org
        filters:
          include_only: ["infra-"]
          exclude: ["archived-"]
      - provider: gitlab
        org: my-group
    skip_bots: true
    cli_wrappers:
      glab: /path/to/glab-wrapper
    approval_cap: 2
"""

import fcntl
import json
import logging
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote as urlquote

import yaml

# Add common helpers to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common" / "helpers"))
from ai import AIError, call_ai_json
from log import setup_logging
from telegram import send_telegram

REVIEWS_FILE = Path.home() / ".scheduled-services" / "services" / "pr-auto-approve" / "reviews.md"
LOCK_FILE = Path.home() / ".scheduled-services" / "services" / "pr-auto-approve" / ".lock"
MAX_FILES = 7
MAX_LINES = 300
DEFAULT_APPROVAL_CAP = 2


def load_config(config_path: Path) -> dict:
    """Load and validate config.yaml."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    if not config or "targets" not in config:
        raise ValueError("Config must contain 'targets' key")
    if not config["targets"]:
        raise ValueError("Config 'targets' list must not be empty")
    return config


def _wrap_cmd(cmd: list[str], wrapper: str | None) -> list[str]:
    """Prepend CLI wrapper to command if configured."""
    if wrapper:
        return [wrapper] + cmd
    return cmd


def _run_cmd(cmd: list[str], timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a subprocess command and return the result."""
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _encode_gitlab_path(path: str) -> str:
    """URL-encode a GitLab project path for API calls."""
    return urlquote(path, safe="")


def start_glab_proxy(cli_wrappers: dict, hostname: str, logger: logging.Logger) -> bool:
    """Start glab proxy if wrapper is configured. Returns True on success."""
    wrapper = cli_wrappers.get("glab")
    if not wrapper:
        return True
    try:
        result = _run_cmd([wrapper, "--start-proxy", hostname], timeout=60)
        if result.returncode != 0:
            logger.error(f"Failed to start glab proxy: {result.stderr.strip()}")
            return False
        return True
    except Exception as e:
        logger.error(f"Failed to start glab proxy: {e}")
        return False


def stop_glab_proxy(cli_wrappers: dict, logger: logging.Logger) -> None:
    """Stop glab proxy if wrapper was configured."""
    wrapper = cli_wrappers.get("glab")
    if not wrapper:
        return
    try:
        _run_cmd([wrapper, "--stop-proxy"], timeout=60)
    except Exception as e:
        logger.warning(f"Failed to stop glab proxy: {e}")


def verify_api_access(provider: str, cli_wrappers: dict) -> str | None:
    """Verify API access for a provider. Returns own username or None on failure."""
    if provider == "github":
        cmd = _wrap_cmd(["gh", "api", "user", "--jq", ".login"], cli_wrappers.get("gh"))
    elif provider == "gitlab":
        cmd = _wrap_cmd(["glab", "api", "user"], cli_wrappers.get("glab"))
    else:
        return None
    try:
        result = _run_cmd(cmd)
        if result.returncode == 0 and result.stdout.strip():
            if provider == "gitlab":
                return json.loads(result.stdout).get("username", "")
            return result.stdout.strip()
    except Exception:
        pass
    return None


# --- Repo discovery ---


def discover_repos(target: dict, cli_wrappers: dict, logger: logging.Logger) -> list[str]:
    """Discover up to 5 recently updated repos for a target.

    Returns list of repo full names (org/repo).
    """
    provider = target["provider"]
    org = target["org"]
    filters = target.get("filters", {})
    include_only = filters.get("include_only", [])
    exclude = filters.get("exclude", [])

    if provider == "github":
        repos = _discover_github_repos(org, cli_wrappers, logger)
    elif provider == "gitlab":
        repos = _discover_gitlab_repos(org, cli_wrappers, logger)
    else:
        logger.warning(f"Unknown provider: {provider}")
        return []

    repos = _filter_repos(repos, include_only, exclude)
    return repos[:5]


def _discover_github_repos(org: str, cli_wrappers: dict, logger: logging.Logger) -> list[str]:
    """Find recently updated repos in a GitHub org via REST API, with search fallback."""
    cmd = _wrap_cmd(
        ["gh", "api", f"orgs/{org}/repos?sort=pushed&per_page=10&direction=desc", "--jq", ".[].full_name"],
        cli_wrappers.get("gh"),
    )
    try:
        result = _run_cmd(cmd)
        if result.returncode == 0 and result.stdout.strip():
            return [r.strip() for r in result.stdout.strip().split("\n") if r.strip()]
    except Exception as e:
        logger.warning(f"GitHub REST repo list failed for {org}: {e}")

    # Fallback: search API (may lag behind)
    logger.info(f"Falling back to search API for {org}")
    cmd = _wrap_cmd(
        [
            "gh",
            "search",
            "repos",
            f"org:{org}",
            "--sort",
            "updated",
            "--limit",
            "10",
            "--json",
            "fullName",
            "--jq",
            ".[].fullName",
        ],
        cli_wrappers.get("gh"),
    )
    try:
        result = _run_cmd(cmd)
        if result.returncode == 0 and result.stdout.strip():
            return [r.strip() for r in result.stdout.strip().split("\n") if r.strip()]
    except Exception as e:
        logger.warning(f"GitHub search fallback failed for {org}: {e}")
    return []


def _discover_gitlab_repos(org: str, cli_wrappers: dict, logger: logging.Logger) -> list[str]:
    """Find recently updated projects in a GitLab group."""
    encoded_org = _encode_gitlab_path(org)
    api_path = f"groups/{encoded_org}/projects?order_by=last_activity_at&sort=desc&per_page=10"
    cmd = _wrap_cmd(
        ["glab", "api", api_path],
        cli_wrappers.get("glab"),
    )
    try:
        result = _run_cmd(cmd)
        if result.returncode == 0 and result.stdout.strip():
            projects = json.loads(result.stdout)
            return [p["path_with_namespace"] for p in projects if "path_with_namespace" in p]
    except Exception as e:
        logger.warning(f"GitLab repo discovery failed for {org}: {e}")
    return []


def _filter_repos(repos: list[str], include_only: list[str], exclude: list[str]) -> list[str]:
    """Apply include_only and exclude pattern filters to repo names."""
    if include_only:
        repos = [r for r in repos if any(p in r for p in include_only)]
    if exclude:
        repos = [r for r in repos if not any(p in r for p in exclude)]
    return repos


# --- PR/MR listing ---


def list_prs(
    repo: str, provider: str, own_username: str, skip_bots: bool, cli_wrappers: dict, logger: logging.Logger
) -> list[dict]:
    """List open non-draft PRs/MRs for a repo, excluding own and optionally bots."""
    if provider == "github":
        return _list_github_prs(repo, own_username, skip_bots, cli_wrappers, logger)
    elif provider == "gitlab":
        return _list_gitlab_mrs(repo, own_username, skip_bots, cli_wrappers, logger)
    return []


def _list_github_prs(
    repo: str, own_username: str, skip_bots: bool, cli_wrappers: dict, logger: logging.Logger
) -> list[dict]:
    """List open non-draft PRs on a GitHub repo."""
    cmd = _wrap_cmd(
        [
            "gh",
            "pr",
            "list",
            "--repo",
            repo,
            "--state",
            "open",
            "--json",
            "number,title,author,isDraft,additions,deletions,changedFiles",
        ],
        cli_wrappers.get("gh"),
    )
    try:
        result = _run_cmd(cmd)
        if result.returncode != 0:
            logger.warning(f"Failed to list PRs for {repo}: {result.stderr.strip()}")
            return []
        prs = json.loads(result.stdout)
    except Exception as e:
        logger.warning(f"Failed to list PRs for {repo}: {e}")
        return []

    filtered = []
    for pr in prs:
        if pr.get("isDraft"):
            continue
        author = pr.get("author", {}).get("login", "")
        if author == own_username:
            continue
        if skip_bots and author.endswith("[bot]"):
            continue
        filtered.append(
            {
                "number": pr["number"],
                "title": pr.get("title", ""),
                "author": author,
                "additions": pr.get("additions", 0),
                "deletions": pr.get("deletions", 0),
                "changed_files": pr.get("changedFiles", 0),
            }
        )
    return filtered


def _list_gitlab_mrs(
    repo: str, own_username: str, skip_bots: bool, cli_wrappers: dict, logger: logging.Logger
) -> list[dict]:
    """List open non-draft MRs on a GitLab project."""
    encoded = _encode_gitlab_path(repo)
    cmd = _wrap_cmd(
        ["glab", "api", f"projects/{encoded}/merge_requests?state=opened&per_page=20"],
        cli_wrappers.get("glab"),
    )
    try:
        result = _run_cmd(cmd)
        if result.returncode != 0:
            logger.warning(f"Failed to list MRs for {repo}: {result.stderr.strip()}")
            return []
        mrs = json.loads(result.stdout)
    except Exception as e:
        logger.warning(f"Failed to list MRs for {repo}: {e}")
        return []

    filtered = []
    for mr in mrs:
        if mr.get("work_in_progress") or mr.get("draft"):
            continue
        author = mr.get("author", {}).get("username", "")
        if author == own_username:
            continue
        if skip_bots and (mr.get("author", {}).get("bot", False) or author.endswith("[bot]")):
            continue
        filtered.append(
            {
                "number": mr["iid"],
                "title": mr.get("title", ""),
                "author": author,
                "additions": 0,
                "deletions": 0,
                "changed_files": 0,
            }
        )
    return filtered


# --- Algorithmic gates ---


def is_already_approved(
    repo: str, pr_number: int, own_username: str, provider: str, cli_wrappers: dict, logger: logging.Logger
) -> bool:
    """Check if we already approved this PR/MR."""
    if provider == "github":
        cmd = _wrap_cmd(
            ["gh", "api", f"repos/{repo}/pulls/{pr_number}/reviews"],
            cli_wrappers.get("gh"),
        )
        try:
            result = _run_cmd(cmd)
            if result.returncode == 0:
                reviews = json.loads(result.stdout)
                return any(
                    r.get("user", {}).get("login") == own_username and r.get("state") == "APPROVED" for r in reviews
                )
        except Exception:
            pass
    elif provider == "gitlab":
        encoded = _encode_gitlab_path(repo)
        cmd = _wrap_cmd(
            ["glab", "api", f"projects/{encoded}/merge_requests/{pr_number}/approvals"],
            cli_wrappers.get("glab"),
        )
        try:
            result = _run_cmd(cmd)
            if result.returncode == 0:
                data = json.loads(result.stdout)
                return any(a.get("user", {}).get("username") == own_username for a in data.get("approved_by", []))
        except Exception:
            pass
    return False


def check_complexity(
    pr_data: dict, repo: str, pr_number: int, provider: str, cli_wrappers: dict, logger: logging.Logger
) -> tuple[bool, int, int]:
    """Check PR/MR complexity. Returns (within_limits, files_changed, lines_changed).

    For GitHub, uses stats from PR listing. For GitLab, fetches from API.
    """
    files = pr_data.get("changed_files", 0)
    lines = pr_data.get("additions", 0) + pr_data.get("deletions", 0)

    # For GitLab, stats aren't in the listing — fetch separately
    if provider == "gitlab" and files == 0 and lines == 0:
        files, lines = _fetch_gitlab_mr_stats(repo, pr_number, cli_wrappers, logger)
        if files < 0:  # Stats fetch failed — fail closed
            return False, 0, 0

    within = files <= MAX_FILES and lines <= MAX_LINES
    return within, files, lines


def _fetch_gitlab_mr_stats(repo: str, mr_number: int, cli_wrappers: dict, logger: logging.Logger) -> tuple[int, int]:
    """Fetch diff stats for a GitLab MR. Returns (files_changed, lines_changed) or (-1, -1) on failure."""
    encoded = _encode_gitlab_path(repo)
    cmd = _wrap_cmd(
        ["glab", "api", f"projects/{encoded}/merge_requests/{mr_number}/changes?access_raw_diffs=true"],
        cli_wrappers.get("glab"),
    )
    try:
        result = _run_cmd(cmd, timeout=60)
        if result.returncode != 0:
            return -1, -1
        data = json.loads(result.stdout)
        changes = data.get("changes", [])
        files = len(changes)
        lines = 0
        for change in changes:
            diff_text = change.get("diff", "")
            for line in diff_text.split("\n"):
                if (line.startswith("+") and not line.startswith("+++")) or (
                    line.startswith("-") and not line.startswith("---")
                ):
                    lines += 1
        return files, lines
    except Exception as e:
        logger.warning(f"Failed to fetch MR stats for {repo}!{mr_number}: {e}")
        return -1, -1


# --- AI review ---


def get_diff(repo: str, pr_number: int, provider: str, cli_wrappers: dict) -> str:
    """Get the diff text for a PR/MR."""
    if provider == "github":
        cmd = _wrap_cmd(["gh", "pr", "diff", str(pr_number), "--repo", repo], cli_wrappers.get("gh"))
    elif provider == "gitlab":
        encoded = _encode_gitlab_path(repo)
        cmd = _wrap_cmd(
            ["glab", "api", f"projects/{encoded}/merge_requests/{pr_number}/changes"],
            cli_wrappers.get("glab"),
        )
    else:
        return ""
    try:
        result = _run_cmd(cmd, timeout=60)
        if result.returncode != 0:
            return ""
        if provider == "gitlab":
            data = json.loads(result.stdout)
            return "\n".join(c.get("diff", "") for c in data.get("changes", []))
        return result.stdout
    except Exception:
        return ""


def build_review_prompt(diff: str, pr_title: str) -> str:
    """Build the AI prompt for reviewing a PR diff."""
    return (
        "You are reviewing an infrastructure/DevOps pull request for safety.\n\n"
        f"PR Title: {pr_title}\n\n"
        "Assess the diff below. Check for:\n"
        "- Open/permissive security groups (0.0.0.0/0 on sensitive ports)\n"
        "- Hardcoded secrets, tokens, or passwords\n"
        "- Overly permissive IAM policies (*, admin access)\n"
        "- Dangerous operations (resource destruction without safeguards)\n"
        "- Obvious logic errors\n\n"
        "Return ONLY a JSON object (no markdown fences):\n"
        '{"decision": "approve" or "skip", "reason": "brief explanation"}\n\n'
        "If the changes are safe routine infra/DevOps changes, decide 'approve'.\n"
        "If you see any concerns or the changes are outside infra/DevOps scope, decide 'skip'.\n\n"
        f"Diff:\n{diff}"
    )


def approve_pr(repo: str, pr_number: int, provider: str, cli_wrappers: dict, logger: logging.Logger) -> bool:
    """Approve a PR/MR silently. Returns True on success."""
    if provider == "github":
        cmd = _wrap_cmd(["gh", "pr", "review", str(pr_number), "--repo", repo, "--approve"], cli_wrappers.get("gh"))
    elif provider == "gitlab":
        encoded = _encode_gitlab_path(repo)
        cmd = _wrap_cmd(
            ["glab", "api", "-X", "POST", f"projects/{encoded}/merge_requests/{pr_number}/approve"],
            cli_wrappers.get("glab"),
        )
    else:
        return False
    try:
        result = _run_cmd(cmd)
        return result.returncode == 0
    except Exception as e:
        logger.warning(f"Failed to approve {repo}#{pr_number}: {e}")
        return False


# --- Review log ---


def append_review(
    reviews_file: Path, repo: str, pr_number: int, author: str, title: str, summary: str, action: str
) -> None:
    """Append a review entry to reviews.md (append-only markdown table)."""
    reviews_file.parent.mkdir(parents=True, exist_ok=True)
    if not reviews_file.exists():
        reviews_file.write_text(
            "| Date | Repo | PR# | Author | Title | Summary | Action |\n"
            "|------|------|-----|--------|-------|---------|--------|\n"
        )
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    title_safe = title.replace("|", "/")[:60]
    summary_safe = summary.replace("|", "/")[:80]
    line = f"| {today} | {repo} | #{pr_number} | {author} | {title_safe} | {summary_safe} | {action} |\n"
    with open(reviews_file, "a") as f:
        f.write(line)


def read_today_reviews(reviews_file: Path) -> list[dict]:
    """Read today's review entries from reviews.md."""
    if not reviews_file.exists():
        return []
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    entries = []
    for line in reviews_file.read_text().splitlines():
        if not line.startswith("|") or line.startswith("| Date") or line.startswith("|---"):
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 8:
            continue
        date, repo, pr_num, author, title, summary, action = parts[1:8]
        if date == today:
            entries.append(
                {
                    "repo": repo,
                    "pr_number": pr_num,
                    "author": author,
                    "title": title,
                    "summary": summary,
                    "action": action,
                }
            )
    return entries


def count_today_approvals(reviews_file: Path) -> int:
    """Count how many approvals have been made today."""
    return sum(1 for e in read_today_reviews(reviews_file) if e["action"] == "approved")


def format_day_summary(entries: list[dict]) -> str:
    """Format day summary grouped by repo."""
    if not entries:
        return "PR Auto-Approve: no reviews today"
    by_repo: dict[str, list[dict]] = {}
    for e in entries:
        by_repo.setdefault(e["repo"], []).append(e)
    lines = [f"PR Auto-Approve — daily summary ({len(entries)} reviews):"]
    for repo, repo_entries in by_repo.items():
        lines.append(f"\n{repo}:")
        for e in repo_entries:
            lines.append(f"  {e['pr_number']} ({e['author']}): {e['action']} — {e['title']}")
    return "\n".join(lines)


def format_warnings(warnings: list[str]) -> str:
    """Format warnings list for logging/notification."""
    if not warnings:
        return ""
    return "Warnings: " + "; ".join(warnings)


# --- Main flows ---


def run_review(config_path: Path) -> None:
    """Main review execution flow."""
    logger, _log_file = setup_logging("pr-auto-approve")
    logger.info("Starting pr-auto-approve service (review mode)")

    # Acquire exclusive lock to prevent concurrent runs from exceeding approval_cap
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOCK_FILE, "w") as lock_fp:
        try:
            fcntl.flock(lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            logger.info("Another pr-auto-approve instance is running, exiting")
            return
        try:
            _run_review_inner(config_path, logger)
        finally:
            fcntl.flock(lock_fp, fcntl.LOCK_UN)


def _run_review_inner(config_path: Path, logger: logging.Logger) -> None:
    """Review flow after lock is acquired."""
    try:
        config = load_config(config_path)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        send_telegram(f"pr-auto-approve FATAL: {e}")
        return

    hostname = config.get("hostname", "")
    targets = config.get("targets", [])
    skip_bots = config.get("skip_bots", True)
    cli_wrappers = config.get("cli_wrappers", {})
    approval_cap = config.get("approval_cap", DEFAULT_APPROVAL_CAP)
    reviews_file = REVIEWS_FILE
    warnings: list[str] = []

    # Start glab proxy if needed
    has_gitlab = any(t.get("provider") == "gitlab" for t in targets)
    if has_gitlab and not start_glab_proxy(cli_wrappers, hostname, logger):
        send_telegram("pr-auto-approve FATAL: failed to start glab proxy", hostname=hostname)
        return

    try:
        _run_review_loop(targets, hostname, skip_bots, cli_wrappers, approval_cap, reviews_file, warnings, logger)
    finally:
        if has_gitlab:
            stop_glab_proxy(cli_wrappers, logger)

    if warnings:
        warn_text = format_warnings(warnings)
        logger.warning(warn_text)
        send_telegram(f"pr-auto-approve: {warn_text}", hostname=hostname)


def _run_review_loop(
    targets: list[dict],
    hostname: str,
    skip_bots: bool,
    cli_wrappers: dict,
    approval_cap: int,
    reviews_file: Path,
    warnings: list[str],
    logger: logging.Logger,
) -> None:
    """Inner review loop — separated so the finally block in run_review can clean up."""
    # Verify API access per provider
    provider_usernames: dict[str, str] = {}
    for target in targets:
        provider = target["provider"]
        if provider in provider_usernames:
            continue
        username = verify_api_access(provider, cli_wrappers)
        if not username:
            logger.error(f"API access failed for {provider}")
            send_telegram(f"pr-auto-approve FATAL: API access failed for {provider}", hostname=hostname)
            return
        provider_usernames[provider] = username
        logger.info(f"Verified {provider} access as {username}")

    today_approvals = count_today_approvals(reviews_file)
    if today_approvals >= approval_cap:
        logger.info(f"Approval cap reached ({today_approvals}/{approval_cap})")
        return
    remaining = approval_cap - today_approvals

    for target in targets:
        if remaining <= 0:
            break
        provider = target["provider"]
        org = target["org"]
        own_username = provider_usernames[provider]
        logger.info(f"Processing target: {provider}/{org}")

        repos = discover_repos(target, cli_wrappers, logger)
        if not repos:
            warnings.append(f"No repos found for {provider}/{org}")
            continue
        logger.info(f"Found repos: {repos}")

        for repo in repos:
            if remaining <= 0:
                break
            prs = list_prs(repo, provider, own_username, skip_bots, cli_wrappers, logger)
            if not prs:
                continue
            logger.info(f"Found {len(prs)} candidate PRs for {repo}")

            for pr in prs:
                if remaining <= 0:
                    break
                pr_number = pr["number"]
                logger.info(f"Evaluating {repo}#{pr_number}: {pr['title']}")

                if is_already_approved(repo, pr_number, own_username, provider, cli_wrappers, logger):
                    logger.info(f"Already approved {repo}#{pr_number}, skipping")
                    continue

                within, files, lines = check_complexity(pr, repo, pr_number, provider, cli_wrappers, logger)
                if not within:
                    logger.info(f"Too complex: {repo}#{pr_number} ({files} files, {lines} lines)")
                    summary = f"{files} files, {lines} lines"
                    append_review(
                        reviews_file,
                        repo,
                        pr_number,
                        pr["author"],
                        pr["title"],
                        summary,
                        "skipped-complexity",
                    )
                    continue

                diff = get_diff(repo, pr_number, provider, cli_wrappers)
                if not diff:
                    logger.warning(f"Could not get diff for {repo}#{pr_number}")
                    warnings.append(f"No diff for {repo}#{pr_number}")
                    continue

                if len(diff) > 8000:
                    logger.info(f"Diff too large for AI review: {repo}#{pr_number} ({len(diff)} chars)")
                    append_review(
                        reviews_file,
                        repo,
                        pr_number,
                        pr["author"],
                        pr["title"],
                        f"diff {len(diff)} chars",
                        "skipped-large-diff",
                    )
                    continue

                prompt = build_review_prompt(diff, pr["title"])
                try:
                    ai_result = call_ai_json(prompt)
                except AIError as e:
                    logger.warning(f"AI review failed for {repo}#{pr_number}: {e}")
                    warnings.append(f"AI failed for {repo}#{pr_number}")
                    continue

                if not isinstance(ai_result, dict):
                    logger.warning(f"AI returned non-dict for {repo}#{pr_number}: {type(ai_result).__name__}")
                    warnings.append(f"AI unexpected response for {repo}#{pr_number}")
                    continue

                decision = ai_result.get("decision", "skip")
                reason = ai_result.get("reason", "")
                logger.info(f"AI decision for {repo}#{pr_number}: {decision} — {reason}")

                if decision == "approve":
                    success = approve_pr(repo, pr_number, provider, cli_wrappers, logger)
                    action = "approved" if success else "approve-failed"
                    if success:
                        remaining -= 1
                    else:
                        warnings.append(f"Approval failed for {repo}#{pr_number}")
                else:
                    action = "skipped-ai"

                append_review(reviews_file, repo, pr_number, pr["author"], pr["title"], reason, action)


def run_day_summary(config_path: Path) -> None:
    """Day summary mode — read today's reviews and send telegram summary."""
    logger, _log_file = setup_logging("pr-auto-approve")
    logger.info("Starting pr-auto-approve service (day-summary mode)")

    try:
        config = load_config(config_path)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        send_telegram(f"pr-auto-approve FATAL: {e}")
        return

    hostname = config.get("hostname", "")
    entries = read_today_reviews(REVIEWS_FILE)
    summary = format_day_summary(entries)
    logger.info(f"Day summary: {len(entries)} reviews")
    send_telegram(summary, hostname=hostname)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="PR auto-approve service")
    parser.add_argument("--day-summary-only", action="store_true", help="Send end-of-day summary and exit")
    parser.add_argument("--tests", action="store_true", help="Run tests and exit")
    args = parser.parse_args()

    if args.tests:
        return

    config_path = Path(__file__).resolve().parent / "config.yaml"
    if args.day_summary_only:
        run_day_summary(config_path)
    else:
        run_review(config_path)


if __name__ == "__main__":
    if "--tests" in sys.argv:
        import unittest
        from unittest.mock import MagicMock, patch

        _mod = sys.modules[__name__]

        class TestLoadConfig(unittest.TestCase):
            def test_valid_config(self):
                import tempfile

                with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                    yaml.dump({"hostname": "test", "targets": [{"provider": "github", "org": "myorg"}]}, f)
                    f.flush()
                    config = load_config(Path(f.name))
                self.assertEqual(config["hostname"], "test")
                self.assertEqual(len(config["targets"]), 1)
                Path(f.name).unlink()

            def test_missing_config(self):
                with self.assertRaises(FileNotFoundError):
                    load_config(Path("/nonexistent/config.yaml"))

            def test_missing_targets_key(self):
                import tempfile

                with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                    yaml.dump({"hostname": "test"}, f)
                    f.flush()
                    with self.assertRaises(ValueError):
                        load_config(Path(f.name))
                Path(f.name).unlink()

            def test_empty_targets(self):
                import tempfile

                with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                    yaml.dump({"hostname": "test", "targets": []}, f)
                    f.flush()
                    with self.assertRaises(ValueError):
                        load_config(Path(f.name))
                Path(f.name).unlink()

        class TestDiscoverRepos(unittest.TestCase):
            @patch.object(_mod, "_run_cmd")
            def test_github_rest_api(self, mock_run):
                mock_run.return_value = MagicMock(returncode=0, stdout="org/repo1\norg/repo2\norg/repo3\n")
                logger = MagicMock()
                repos = _discover_github_repos("org", {}, logger)
                self.assertEqual(repos, ["org/repo1", "org/repo2", "org/repo3"])

            @patch.object(_mod, "_run_cmd")
            def test_github_fallback_to_search(self, mock_run):
                # First call (REST) fails, second call (search) succeeds
                mock_run.side_effect = [
                    MagicMock(returncode=1, stdout="", stderr="not found"),
                    MagicMock(returncode=0, stdout="org/repo-a\n"),
                ]
                logger = MagicMock()
                repos = _discover_github_repos("org", {}, logger)
                self.assertEqual(repos, ["org/repo-a"])
                self.assertEqual(mock_run.call_count, 2)

            @patch.object(_mod, "_run_cmd")
            def test_gitlab_repos(self, mock_run):
                mock_run.return_value = MagicMock(
                    returncode=0,
                    stdout=json.dumps(
                        [
                            {"path_with_namespace": "group/proj1"},
                            {"path_with_namespace": "group/proj2"},
                        ]
                    ),
                )
                logger = MagicMock()
                repos = _discover_gitlab_repos("group", {}, logger)
                self.assertEqual(repos, ["group/proj1", "group/proj2"])

            def test_filter_include_only(self):
                repos = ["org/infra-core", "org/infra-tools", "org/webapp"]
                result = _filter_repos(repos, include_only=["infra-"], exclude=[])
                self.assertEqual(result, ["org/infra-core", "org/infra-tools"])

            def test_filter_exclude(self):
                repos = ["org/infra-core", "org/archived-stuff", "org/webapp"]
                result = _filter_repos(repos, include_only=[], exclude=["archived-"])
                self.assertEqual(result, ["org/infra-core", "org/webapp"])

            def test_filter_combined(self):
                repos = ["org/infra-core", "org/infra-old", "org/webapp"]
                result = _filter_repos(repos, include_only=["infra-"], exclude=["old"])
                self.assertEqual(result, ["org/infra-core"])

            @patch.object(_mod, "_discover_github_repos")
            def test_discover_repos_limits_to_5(self, mock_discover):
                mock_discover.return_value = [f"org/repo{i}" for i in range(10)]
                logger = MagicMock()
                target = {"provider": "github", "org": "org"}
                repos = discover_repos(target, {}, logger)
                self.assertEqual(len(repos), 5)

        def _gh_pr(num, title, login, draft=False, adds=5, dels=3, files=2):
            """Helper to build a GitHub PR JSON object for tests."""
            return {
                "number": num,
                "title": title,
                "author": {"login": login},
                "isDraft": draft,
                "additions": adds,
                "deletions": dels,
                "changedFiles": files,
            }

        class TestListPrs(unittest.TestCase):
            @patch.object(_mod, "_run_cmd")
            def test_github_filters_drafts(self, mock_run):
                prs_json = json.dumps(
                    [
                        _gh_pr(1, "PR1", "user1"),
                        _gh_pr(2, "Draft", "user2", draft=True, adds=0, dels=0, files=0),
                    ]
                )
                mock_run.return_value = MagicMock(returncode=0, stdout=prs_json)
                logger = MagicMock()
                result = _list_github_prs("org/repo", "me", True, {}, logger)
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0]["number"], 1)

            @patch.object(_mod, "_run_cmd")
            def test_github_filters_own(self, mock_run):
                prs_json = json.dumps(
                    [
                        _gh_pr(1, "Own PR", "me"),
                        _gh_pr(2, "Other PR", "other"),
                    ]
                )
                mock_run.return_value = MagicMock(returncode=0, stdout=prs_json)
                logger = MagicMock()
                result = _list_github_prs("org/repo", "me", True, {}, logger)
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0]["author"], "other")

            @patch.object(_mod, "_run_cmd")
            def test_github_filters_bots(self, mock_run):
                prs_json = json.dumps(
                    [
                        _gh_pr(1, "Bot PR", "dependabot[bot]"),
                        _gh_pr(2, "Human PR", "dev"),
                    ]
                )
                mock_run.return_value = MagicMock(returncode=0, stdout=prs_json)
                logger = MagicMock()
                result = _list_github_prs("org/repo", "me", True, {}, logger)
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0]["author"], "dev")

            @patch.object(_mod, "_run_cmd")
            def test_github_keeps_bots_when_skip_false(self, mock_run):
                prs_json = json.dumps([_gh_pr(1, "Bot PR", "dependabot[bot]")])
                mock_run.return_value = MagicMock(returncode=0, stdout=prs_json)
                logger = MagicMock()
                result = _list_github_prs("org/repo", "me", False, {}, logger)
                self.assertEqual(len(result), 1)

            @patch.object(_mod, "_run_cmd")
            def test_gitlab_filters_drafts_and_own(self, mock_run):
                mrs_json = json.dumps(
                    [
                        {
                            "iid": 1,
                            "title": "MR1",
                            "author": {"username": "user1"},
                            "draft": False,
                            "work_in_progress": False,
                        },
                        {
                            "iid": 2,
                            "title": "Draft",
                            "author": {"username": "user2"},
                            "draft": True,
                            "work_in_progress": False,
                        },
                        {
                            "iid": 3,
                            "title": "Own",
                            "author": {"username": "me"},
                            "draft": False,
                            "work_in_progress": False,
                        },
                    ]
                )
                mock_run.return_value = MagicMock(returncode=0, stdout=mrs_json)
                logger = MagicMock()
                result = _list_gitlab_mrs("group/proj", "me", True, {}, logger)
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0]["number"], 1)

        class TestComplexityGate(unittest.TestCase):
            def test_github_within_limits(self):
                pr_data = {"changed_files": 3, "additions": 50, "deletions": 30}
                logger = MagicMock()
                within, files, lines = check_complexity(pr_data, "org/repo", 1, "github", {}, logger)
                self.assertTrue(within)
                self.assertEqual(files, 3)
                self.assertEqual(lines, 80)

            def test_github_too_many_files(self):
                pr_data = {"changed_files": 10, "additions": 50, "deletions": 30}
                logger = MagicMock()
                within, files, lines = check_complexity(pr_data, "org/repo", 1, "github", {}, logger)
                self.assertFalse(within)

            def test_github_too_many_lines(self):
                pr_data = {"changed_files": 2, "additions": 200, "deletions": 200}
                logger = MagicMock()
                within, files, lines = check_complexity(pr_data, "org/repo", 1, "github", {}, logger)
                self.assertFalse(within)

            @patch.object(_mod, "_fetch_gitlab_mr_stats", return_value=(3, 50))
            def test_gitlab_fetches_stats(self, mock_fetch):
                pr_data = {"changed_files": 0, "additions": 0, "deletions": 0}
                logger = MagicMock()
                within, files, lines = check_complexity(pr_data, "group/proj", 1, "gitlab", {}, logger)
                self.assertTrue(within)
                self.assertEqual(files, 3)
                self.assertEqual(lines, 50)
                mock_fetch.assert_called_once()

            @patch.object(_mod, "_fetch_gitlab_mr_stats", return_value=(10, 500))
            def test_gitlab_over_limits(self, mock_fetch):
                pr_data = {"changed_files": 0, "additions": 0, "deletions": 0}
                logger = MagicMock()
                within, files, lines = check_complexity(pr_data, "group/proj", 1, "gitlab", {}, logger)
                self.assertFalse(within)

            def test_boundary_at_limits(self):
                pr_data = {"changed_files": MAX_FILES, "additions": MAX_LINES, "deletions": 0}
                logger = MagicMock()
                within, _, _ = check_complexity(pr_data, "org/repo", 1, "github", {}, logger)
                self.assertTrue(within)

            def test_boundary_just_over(self):
                pr_data = {"changed_files": MAX_FILES + 1, "additions": 0, "deletions": 0}
                logger = MagicMock()
                within, _, _ = check_complexity(pr_data, "org/repo", 1, "github", {}, logger)
                self.assertFalse(within)

        class TestApprovalCap(unittest.TestCase):
            def test_count_approvals(self):
                import tempfile

                with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
                    today = datetime.now(UTC).strftime("%Y-%m-%d")
                    f.write("| Date | Repo | PR# | Author | Title | Summary | Action |\n")
                    f.write("|------|------|-----|--------|-------|---------|--------|\n")
                    f.write(f"| {today} | org/repo | #1 | dev | Fix | safe | approved |\n")
                    f.write(f"| {today} | org/repo | #2 | dev | Update | too complex | skipped-complexity |\n")
                    f.write(f"| {today} | org/repo | #3 | dev | Add | safe | approved |\n")
                    f.flush()
                    count = count_today_approvals(Path(f.name))
                self.assertEqual(count, 2)
                Path(f.name).unlink()

            def test_count_no_file(self):
                self.assertEqual(count_today_approvals(Path("/nonexistent/reviews.md")), 0)

            def test_count_ignores_other_dates(self):
                import tempfile

                with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
                    f.write("| Date | Repo | PR# | Author | Title | Summary | Action |\n")
                    f.write("|------|------|-----|--------|-------|---------|--------|\n")
                    f.write("| 2020-01-01 | org/repo | #1 | dev | Old | safe | approved |\n")
                    f.flush()
                    count = count_today_approvals(Path(f.name))
                self.assertEqual(count, 0)
                Path(f.name).unlink()

        class TestReviewLog(unittest.TestCase):
            def test_creates_file_with_header(self):
                import tempfile

                tmpdir = tempfile.mkdtemp()
                reviews_file = Path(tmpdir) / "reviews.md"
                append_review(reviews_file, "org/repo", 42, "dev", "Fix thing", "looks safe", "approved")
                content = reviews_file.read_text()
                self.assertIn("| Date | Repo |", content)
                self.assertIn("org/repo", content)
                self.assertIn("#42", content)
                self.assertIn("approved", content)
                import shutil

                shutil.rmtree(tmpdir)

            def test_appends_to_existing(self):
                import tempfile

                tmpdir = tempfile.mkdtemp()
                reviews_file = Path(tmpdir) / "reviews.md"
                append_review(reviews_file, "org/repo", 1, "dev", "First", "ok", "approved")
                append_review(reviews_file, "org/repo", 2, "dev", "Second", "ok", "skipped-ai")
                content = reviews_file.read_text()
                rows = [x for x in content.splitlines() if x.startswith("|") and "org/repo" in x]
                self.assertEqual(len(rows), 2)
                import shutil

                shutil.rmtree(tmpdir)

            def test_replaces_pipes_in_title(self):
                import tempfile

                tmpdir = tempfile.mkdtemp()
                reviews_file = Path(tmpdir) / "reviews.md"
                append_review(reviews_file, "org/repo", 1, "dev", "Fix | bar", "ok", "approved")
                content = reviews_file.read_text()
                self.assertIn("Fix / bar", content)
                self.assertNotIn("Fix | bar", content.split("\n")[-2])
                import shutil

                shutil.rmtree(tmpdir)

            def test_truncates_long_title(self):
                import tempfile

                tmpdir = tempfile.mkdtemp()
                reviews_file = Path(tmpdir) / "reviews.md"
                long_title = "A" * 100
                append_review(reviews_file, "org/repo", 1, "dev", long_title, "ok", "approved")
                content = reviews_file.read_text()
                # Title should be truncated to 60 chars
                lines = content.splitlines()
                data_line = [x for x in lines if "AAAA" in x][0]
                title_field = data_line.split("|")[5].strip()
                self.assertLessEqual(len(title_field), 60)
                import shutil

                shutil.rmtree(tmpdir)

        class TestReadTodayReviews(unittest.TestCase):
            def test_reads_today(self):
                import tempfile

                today = datetime.now(UTC).strftime("%Y-%m-%d")
                with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
                    f.write("| Date | Repo | PR# | Author | Title | Summary | Action |\n")
                    f.write("|------|------|-----|--------|-------|---------|--------|\n")
                    f.write(f"| {today} | org/repo | #10 | alice | Fix X | safe change | approved |\n")
                    f.write("| 2020-01-01 | org/old | #1 | bob | Old | old | approved |\n")
                    f.flush()
                    entries = read_today_reviews(Path(f.name))
                self.assertEqual(len(entries), 1)
                self.assertEqual(entries[0]["repo"], "org/repo")
                self.assertEqual(entries[0]["action"], "approved")
                Path(f.name).unlink()

            def test_empty_file(self):
                import tempfile

                with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
                    f.write("| Date | Repo | PR# | Author | Title | Summary | Action |\n")
                    f.write("|------|------|-----|--------|-------|---------|--------|\n")
                    f.flush()
                    entries = read_today_reviews(Path(f.name))
                self.assertEqual(len(entries), 0)
                Path(f.name).unlink()

        class TestDaySummary(unittest.TestCase):
            def test_no_reviews(self):
                result = format_day_summary([])
                self.assertIn("no reviews today", result)

            def test_groups_by_repo(self):
                e = {"summary": "ok"}
                entries = [
                    {
                        **e,
                        "repo": "org/repo1",
                        "pr_number": "#1",
                        "author": "alice",
                        "title": "Fix",
                        "action": "approved",
                    },
                    {
                        **e,
                        "repo": "org/repo1",
                        "pr_number": "#2",
                        "author": "bob",
                        "title": "Update",
                        "action": "skipped-ai",
                    },
                    {
                        **e,
                        "repo": "org/repo2",
                        "pr_number": "#5",
                        "author": "carol",
                        "title": "Add",
                        "action": "approved",
                    },
                ]
                result = format_day_summary(entries)
                self.assertIn("3 reviews", result)
                self.assertIn("org/repo1:", result)
                self.assertIn("org/repo2:", result)
                self.assertIn("#1 (alice)", result)
                self.assertIn("#5 (carol)", result)

        class TestBuildReviewPrompt(unittest.TestCase):
            def test_includes_title_and_diff(self):
                prompt = build_review_prompt("+ new line\n- old line", "Update IAM policy")
                self.assertIn("Update IAM policy", prompt)
                self.assertIn("+ new line", prompt)
                self.assertIn("security groups", prompt.lower())
                self.assertIn("approve", prompt)

            def test_includes_full_diff(self):
                # Diffs > 8000 chars are skipped upstream; build_review_prompt receives the full diff
                diff = "x" * 5000
                prompt = build_review_prompt(diff, "Big PR")
                self.assertIn(diff, prompt)

        class TestFormatWarnings(unittest.TestCase):
            def test_no_warnings(self):
                self.assertEqual(format_warnings([]), "")

            def test_single_warning(self):
                result = format_warnings(["API error for org/repo"])
                self.assertIn("Warnings:", result)
                self.assertIn("API error", result)

            def test_multiple_warnings(self):
                result = format_warnings(["warn1", "warn2"])
                self.assertIn("; ", result)

        class TestRunReview(unittest.TestCase):
            """Integration tests for the main review flow."""

            @patch.object(_mod, "send_telegram")
            @patch.object(_mod, "call_ai_json", return_value={"decision": "approve", "reason": "safe infra change"})
            @patch.object(_mod, "approve_pr", return_value=True)
            @patch.object(_mod, "get_diff", return_value="+ new_resource\n- old_resource\n")
            @patch.object(_mod, "is_already_approved", return_value=False)
            @patch.object(_mod, "list_prs")
            @patch.object(_mod, "discover_repos", return_value=["org/infra"])
            @patch.object(_mod, "verify_api_access", return_value="myuser")
            @patch.object(_mod, "setup_logging")
            def test_full_approve_flow(
                self,
                mock_log,
                mock_verify,
                mock_discover,
                mock_list,
                mock_approved,
                mock_diff,
                mock_approve,
                mock_ai,
                mock_tg,
            ):
                import tempfile

                mock_logger = MagicMock(spec=logging.Logger)
                mock_log.return_value = (mock_logger, Path(tempfile.mktemp(suffix=".log")))

                tmpdir = tempfile.mkdtemp()
                config_path = Path(tmpdir) / "config.yaml"
                reviews_path = Path(tmpdir) / "reviews.md"
                config = {
                    "hostname": "testhost",
                    "targets": [{"provider": "github", "org": "myorg"}],
                    "skip_bots": True,
                    "approval_cap": 2,
                }
                with open(config_path, "w") as f:
                    yaml.dump(config, f)

                mock_list.return_value = [
                    {
                        "number": 42,
                        "title": "Update terraform",
                        "author": "dev",
                        "additions": 10,
                        "deletions": 5,
                        "changed_files": 2,
                    }
                ]

                lock_path = Path(tmpdir) / ".lock"
                with patch.object(_mod, "REVIEWS_FILE", reviews_path), patch.object(_mod, "LOCK_FILE", lock_path):
                    run_review(config_path)

                mock_ai.assert_called_once()
                mock_approve.assert_called_once_with(
                    "org/infra",
                    42,
                    "github",
                    {},
                    mock_logger,
                )
                self.assertTrue(reviews_path.exists())
                content = reviews_path.read_text()
                self.assertIn("approved", content)
                self.assertIn("#42", content)

                import shutil

                shutil.rmtree(tmpdir)

            @patch.object(_mod, "send_telegram")
            @patch.object(_mod, "setup_logging")
            def test_config_error(self, mock_log, mock_tg):
                import tempfile

                mock_logger = MagicMock(spec=logging.Logger)
                mock_log.return_value = (mock_logger, Path(tempfile.mktemp(suffix=".log")))

                tmpdir = tempfile.mkdtemp()
                lock_path = Path(tmpdir) / ".lock"
                with patch.object(_mod, "LOCK_FILE", lock_path):
                    run_review(Path("/nonexistent/config.yaml"))
                import shutil

                shutil.rmtree(tmpdir)

                mock_logger.error.assert_called()
                fatal_calls = [c for c in mock_tg.call_args_list if "FATAL" in str(c)]
                self.assertGreater(len(fatal_calls), 0)

            @patch.object(_mod, "send_telegram")
            @patch.object(_mod, "call_ai_json", return_value={"decision": "approve", "reason": "safe"})
            @patch.object(_mod, "approve_pr", return_value=True)
            @patch.object(_mod, "get_diff", return_value="+ line\n")
            @patch.object(_mod, "is_already_approved", return_value=False)
            @patch.object(_mod, "list_prs")
            @patch.object(_mod, "discover_repos", return_value=["org/infra"])
            @patch.object(_mod, "verify_api_access", return_value="myuser")
            @patch.object(_mod, "setup_logging")
            def test_approval_cap_enforced(
                self,
                mock_log,
                mock_verify,
                mock_discover,
                mock_list,
                mock_approved,
                mock_diff,
                mock_approve,
                mock_ai,
                mock_tg,
            ):
                import tempfile

                mock_logger = MagicMock(spec=logging.Logger)
                mock_log.return_value = (mock_logger, Path(tempfile.mktemp(suffix=".log")))

                tmpdir = tempfile.mkdtemp()
                config_path = Path(tmpdir) / "config.yaml"
                reviews_path = Path(tmpdir) / "reviews.md"
                config = {
                    "hostname": "testhost",
                    "targets": [{"provider": "github", "org": "myorg"}],
                    "skip_bots": True,
                    "approval_cap": 1,
                }
                with open(config_path, "w") as f:
                    yaml.dump(config, f)

                pr_base = {"author": "dev", "additions": 5, "deletions": 3, "changed_files": 1}
                mock_list.return_value = [
                    {**pr_base, "number": 1, "title": "PR1"},
                    {**pr_base, "number": 2, "title": "PR2"},
                ]

                lock_path = Path(tmpdir) / ".lock"
                with patch.object(_mod, "REVIEWS_FILE", reviews_path), patch.object(_mod, "LOCK_FILE", lock_path):
                    run_review(config_path)

                # Only 1 approval due to cap
                self.assertEqual(mock_approve.call_count, 1)

                import shutil

                shutil.rmtree(tmpdir)

            @patch.object(_mod, "send_telegram")
            @patch.object(_mod, "setup_logging")
            def test_day_summary(self, mock_log, mock_tg):
                import tempfile

                mock_logger = MagicMock(spec=logging.Logger)
                mock_log.return_value = (mock_logger, Path(tempfile.mktemp(suffix=".log")))

                tmpdir = tempfile.mkdtemp()
                config_path = Path(tmpdir) / "config.yaml"
                reviews_path = Path(tmpdir) / "reviews.md"

                config = {"hostname": "testhost", "targets": [{"provider": "github", "org": "myorg"}]}
                with open(config_path, "w") as f:
                    yaml.dump(config, f)

                today = datetime.now(UTC).strftime("%Y-%m-%d")
                reviews_path.parent.mkdir(parents=True, exist_ok=True)
                reviews_path.write_text(
                    "| Date | Repo | PR# | Author | Title | Summary | Action |\n"
                    "|------|------|-----|--------|-------|---------|--------|\n"
                    f"| {today} | org/repo | #10 | dev | Fix X | safe | approved |\n"
                )

                with patch.object(_mod, "REVIEWS_FILE", reviews_path):
                    run_day_summary(config_path)

                mock_tg.assert_called()
                sent = mock_tg.call_args[0][0]
                self.assertIn("1 reviews", sent)
                self.assertIn("org/repo", sent)

                import shutil

                shutil.rmtree(tmpdir)

        class TestWarningCollection(unittest.TestCase):
            @patch.object(_mod, "send_telegram")
            @patch.object(_mod, "get_diff", return_value="")
            @patch.object(_mod, "is_already_approved", return_value=False)
            @patch.object(_mod, "list_prs")
            @patch.object(_mod, "discover_repos", return_value=["org/infra"])
            @patch.object(_mod, "verify_api_access", return_value="myuser")
            @patch.object(_mod, "setup_logging")
            def test_empty_diff_warning(
                self,
                mock_log,
                mock_verify,
                mock_discover,
                mock_list,
                mock_approved,
                mock_diff,
                mock_tg,
            ):
                import tempfile

                mock_logger = MagicMock(spec=logging.Logger)
                mock_log.return_value = (mock_logger, Path(tempfile.mktemp(suffix=".log")))

                tmpdir = tempfile.mkdtemp()
                config_path = Path(tmpdir) / "config.yaml"
                reviews_path = Path(tmpdir) / "reviews.md"
                config = {
                    "hostname": "testhost",
                    "targets": [{"provider": "github", "org": "myorg"}],
                    "approval_cap": 5,
                }
                with open(config_path, "w") as f:
                    yaml.dump(config, f)

                mock_list.return_value = [
                    {
                        "number": 1,
                        "title": "PR1",
                        "author": "dev",
                        "additions": 5,
                        "deletions": 3,
                        "changed_files": 1,
                    }
                ]

                lock_path = Path(tmpdir) / ".lock"
                with patch.object(_mod, "REVIEWS_FILE", reviews_path), patch.object(_mod, "LOCK_FILE", lock_path):
                    run_review(config_path)

                # Should log a warning about empty diff
                warning_calls = [c for c in mock_logger.warning.call_args_list if "diff" in str(c).lower()]
                self.assertGreater(len(warning_calls), 0)

                import shutil

                shutil.rmtree(tmpdir)

            @patch.object(_mod, "send_telegram")
            @patch.object(_mod, "discover_repos", return_value=[])
            @patch.object(_mod, "verify_api_access", return_value="myuser")
            @patch.object(_mod, "setup_logging")
            def test_no_repos_warning(self, mock_log, mock_verify, mock_discover, mock_tg):
                import tempfile

                mock_logger = MagicMock(spec=logging.Logger)
                mock_log.return_value = (mock_logger, Path(tempfile.mktemp(suffix=".log")))

                tmpdir = tempfile.mkdtemp()
                config_path = Path(tmpdir) / "config.yaml"
                reviews_path = Path(tmpdir) / "reviews.md"
                config = {
                    "hostname": "testhost",
                    "targets": [{"provider": "github", "org": "myorg"}],
                    "approval_cap": 5,
                }
                with open(config_path, "w") as f:
                    yaml.dump(config, f)

                lock_path = Path(tmpdir) / ".lock"
                with patch.object(_mod, "REVIEWS_FILE", reviews_path), patch.object(_mod, "LOCK_FILE", lock_path):
                    run_review(config_path)

                # Should log a warning about no repos
                warning_calls = [
                    c for c in mock_logger.warning.call_args_list if "No repos" in str(c) or "Warnings" in str(c)
                ]
                self.assertGreater(len(warning_calls), 0)

                import shutil

                shutil.rmtree(tmpdir)

        class TestGlabProxy(unittest.TestCase):
            @patch.object(_mod, "_run_cmd")
            def test_start_proxy_success(self, mock_run):
                mock_run.return_value = MagicMock(returncode=0)
                logger = MagicMock()
                result = start_glab_proxy({"glab": "/path/to/wrapper"}, "host", logger)
                self.assertTrue(result)
                mock_run.assert_called_once_with(["/path/to/wrapper", "--start-proxy", "host"], timeout=60)

            @patch.object(_mod, "_run_cmd")
            def test_start_proxy_failure(self, mock_run):
                mock_run.return_value = MagicMock(returncode=1, stderr="auth failed")
                logger = MagicMock()
                result = start_glab_proxy({"glab": "/path/to/wrapper"}, "host", logger)
                self.assertFalse(result)

            def test_no_wrapper_skips(self):
                logger = MagicMock()
                result = start_glab_proxy({}, "host", logger)
                self.assertTrue(result)

            @patch.object(_mod, "_run_cmd")
            def test_stop_proxy(self, mock_run):
                mock_run.return_value = MagicMock(returncode=0)
                logger = MagicMock()
                stop_glab_proxy({"glab": "/path/to/wrapper"}, logger)
                mock_run.assert_called_once_with(["/path/to/wrapper", "--stop-proxy"], timeout=60)

        class TestVerifyAccess(unittest.TestCase):
            @patch.object(_mod, "_run_cmd")
            def test_github_success(self, mock_run):
                mock_run.return_value = MagicMock(returncode=0, stdout="myuser\n")
                result = verify_api_access("github", {})
                self.assertEqual(result, "myuser")

            @patch.object(_mod, "_run_cmd")
            def test_github_failure(self, mock_run):
                mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="auth error")
                result = verify_api_access("github", {})
                self.assertIsNone(result)

            @patch.object(_mod, "_run_cmd")
            def test_gitlab_success(self, mock_run):
                mock_run.return_value = MagicMock(returncode=0, stdout=json.dumps({"username": "gluser"}))
                result = verify_api_access("gitlab", {})
                self.assertEqual(result, "gluser")

            def test_unknown_provider(self):
                result = verify_api_access("bitbucket", {})
                self.assertIsNone(result)

        class TestEncodeGitlabPath(unittest.TestCase):
            def test_encodes_slash(self):
                self.assertEqual(_encode_gitlab_path("group/project"), "group%2Fproject")

            def test_no_slash(self):
                self.assertEqual(_encode_gitlab_path("project"), "project")

        unittest.main(argv=[sys.argv[0]], exit=True)
    else:
        main()
