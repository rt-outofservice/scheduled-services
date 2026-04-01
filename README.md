# Scheduled Services

Standalone Python scripts replacing Claude Code plugin skills. Business logic is algorithmic; AI (`claude -p`) is invoked only when genuinely needed (content analysis, summarization, code review).

Deployed via [umputun/spot](https://github.com/umputun/spot) to `~/.scheduled-services` with cron-only scheduling.

## Services

| Service | Description |
|---------|-------------|
| **news-digest** | Fetches RSS/Atom feeds, deduplicates articles, uses AI to generate per-group digests, sends via Telegram |
| **pr-auto-approve** | Discovers open PRs/MRs across GitHub/GitLab orgs, applies complexity gates, uses AI for safety review, auto-approves infra changes |
| **slack-summary** | Dumps Slack channels via slackdump, resolves users, uses AI to summarize discussions, sends via Telegram |

## Directory Structure

```
common/helpers/          Shared Python modules (logging, telegram, ai)
services/<name>/         Each service: main script, config.yaml, .bindeps
scripts/                 Utility scripts (migration, etc.)
docs/plans/              Implementation plans
playbook.main.yaml       Spot deployment playbook
env.example-main.yml     Environment variable template
```

## Adding a New Service

1. Create `services/<name>/` with a main Python script
2. Create `services/<name>/.bindeps` listing any required external binaries (one per line, empty if none)
3. The script should:
   - Accept `--tests` to run embedded unittest tests
   - Load config from `config.yaml` in its own directory
   - Import shared helpers via sys.path:
     ```python
     import sys
     from pathlib import Path
     sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common" / "helpers"))
     ```
   - Use `setup_logging(service_name)` for daily log files
   - Use `send_telegram()` for notifications
   - Never fail silently: fatal errors are logged AND sent via Telegram
4. Add the service to `env.example-main.yml` with `SERVICE_CONFIG`, `CRON_SCHEDULE`, and enable flag
5. Add the service to the `playbook.main.yaml` config generation and crontab sections

## Deployment

Prerequisites: [spot](https://github.com/umputun/spot), `uv`, `yq`, `python3 >= 3.11`, `claude` (Claude Code CLI), plus any per-service binary deps listed in `.bindeps` files.

1. Copy `env.example-main.yml` to `env.<host>-main.yml` and fill in real values
2. Deploy:
   ```
   spot -p playbook.main.yaml -E env.<host>-main.yml
   ```

The playbook will:
- Validate all required binary dependencies
- Sync project files to `~/.scheduled-services/`
- Set up the Python virtualenv via `uv sync`
- Write each service's `config.yaml` from the environment file
- Install crontab entries under tagged blocks (`# BEGIN/END managed:scheduled-<service>`)
- Send a Telegram notification on completion

## Running Tests Locally

Each Python script with business logic includes embedded tests:

```bash
# Run a single service's tests
uv run python services/news-digest/news_digest.py --tests

# Run all tests
uv run python common/helpers/log.py --tests
uv run python common/helpers/telegram.py --tests
uv run python common/helpers/ai.py --tests
uv run python services/news-digest/fetch_feeds.py --tests
uv run python services/news-digest/news_digest.py --tests
uv run python services/slack-summary/slack_summary.py --tests
uv run python services/pr-auto-approve/pr_auto_approve.py --tests

# Lint and format
uv run ruff check .
uv run ruff format --check .

# Pre-commit hooks
pre-commit run --all-files
```

## Migration from Claude Code Plugins

If you previously used the Claude Code plugin-based system:

```bash
bash scripts/migrate.sh
```

This idempotently removes:
- Old crontab entries tagged `managed:claude-*`
- macOS LaunchAgents matching `com.user.claude-*.plist`
- Old skills-wrapper at `~/.bin/claude-skills-wrapper/`
- Old Claude Code plugins (digest, git, summary)

Safe to run multiple times.
