# Scheduled Services

Standalone Python scripts replacing Claude Code plugin skills. Business logic is algorithmic; AI (`claude -p`) is invoked only when genuinely needed (content analysis, summarization, code review).

Deployed via [umputun/spot](https://github.com/umputun/spot) to `~/.scheduled-services` with cron-only scheduling.

## Services

| Service | Description | Key dependencies |
|---------|-------------|-----------------|
| **news-digest** | Fetches RSS/Atom feeds, deduplicates articles, uses AI to generate per-group digests, sends via Telegram | `claude` |
| **pr-auto-approve** | Discovers open PRs/MRs across GitHub/GitLab orgs, applies complexity gates, uses AI for safety review, auto-approves infra changes | `claude`, `gh`, `glab` (if GitLab targets) |
| **slack-summary** | Dumps Slack channels via slackdump, resolves users, uses AI to summarize discussions, sends via Telegram | `claude`, `slackdump` |

## Directory Structure

```
common/helpers/          Shared Python modules (logging, telegram, ai)
services/<name>/         Each service: main script, config.yaml, .bindeps
scripts/                 Utility scripts (migration, etc.)
docs/plans/              Implementation plans
playbook.main.yaml       Spot deployment playbook
env.example-main.yml     Environment variable template
env.<host>-main.yml      Per-host configuration (not committed — see below)
```

## Per-Host Configuration

Each target machine gets its own env file: `env.<host>-main.yml`. These files define which services are enabled and their full configuration. The file format follows [spot](https://github.com/umputun/spot) conventions.

Each service has three env vars:
- `<PREFIX>_ENABLED` — `"true"` or `"false"`
- `<PREFIX>_SERVICE_CONFIG` — raw YAML written verbatim to `config.yaml`
- `<PREFIX>_CRON_SCHEDULE` — raw crontab entries (multiple lines supported)

See `env.example-main.yml` for the full schema and examples.

### Current hosts

| Host | Enabled services | Notes |
|------|-----------------|-------|
| saturn | news-digest | Personal feeds (politics, tech, sports, FL news) |
| angry-lobster | pr-auto-approve | GitHub — CorelCorp2-new (filtered repos) |
| polite-wombat | slack-summary | Pango Slack channels |
| speedy-elk | slack-summary | Flywire Slack channels |

## Prerequisites

### Global (all hosts)

| Tool | Purpose |
|------|---------|
| [spot](https://github.com/umputun/spot) | Deployment (runs on your local machine, not on targets) |
| [uv](https://docs.astral.sh/uv/) | Python package manager / runner |
| `python3 >= 3.11` | Runtime |
| `yq` | YAML validation during deploy |
| `rsync` | File sync during deploy |

### Per-service

Each service declares required binaries in its `.bindeps` file. The playbook validates these on the target before deploying.

| Service | Binary | Notes |
|---------|--------|-------|
| All services | `claude` | [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) — must be authenticated (`ANTHROPIC_API_KEY` in shell env) |
| pr-auto-approve | `gh` | [GitHub CLI](https://cli.github.com/) — must be authenticated (`gh auth login`) |
| pr-auto-approve | `glab` | [GitLab CLI](https://gitlab.com/gitlab-org/cli) — only if config has GitLab targets |
| slack-summary | `slackdump` | [slackdump](https://github.com/rusq/slackdump) — must be authenticated |

### Environment variables (on target)

| Variable | Required by | Notes |
|----------|-------------|-------|
| `ANTHROPIC_API_KEY` | All services (via `claude`) | Must be set in the shell environment (e.g. `.bashrc`/`.zshrc`) |
| `TELEPUSH_TOKEN` | All services | Telegram notification delivery via [Telepush](https://github.com/muety/telepush) |
| `SSL_CERT_FILE` or `REQUESTS_CA_BUNDLE` | Hosts behind corporate proxy | e.g. Zscaler CA cert path |

### CLI wrappers (`~/.bin/` PATH shadowing)

When a service needs to route CLI traffic through a proxy or add authentication (e.g. Cloudflare Access for self-hosted GitLab), place a wrapper script in `~/.bin/` that shadows the original binary. Ensure `~/.bin` is earlier in `PATH` than the real binary.

**pr-auto-approve — glab proxy support:**

If `cli_wrappers.glab: true` is set in the service config, the service calls:
1. `glab --start-proxy <hostname>` before processing GitLab targets
2. `glab <normal args>` for all API calls (routed through proxy by the wrapper)
3. `glab --stop-proxy` after processing completes

The wrapper script at `~/.bin/glab` must handle all three modes:
- `--start-proxy <host>` — authenticate and start a local proxy (e.g. Cloudflare Access tunnel), exit 0
- `--stop-proxy` — tear down the proxy
- Any other args — set required env vars (e.g. `HTTPS_PROXY`) and exec the real `glab`

The same pattern applies to `gh` if needed in the future.

## Deployment

1. Create `env.<host>-main.yml` from `env.example-main.yml`
2. Deploy:
   ```
   spot -p playbook.main.yaml -E env.<host>-main.yml
   ```

The playbook will:
- Validate all required binary dependencies (global + per-service `.bindeps`)
- Sync project files to `~/.scheduled-services/`
- Set up the Python virtualenv via `uv sync`
- Write each enabled service's `config.yaml` from the env file
- Install crontab entries under tagged blocks (`# BEGIN/END managed:scheduled-<service>`)
- Send a Telegram notification on completion

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
