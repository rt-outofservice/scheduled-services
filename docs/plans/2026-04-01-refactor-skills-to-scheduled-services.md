# Refactor Claude Code Skills to Scheduled Services

## Overview

Migrate three Claude Code plugins (news-digest, pr-auto-approve, slack-summary) from AI-agent-driven skill execution to standalone Python scripts. Business logic is handled algorithmically; AI (`claude -p`) is invoked only for tasks that genuinely need it (content analysis, summarization, code review). Deployed via umputun/spot to `~/.scheduled-services` with cron-only scheduling on both macOS and Linux.

## Context

- Current skills: `/Users/rt/Projects/claude-code-plugins/{digest,git,summary}/skills/*/SKILL.md`
- Current wrapper: `/Users/rt/Projects/nest/spot/claude/bin/skills-wrapper/` (run.py, generate-cron.py, config.yaml template)
- Current spot playbook: `/Users/rt/Projects/nest/spot/claude/playbook.plugins.yaml`
- Existing reusable code: `fetch-feeds.py` (RSS/Atom parser, ~500 lines with tests)
- Target repo: `/Users/rt/Projects/scheduled-services`
- Deployment target: `~/.scheduled-services` on remote hosts
- Logs: `~/.logs/scheduled-services/<service_name>/`
- Related patterns: spot playbook conventions (on_error anchors, deploy versioning, tagged crontab entries, `tg()` telegram function)

## Development Approach

- **Testing approach**: Regular (code first, then embedded tests with `--tests` flag)
- Complete each task fully before moving to the next
- All Python scripts use `uv` for dependency management via root `pyproject.toml`
- Each script includes embedded `unittest` tests runnable with `--tests` argument
- Minimize AI calls — script everything that can be reasonably scripted and tested
- Common utilities imported via `sys.path` manipulation (scripts add `../../common/helpers` to path)
- `common/` is a general-purpose shared directory with subfolders: `common/helpers/` for Python helper modules, other subfolders added as needed for shared configs, templates, etc.
- **No schedule/time validation in Python scripts** — cron handles all scheduling. If a script is triggered, it executes. Variant behaviors (e.g. day summary) are controlled via CLI arguments from separate cron entries.
- **Never fail silently** — all fatal errors are logged AND sent via telegram. Non-fatal errors (failed feeds, API timeouts) are logged and appended to the normal telegram message so the user is always informed without needing to check logs.
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**

## Implementation Steps

### Task 1: Project scaffolding and common utilities

**Files:**
- Create: `pyproject.toml`
- Create: `.pre-commit-config.yaml`
- Create: `CLAUDE.md`
- Create: `common/helpers/__init__.py`
- Create: `common/helpers/logging.py`
- Create: `common/helpers/telegram.py`
- Create: `common/helpers/ai.py`

- [x] Create `pyproject.toml` — uv project, Python >=3.11, runtime deps (pyyaml), dev deps (ruff, yamllint, pre-commit)
- [x] Create `.pre-commit-config.yaml` — hooks: ruff (lint + format), yamllint, shellcheck, custom hook running `--tests` on Python files that contain embedded test classes
- [x] Create `CLAUDE.md` — project conventions: directory layout, testing pattern, config.yaml generation approach, logging conventions, how `common/helpers/` imports work, error handling philosophy (never silent)
- [x] Create `common/helpers/__init__.py` — package init, re-exports key functions for convenience
- [x] Create `common/helpers/logging.py` — `setup_logging(service_name)` configures daily log file at `~/.logs/scheduled-services/<service_name>/<service_name>_MM-DD-YYYY.log`, keeps 30 most recent files per service (deletes older), returns configured `logging.Logger` and log file path. Redirects stdout/stderr to the log file as well.
- [x] Create `common/helpers/telegram.py` — `send_telegram(message, hostname="")` sources `tg()` from `~/.bash.d/telegram.bash` or `~/.zsh.d/telegram.zsh` and calls it via bash subprocess. Prepends `*hostname* — ` prefix when hostname is set. Auto-splits messages exceeding 4000 chars. Escapes Markdown V1 special chars (`*`, `_`, `` ` ``, `[`) in content portions.
- [x] Create `common/helpers/ai.py` — `call_ai(prompt, model="", timeout=600)` invokes `claude -p` subprocess, returns stdout. Raises `AIError` on non-zero exit. `call_ai_json(prompt, model="")` variant that parses JSON from response.
- [x] Write embedded tests for all three common helper modules
- [x] Run tests: `uv run python common/helpers/logging.py --tests && uv run python common/helpers/telegram.py --tests && uv run python common/helpers/ai.py --tests`

### Task 2: news-digest service

**Files:**
- Create: `services/news-digest/news_digest.py`
- Create: `services/news-digest/fetch_feeds.py` (adapted from existing plugin)
- Create: `services/news-digest/.bindeps`

- [ ] Adapt `fetch_feeds.py` from `/Users/rt/Projects/claude-code-plugins/digest/scripts/fetch-feeds.py`. Keep RSS/Atom parsing, date handling, time-window filtering. Make it importable as a module (`fetch_all(groups_dict, hours) -> dict`) while keeping CLI entry point. Preserve and adapt existing tests.
- [ ] Create `news_digest.py` — main script:
  1. Parse CLI args: `--digest-names` (comma-separated list of digest group names to process; if omitted, process all groups from config), `--tests` (run tests and exit)
  2. Load `config.yaml` (hostname, feed groups with lang/mode/subcategories settings, hours window per group)
  3. Filter feed groups to only those matching `--digest-names` if provided
  4. For each selected feed group: call `fetch_feeds.fetch_all()` to get items within the configured time window
  5. Dedup against earlier runs: read today's log file, extract previously sent headlines, filter out items matching by normalized string comparison. Collect a warnings list for any feeds that returned errors.
  6. Detailed mode: shorten article URLs via spoo.me POST API with rate limiting (max 5/sec), fall back to original URL on failure (log warning)
  7. Call AI (`claude -p`) per group with fetched items as JSON — prompt specifies: language, output format (detailed: cross-source analysis with headlines + URLs; summary: narrative per subcategory), max length constraints
  8. Send each group's digest via `send_telegram()`, truncate from bottom if >4000 chars. Append a brief warnings section at the end if any feeds/URLs failed (e.g. "Note: 2 feeds unavailable, URL shortener failed for 3 links")
  9. Fatal errors (config missing, all feeds in a group failed, AI call failed) — log and send telegram error notification immediately
- [ ] Create `.bindeps` — empty file (no external binary deps)
- [ ] Write embedded tests: config loading, `--digest-names` filtering, dedup matching, URL shortening (mocked HTTP), message truncation, AI prompt construction, warning collection/formatting
- [ ] Run tests: `uv run python services/news-digest/news_digest.py --tests`

### Task 3: slack-summary service

**Files:**
- Create: `services/slack-summary/slack_summary.py`
- Create: `services/slack-summary/.bindeps`

- [ ] Create `slack_summary.py` — main script:
  1. Parse CLI args: `--tests` (run tests and exit)
  2. Load `config.yaml` (hostname, channels list, timeframe, user_id)
  3. Validate slackdump auth: test dump against first channel with `-time-from` set to now (zero messages). On failure: send telegram asking user to re-auth, retry every 60s up to 10 times, send success/give-up notification
  4. Compute absolute timestamp from timeframe (e.g., `14h` -> ISO 8601 UTC)
  5. Dump each channel via `slackdump dump -time-from <ts> -o /tmp/slack-summary/ <channel>`, sleep 5s between channels. Log and collect warnings for any channels that fail (continue with remaining).
  6. Parse dumped `<channel>.json` files: extract messages, filter out non-message types and bot subtypes, extract user/text/timestamp per message
  7. Resolve user IDs: maintain persistent cache at `~/.scheduled-services/services/slack-summary/user-cache.txt` (USERID=DisplayName per line), resolve uncached IDs via `slackdump list users`, sleep 1s between API calls
  8. Detect mentions (`<@user_id>` in text, if user_id configured) and DM channels (D-prefix)
  9. Call AI (`claude -p`) with parsed messages per channel — prompt: generate narrative summary focusing on technical/operational topics, include mentions/DMs section at top, attribute to @DisplayName, skip casual chat
  10. Format and send via `send_telegram()`, split if >4000 chars. Append warnings section if any channels failed to dump or had parse errors.
  11. Clean up `/tmp/slack-summary/`
  12. Fatal errors (slackdump auth permanently failed, config missing) — log and send telegram error notification immediately
- [ ] Create `.bindeps` — contains: `slackdump`
- [ ] Write embedded tests: auth validation flow (mocked subprocess), message parsing/filtering, user cache read/write, mention detection, DM detection, timestamp computation, warning collection
- [ ] Run tests: `uv run python services/slack-summary/slack_summary.py --tests`

### Task 4: pr-auto-approve service

**Files:**
- Create: `services/pr-auto-approve/pr_auto_approve.py`
- Create: `services/pr-auto-approve/.bindeps`

- [ ] Create `pr_auto_approve.py` — main script with two modes controlled by CLI args:
  - `--day-summary-only`: read today's entries from `reviews.md`, compose and send end-of-day telegram summary grouped by repo, then exit. Called by a separate cron entry at end of day.
  - Default (no flag): normal review mode (steps below)
  - `--tests`: run tests and exit

  1. Load `config.yaml` (hostname, targets as provider/org pairs, timeframe, skip_bots, filters with include_only/exclude, cli_wrappers, approval_cap default 2)
  2. If glab cli_wrapper configured: run `<wrapper> --start-proxy <hostname>`, send telegram error and exit on failure
  3. Verify API access: `gh api user --jq .login` / `glab api user --jq .username`, store own username. On failure: send telegram error and exit.
  4. For each target: find up to 5 recently updated repos via API search (with fallback for GitHub search lag), apply include_only/exclude filters. Log and collect warnings for any targets with API errors (continue with remaining).
  5. For each repo: list open non-draft PRs/MRs via `gh pr list` / `glab mr list`, skip own, skip bot-authored if skip_bots is true
  6. Algorithmic gates per PR/MR:
     - Already approved by us: check via reviews API, skip if yes
     - Complexity: parse diff to count files changed and lines added/removed, skip if >7 files or >300 lines
  7. For PRs passing gates and under approval cap: call AI (`claude -p`) with the diff — prompt: assess if infra/DevOps scope and if safe (check for open security groups, hardcoded secrets, overpermissive IAM, dangerous ops, obvious logic errors). Return JSON `{"decision": "approve"|"skip", "reason": "..."}`
  8. If AI approves: `gh pr review --approve` / `glab mr approve` (silent, no comment ever)
  9. Append every reviewed PR/MR to `~/.scheduled-services/services/pr-auto-approve/reviews.md` (append-only markdown table: date, repo, PR#, author, title, summary, action)
  10. Cleanup: if glab wrapper was started, run `<wrapper> --stop-proxy`
  11. If any warnings were collected (API errors for certain orgs, AI failures for certain PRs), log all and include brief summary in any telegram notifications
- [ ] Create `.bindeps` — contains: `gh` and `glab`
- [ ] Write embedded tests: repo discovery parsing (mocked gh/glab JSON output), PR filtering (bots, drafts, own username), complexity gate (parse diff stat output), approval cap enforcement, review log append format, AI response JSON parsing, day-summary formatting, warning collection
- [ ] Run tests: `uv run python services/pr-auto-approve/pr_auto_approve.py --tests`

### Task 5: Spot playbook and environment config

**Files:**
- Create: `playbook.main.yaml`
- Create: `env.example-main.yml`

- [ ] Create `env.example-main.yml` — documented example. Per-service variables use two keys:
  - `SERVICE_CONFIG: |` — raw YAML content that gets written verbatim to `config.yaml` for that service
  - `CRON_SCHEDULE: |` — raw crontab content (one or more lines) written verbatim into the crontab. This allows multiple entries per service (e.g. every 30 min for normal runs + one end-of-day entry with `--day-summary-only`). Each line is a standard cron expression + command.
  - Per-service enable flag (e.g. `NEWS_DIGEST_ENABLED: true`)
  - Example showing news-digest with multiple cron entries for different `--digest-names` at different schedules
  - Example showing pr-auto-approve with a regular interval entry + a separate `--day-summary-only` entry
- [ ] Create `playbook.main.yaml` with task groups:
  1. **preflight**: verify playbook directory, compute deploy version (`git log -1 --format='%h@%cs'`)
  2. **validate-deps**: for each enabled service, read its `.bindeps` file and verify each binary exists via `command -v`. Also verify `uv`, `yq`, `python3` are present. Fail with clear message listing all missing tools. Use spot `script` command — no dependency installation.
  3. **deploy-files**: use spot `sync` to copy `common/`, enabled `services/<name>/` directories, and `pyproject.toml` to `~/.scheduled-services/`. Use spot `copy` for the deploy version file.
  4. **setup-venv**: run `cd ~/.scheduled-services && uv sync --native-tls` to create/update virtualenv
  5. **generate-configs**: for each enabled service, write the `SERVICE_CONFIG` value as-is to `~/.scheduled-services/services/<name>/config.yaml`. Use `yq` only for validation (valid YAML check), not for assembly.
  6. **configure-crontab**: for each enabled service, take its `CRON_SCHEDULE` content and install it into crontab under tagged blocks (`# BEGIN managed:scheduled-<service>` / `# END managed:scheduled-<service>`). Remove stale tagged blocks before adding new ones. Validate cron syntax via `crontab -T` on Linux; on macOS do basic regex validation of the 5-field schedule. Each cron command should be: `cd ~/.scheduled-services && uv run python services/<name>/<script>.py <args>`.
  7. **notify-completion**: send telegram with deploy version and list of enabled services
- [ ] Use spot built-in commands (`copy`, `sync`) wherever possible; minimize bash in `script` blocks
- [ ] Use YAML anchor `&on_error` for shared telegram error handler across all task groups
- [ ] Validate playbook syntax: `spot -p playbook.main.yaml --dry` (or equivalent check)

### Task 6: Migration script

**Files:**
- Create: `scripts/migrate.sh`

- [ ] Create `migrate.sh` — idempotent cleanup of old plugin-based system:
  1. Remove old crontab entries tagged `managed:claude-*` (parse and rewrite crontab)
  2. On macOS: `launchctl bootout` and delete plists matching `com.user.claude-*.plist` from `~/Library/LaunchAgents/`
  3. Remove old skills-wrapper at `~/.bin/claude-skills-wrapper/`
  4. Disable old claude-code plugins: `claude plugin disable digest git summary` (best-effort, continue on failure)
  5. Print summary of what was removed vs what was already absent
- [ ] Script must be runnable standalone (`bash scripts/migrate.sh`) and via spot as an ad-hoc task
- [ ] Ensure shellcheck compliance
- [ ] Test idempotency: running twice produces no errors and "already clean" messages on second run

### Task 7: Verify acceptance criteria

- [ ] Run full test suite across all Python files with embedded tests
- [ ] Run ruff lint and format check: `uv run ruff check . && uv run ruff format --check .`
- [ ] Run yamllint on all YAML files: `yamllint playbook.main.yaml env.example-main.yml`
- [ ] Run shellcheck on migration script: `shellcheck scripts/migrate.sh`
- [ ] Run pre-commit on all files: `pre-commit run --all-files`

### Task 8: Update documentation

- [ ] Update README.md with: project overview, directory structure, how to add a new service, deployment instructions (`spot -p playbook.main.yaml -E env.<host>-main.yml`), how to run tests locally, migration steps
- [ ] Move this plan to `docs/plans/completed/`
