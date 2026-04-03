# Teams Summary Service

## Overview

New scheduled service that reads pre-existing MS Teams JSON message dumps from the local filesystem, summarizes conversations per channel via AI (haiku), and posts to Telegram. Supports two modes: summary mode (default, matching slack-summary output format) and notify-on-match mode (keyword monitoring with contextual notification).

## Context

- Files involved:
  - Create: `services/teams-summary/teams_summary.py` (main script)
  - Create: `services/teams-summary/.bindeps` (empty, no external CLI tools needed)
  - Modify: `playbook.main.yaml` (add teams-summary deployment block)
  - Modify: `env.example-main.yml` (add teams-summary config/cron template)
- Related patterns: `services/slack-summary/slack_summary.py` (closest reference for AI prompts and telegram format), `services/news-digest/news_digest.py` (reference for --digest-names style CLI filtering)
- Dependencies: `pyyaml` (already available), `zoneinfo` (stdlib), common helpers (log, telegram, ai)

## Design Decisions

### File selection algorithm

Filenames follow: `<chat_name>_<lookback_mins>_<datetime>.json`. Parse using regex `^(.+?)_(\d+mins)_(\d{4}-\d{2}-\d{2}-\d{4})\.json$` to extract three groups: chat_name, lookback period, and creation datetime. This is more reliable than splitting since channel names may contain underscores and other special characters. Each file covers a time window: `[file_datetime - lookback, file_datetime]`. A file is selected if its window overlaps with the requested timeframe window `[now - timeframe, now]`. All times interpreted in America/New_York timezone.

### Date folder scanning

Only scan `yyyy-mm-dd` folders whose date falls within `[start_date - 1 day, today]` to avoid unnecessary I/O. The extra day buffer handles files created just after midnight that cover the previous day's messages.

### Empty file detection

Since JSON structure is complex, use a simple heuristic: skip files smaller than a configurable threshold (default 500 bytes). Remaining files are passed to AI as-is; if a file has structure but no substantive messages, AI will note "no activity" per its prompt instructions.

### Channel filtering

Config `channels` key (optional list of channel name substrings). If absent or empty, all discovered channels are processed. Matching is case-insensitive substring match against the parsed chat_name from the filename.

### Overlapping files

When multiple files for the same channel have overlapping time windows, all are included but the AI prompt explicitly instructs deduplication: "Messages may overlap between dumps; do not repeat information."

## Development Approach

- **Testing approach**: Regular (code first, then tests per task)
- Complete each task fully before moving to the next
- Follow existing service patterns exactly (argparse, config loading, logging, error handling, telegram format)
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**

## Implementation Steps

### Task 1: Service skeleton with config and CLI

**Files:**
- Create: `services/teams-summary/teams_summary.py`
- Create: `services/teams-summary/.bindeps`

- [x] Create `.bindeps` (empty file, no external tools needed)
- [x] Implement standard boilerplate: shebang, docstring, sys.path manipulation, common helper imports
- [x] Implement `load_config(config_path)` requiring: `data_dir` (path to JSON dumps root), `timeframe` (e.g. "14h"); optional: `hostname`, `channels` (list), `llm_provider`, `llm_model`, `llm_model_effort`, `min_file_size` (default 500)
- [x] Implement `parse_timeframe(timeframe)` reusing same format as slack-summary (Nh, Nm, Nd) but returning an ET-aware datetime pair (window_start, window_end)
- [x] Implement `main()` with argparse: `--tests`, `--timeframe <value>` (overrides config), `--notify-on-match <comma-separated>`. Wire up config loading, logging setup, and early exits for errors
- [x] Implement `if __name__ == "__main__"` block with --tests gating
- [x] Write tests: config loading (valid, missing keys, missing file), timeframe parsing (hours/minutes/days, invalid formats), CLI argument precedence for timeframe

### Task 2: File discovery and selection

**Files:**
- Modify: `services/teams-summary/teams_summary.py`

- [x] Implement `parse_filename(filename)` using regex `^(.+?)_(\d+mins)_(\d{4}-\d{2}-\d{2}-\d{4})\.json$` to extract three groups: chat_name, lookback minutes (strip "mins" suffix, convert to int), file datetime (parse `yyyy-mm-dd-HHMM` in America/New_York timezone). Return a named tuple or dataclass with fields: chat_name, start_time, end_time, path. Return None for non-matching filenames (log warning)
- [x] Implement `discover_files(data_dir, window_start, window_end, min_file_size)` that: scans only date folders overlapping the time window (with 1-day buffer), filters .json files by size threshold, parses each filename via regex, keeps files whose time range overlaps with the requested window
- [x] Implement `group_by_channel(file_infos, channels_filter)` that groups parsed file infos by chat_name, applies optional channel substring filtering (case-insensitive), and sorts files within each group chronologically
- [x] Write tests: filename parsing via regex (normal names, names with underscores and special chars, various lookback formats like 30mins/60mins/120mins, non-matching filenames return None), file discovery (mock filesystem with date folders, time window filtering), channel grouping and filtering, edge cases (no matching files, all files filtered out)

### Task 3: Summary mode (AI + Telegram)

**Files:**
- Modify: `services/teams-summary/teams_summary.py`

- [x] Implement `read_and_summarize_channel(channel_name, file_infos, config, logger)` that reads all JSON files for a channel, concatenates their content with file metadata headers, builds an AI prompt matching slack-summary style (narrative paragraphs, @mentions, bold channel headers, max 3500 chars, Telegram Markdown V1), calls `call_ai()` with configured provider/model (default haiku)
- [x] Implement `run_summary(config, args, logger)` orchestrating the full summary flow: discover files, group by channel, summarize each channel, assemble telegram message with header `\[hostname] *Teams Summary* (date)`, send via `send_telegram()`. Handle: no files found (log + skip), all channels empty after AI (send "no activity" message), partial failures per channel (collect warnings)
- [x] Write tests: AI prompt construction (verify prompt includes instructions for deduplication, narrative format, character limit), telegram message assembly (with/without hostname, with warnings), error handling (AI failure, no files found, empty results)

### Task 4: Notify-on-match mode

**Files:**
- Modify: `services/teams-summary/teams_summary.py`

- [x] Implement `run_notify_on_match(config, args, logger)` that: discovers and filters files same as summary mode, reads all relevant JSON content, builds an AI prompt asking to check if any of the provided keywords/phrases appear in the conversations. The prompt instructs the AI to return a JSON response with fields: `mentioned` (bool), `resolved` (bool), `context` (string summary of the discussion). Use `call_ai_json()` for structured response
- [x] Implement notification logic: if not mentioned -> do nothing (log only), if mentioned but resolved -> do nothing (log only), if mentioned and unresolved -> send telegram notification with context and ask user to step in. Format: `\[hostname] *Teams Alert* (date)\n\n<context from AI about the topic and why input may be helpful>`
- [x] Wire the mode selection in `main()`: if `--notify-on-match` is provided, call `run_notify_on_match()` instead of `run_summary()`
- [x] Write tests: AI prompt includes all keywords, JSON response parsing for all three outcomes (not mentioned, mentioned+resolved, mentioned+unresolved), telegram notification sent only for unresolved matches, error handling (AI failure, malformed JSON response)

### Task 5: Deployment integration

**Files:**
- Modify: `playbook.main.yaml`
- Modify: `env.example-main.yml`

- [x] Add teams-summary service block to playbook.main.yaml following the exact pattern of existing services (config generation from env var, cron injection, .bindeps validation)
- [x] Add `TEAMS_SUMMARY_ENABLED`, `TEAMS_SUMMARY_SERVICE_CONFIG`, `TEAMS_SUMMARY_CRON_SCHEDULE` entries to env.example-main.yml with example config showing all options (data_dir, timeframe, channels, hostname, llm_provider, llm_model)
- [x] Write tests: verify config.yaml example is valid YAML, verify all required config keys are present in example

### Task 6: Verify acceptance criteria

- [x] Run full test suite: `uv run python services/teams-summary/teams_summary.py --tests`
- [x] Run linter: `uv run ruff check .`
- [x] Run formatter: `uv run ruff format --check .`
- [x] Run pre-commit: `pre-commit run --all-files`

### Task 7: Update documentation

- [x] Update CLAUDE.md if any new patterns or CLI arguments introduced (add teams-summary to service CLI arguments section, add persistent state if any)
- [x] Move this plan to `docs/plans/completed/`
