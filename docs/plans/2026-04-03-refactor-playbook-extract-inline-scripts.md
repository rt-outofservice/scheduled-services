# Refactor Playbook: Extract Inline Scripts to Testable Python Modules

## Overview

Extract the three largest inline script blocks from `playbook.main.yaml` (launchd: 188 lines, crontab: 82 lines, config generation: 31 lines) into standalone, testable Python scripts. Simplify all remaining inline scripts to 10 lines or fewer. Remove `yq` as a remote dependency (all YAML handling moves to PyYAML in the new scripts). Target: playbook shrinks from 519 lines to ~150 lines.

## Context

- Files involved:
  - `playbook.main.yaml` — main deployment playbook (519 lines, 12 inline scripts)
  - `templates/launchd.plist.tpl` — LaunchAgent plist template
  - `scripts/migrate.sh` — existing migration utility (not modified)
  - `README.md`, `CLAUDE.md` — documentation
- Related patterns: embedded `--tests` unittest pattern, `sys.path` helper imports
- Dependencies: `pyyaml` (already in pyproject.toml)
- Spot limitations: non-interactive PATH, `set -e`, BSD vs GNU sed, tilde expansion (see memory files)

## Development Approach

- **Testing approach**: TDD for core logic (schedule expansion, crontab parsing, config writing), regular for CLI glue
- Complete each task fully before moving to the next
- New scripts follow project conventions: `--tests` flag, `argparse`, `#!/usr/bin/env python3`
- Scripts are deployment utilities (not services) so they do NOT import from `common/helpers/`
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**

## Implementation Steps

### Task 1: Create `scripts/install_launchd.py`

**Files:**
- Create: `scripts/install_launchd.py`

Standalone Python script replacing the 188-line inline bash block in the `configure-launchd` task. Uses PyYAML for YAML parsing (replacing yq), `xml.etree` or string formatting for plist XML, and subprocess for `launchctl`/`plutil`.

CLI interface:
```
install_launchd.py [--tests] --template PATH --agents-dir PATH --prefix-tag TAG \
  svc:PREFIX [svc:PREFIX ...]
```
Reads `<PREFIX>_ENABLED` and `<PREFIX>_LAUNCHD_SCHEDULE` from environment variables.

- [x] Implement schedule YAML parsing and cartesian-product expansion (replaces inline Python at playbook lines 328-353)
- [x] Implement plist generation from template: placeholder substitution, XML-escaping commands, StartCalendarInterval XML building (replaces build_plist function, lines 357-405)
- [x] Implement agent lifecycle: remove existing agents, validate with plutil, bootstrap with launchctl (replaces install_service function, lines 410-487)
- [x] Implement CLI argument parsing and main entry point
- [x] Write embedded tests: schedule expansion (scalar + array + mixed), XML generation, plist template rendering, label naming (single-job vs multi-job), error cases
- [x] Run tests: `uv run python scripts/install_launchd.py --tests`

### Task 2: Create `scripts/install_crontab.py`

**Files:**
- Create: `scripts/install_crontab.py`

Standalone Python script replacing the 82-line inline bash block in `configure-crontab`. Handles managed block lifecycle and cron syntax validation.

CLI interface:
```
install_crontab.py [--tests] svc:PREFIX [svc:PREFIX ...]
```
Reads `<PREFIX>_ENABLED` and `<PREFIX>_CRON_SCHEDULE` from environment variables.

- [x] Implement crontab reading (subprocess `crontab -l`) and managed-block stripping (`# BEGIN/END managed:scheduled-*`)
- [x] Implement new managed-block building with PATH header and schedule lines
- [x] Implement cron syntax validation (regex-based, matching current playbook logic at lines 289-300)
- [x] Implement crontab installation (`crontab -`) and empty-crontab handling (`crontab -r`)
- [x] Implement CLI argument parsing and main entry point
- [x] Write embedded tests: block stripping, entry building, syntax validation (valid + invalid lines), merge workflow, empty crontab edge case
- [x] Run tests: `uv run python scripts/install_crontab.py --tests`

### Task 3: Create `scripts/write_configs.py`

**Files:**
- Create: `scripts/write_configs.py`

Standalone Python script replacing the 31-line inline bash block in `generate-configs`. Writes and validates service config files.

CLI interface:
```
write_configs.py [--tests] --base-dir PATH svc:PREFIX [svc:PREFIX ...]
```
Reads `<PREFIX>_ENABLED` and `<PREFIX>_SERVICE_CONFIG` from environment variables.

- [x] Implement config reading from env vars, YAML writing with validation (PyYAML), and permission setting (mode 600)
- [x] Implement CLI argument parsing and main entry point
- [x] Write embedded tests: config writing and reading back, YAML validation (valid + invalid), permission verification, skip when no config, missing directory handling
- [x] Run tests: `uv run python scripts/write_configs.py --tests`

### Task 4: Refactor `playbook.main.yaml`

**Files:**
- Modify: `playbook.main.yaml`

Replace large inline scripts with calls to new Python scripts. Compress all remaining inline scripts to 10 lines or fewer.

- [x] Update `sync-project` exclude: change `"scripts"` to `"scripts/migrate.sh"` so deployment scripts are synced to remote
- [x] Replace `generate-configs` (31 lines) with ~5-line script calling `uv run python ~/.scheduled-services/scripts/write_configs.py`
- [x] Replace `configure-crontab` (82 lines) with ~5-line script calling `uv run python ~/.scheduled-services/scripts/install_crontab.py`
- [x] Replace `configure-launchd` (188 lines, including the separate `copy-plist-template` command) with ~8-line script calling `uv run python ~/.scheduled-services/scripts/install_launchd.py`
- [x] Compress `platform-checks` from 30 to ~8 lines using compact associative-array loop
- [x] Simplify `validate-deps` from 28 to ~10 lines: remove `yq` from required tools list, streamline the .bindeps check loop
- [x] Simplify `notify-completion` from 19 to ~10 lines
- [x] Verify PATH export is present in all remaining inline scripts (one line per block, acceptable overhead)

### Task 5: Update documentation

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`

- [x] Update README.md: remove `yq` from prerequisites table, add deployment scripts to directory structure, update "Adding a New Service" section to mention registering in deployment scripts
- [x] Update CLAUDE.md: update directory layout, add deployment scripts info, add test commands for new scripts
- [x] Review and update any references to playbook inline logic that no longer applies

### Task 6: Verify acceptance criteria

- [ ] Run all existing service tests (`uv run python services/*/[!_]*.py --tests` and `uv run python common/helpers/*.py --tests`)
- [ ] Run all new script tests (`uv run python scripts/install_launchd.py --tests`, `scripts/install_crontab.py --tests`, `scripts/write_configs.py --tests`)
- [ ] Run linter: `uv run ruff check .`
- [ ] Run formatter: `uv run ruff format --check .`
- [ ] Verify no inline script block in playbook exceeds 10 lines (excluding comments and the PATH export line)
- [ ] Verify `yq` is no longer listed in required remote tools

### Task 7: Update documentation

- [ ] Move this plan to `docs/plans/completed/`
