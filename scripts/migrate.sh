#!/usr/bin/env bash
# migrate.sh — idempotent cleanup of old Claude Code plugin-based system
#
# Removes:
#   - Old crontab entries tagged managed:claude-*
#   - macOS LaunchAgents matching com.user.claude-*.plist
#   - Old skills-wrapper directory (~/.bin/claude-skills-wrapper/)
#   - Old Claude Code plugins (digest, git, summary)
#
# Safe to run multiple times. Prints summary of actions taken.
#
# Usage:
#   bash scripts/migrate.sh          # standalone
#   spot ... -t migrate              # via spot ad-hoc task

set -euo pipefail

removed=()
skipped=()

# ── 1. Remove old crontab entries tagged managed:claude-* ───────────────
remove_old_crontab_entries() {
    local current
    current=$(crontab -l 2>/dev/null || true)

    if [[ -z "$current" ]]; then
        skipped+=("crontab: no crontab installed")
        return 0
    fi

    if ! printf '%s\n' "$current" | grep -q '# BEGIN managed:claude-'; then
        skipped+=("crontab: no managed:claude-* entries found")
        return 0
    fi

    local cleaned
    cleaned=$(printf '%s\n' "$current" | awk '
        /^# BEGIN managed:claude-/ { skip=1; next }
        /^# END managed:claude-/   { skip=0; next }
        !skip { print }
    ')

    # Trim trailing blank lines
    cleaned=$(printf '%s' "$cleaned" | sed -e :a -e '/^\n*$/{$d;N;ba;}')

    if [[ -z "$cleaned" ]]; then
        # Crontab would be empty — remove it entirely
        crontab -r 2>/dev/null || true
    else
        printf '%s\n' "$cleaned" | crontab -
    fi

    removed+=("crontab: removed managed:claude-* entries")
}

# ── 2. Remove macOS LaunchAgents ────────────────────────────────────────
remove_launch_agents() {
    if [[ "$(uname -s)" != "Darwin" ]]; then
        skipped+=("LaunchAgents: not macOS — skipped")
        return 0
    fi

    local agents_dir="$HOME/Library/LaunchAgents"
    if [[ ! -d "$agents_dir" ]]; then
        skipped+=("LaunchAgents: directory does not exist")
        return 0
    fi

    local found_any=false
    local plist
    for plist in "$agents_dir"/com.user.claude-*.plist; do
        [[ -e "$plist" ]] || continue
        found_any=true

        local label
        label=$(basename "$plist" .plist)

        # Try to bootout (unload) the agent; ignore errors if not loaded
        launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true

        rm -f "$plist"
        removed+=("LaunchAgent: removed $label")
    done

    if [[ "$found_any" == "false" ]]; then
        skipped+=("LaunchAgents: no com.user.claude-*.plist found")
    fi
}

# ── 3. Remove old skills-wrapper ────────────────────────────────────────
remove_skills_wrapper() {
    local wrapper_dir="$HOME/.bin/claude-skills-wrapper"

    if [[ -d "$wrapper_dir" ]]; then
        rm -rf "$wrapper_dir"
        removed+=("skills-wrapper: removed $wrapper_dir")
    else
        skipped+=("skills-wrapper: $wrapper_dir does not exist")
    fi
}

# ── 4. Disable old Claude Code plugins ──────────────────────────────────
disable_old_plugins() {
    if ! command -v claude &>/dev/null; then
        skipped+=("plugins: claude CLI not found — skipped")
        return 0
    fi

    local plugin
    for plugin in digest git summary; do
        if claude plugin disable "$plugin" 2>/dev/null; then
            removed+=("plugin: disabled $plugin")
        else
            skipped+=("plugin: $plugin already disabled or not installed")
        fi
    done
}

# ── Run all cleanup steps ───────────────────────────────────────────────
remove_old_crontab_entries
remove_launch_agents
remove_skills_wrapper
disable_old_plugins

# ── Print summary ───────────────────────────────────────────────────────
echo ""
echo "=== Migration Summary ==="
echo ""

if [[ ${#removed[@]} -gt 0 ]]; then
    echo "Removed:"
    for item in "${removed[@]}"; do
        echo "  - $item"
    done
else
    echo "Nothing to remove — already clean."
fi

if [[ ${#skipped[@]} -gt 0 ]]; then
    echo ""
    echo "Skipped (already absent):"
    for item in "${skipped[@]}"; do
        echo "  - $item"
    done
fi

echo ""
echo "Migration complete."
