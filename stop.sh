#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
stopped=0

for command_file in /proc/[0-9]*/cmdline; do
    [[ -r "$command_file" ]] || continue
    pid="${command_file#/proc/}"
    pid="${pid%/cmdline}"
    [[ "$pid" != "$$" ]] || continue

    process_root="$(readlink -f "/proc/$pid/cwd" 2>/dev/null || true)"
    [[ "$process_root" == "$ROOT" ]] || continue
    command_line="$(tr '\0' ' ' < "$command_file" 2>/dev/null || true)"

    if [[ "$command_line" == *"/build/server "* || "$command_line" == "./build/server "* ]]; then
        kill "$pid" 2>/dev/null && { printf 'Stopped Raft node (pid %s).\n' "$pid"; ((stopped += 1)); }
    elif [[ "$command_line" == *"gateway.py "* ]]; then
        kill "$pid" 2>/dev/null && { printf 'Stopped gateway (pid %s).\n' "$pid"; ((stopped += 1)); }
    elif [[ "$command_line" == *"-m http.server "* && "$command_line" == *"--directory web"* ]]; then
        kill "$pid" 2>/dev/null && { printf 'Stopped dashboard server (pid %s).\n' "$pid"; ((stopped += 1)); }
    fi
done

if ((stopped == 0)); then
    echo "No running project services found."
else
    printf 'Stopped %s project service(s). Runtime data was preserved.\n' "$stopped"
fi
