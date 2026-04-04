# Local bridge skeleton for OpenClaw -> Codex / Claude Code

This folder gives you a minimal Windows-side bridge so OpenClaw can hand work to local coding tools through task packets rather than GUI automation.

## Files

- `watch_tasks.py` — polls `ops/agent_state/tasks.yaml` and dispatches queued tasks
- `run_codex_task.ps1` — wrapper for Codex CLI
- `run_claude_task.ps1` — wrapper for Claude Code CLI

## Expected repo layout

Copy these files into your repo like this:

```text
<repo-root>/
  ops/
    agent_state/
      tasks.yaml
    handoffs/
      incoming/
      in_progress/
      blocked/
      done/
    logs/
  scripts/
    watch_tasks.py
    run_codex_task.ps1
    run_claude_task.ps1
```

## Install Python dependency

```powershell
pip install pyyaml
```

## Dry run

```powershell
python .\scripts\watch_tasks.py --root . --once --dry-run
```

## Real run

```powershell
python .\scripts\watch_tasks.py --root .
```

## What you must customize

Both PowerShell wrappers contain placeholder sections where you should insert your actual CLI command.

### Codex wrapper

Edit `run_codex_task.ps1` and replace the placeholder with your actual Codex CLI command.

### Claude wrapper

Edit `run_claude_task.ps1` and replace the placeholder with your actual Claude Code CLI command.

## How the loop works

1. OpenClaw or you update `ops/agent_state/tasks.yaml`
2. `watch_tasks.py` finds a queued task owned by `codex` or `claude_code`
3. The watcher writes an in-progress packet under `ops/handoffs/in_progress/`
4. The watcher launches the correct wrapper script
5. The wrapper runs the coding tool and writes a summary
6. The watcher marks the task as `done` or `failed`
7. OpenClaw reads the summary and continues orchestration

## Important constraint

Do not let Claude Code and Codex work on the same active write branch at the same time.
