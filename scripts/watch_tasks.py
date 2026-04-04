import argparse
import copy
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml  # type: ignore
except Exception:
    print("Missing dependency: pyyaml. Install with: pip install pyyaml", file=sys.stderr)
    raise


VALID_STATUSES = {"queued", "in_progress", "blocked", "done", "failed"}
DEFAULT_POLL_SECONDS = 15


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"tasks": []}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML structure in {path}")
    data.setdefault("tasks", [])
    if not isinstance(data["tasks"], list):
        raise ValueError("tasks must be a list")
    return data


def dump_yaml(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)
    temp.replace(path)


def append_log(log_path: Path, message: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"[{utc_now()}] {message}\n")


def sanitize_name(value: str) -> str:
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in value)
    return safe[:80] or "task"


def ensure_task_shape(task: Dict[str, Any]) -> None:
    if task.get("status") not in VALID_STATUSES:
        task["status"] = "queued"
    task.setdefault("owner", "")
    task.setdefault("task_id", "")
    task.setdefault("branch", "")
    task.setdefault("repo_path", "")
    task.setdefault("business_goal", "")
    task.setdefault("scope", "")
    task.setdefault("inputs", [])
    task.setdefault("deliverables", [])
    task.setdefault("assumptions", [])
    task.setdefault("validation_required", [])
    task.setdefault("evidence_level", "")
    task.setdefault("next_receiver", "openclaw")
    task.setdefault("artifacts", {})


class TaskRunner:
    def __init__(self, root: Path, dry_run: bool = False):
        self.root = root
        self.ops = root / "ops"
        self.handoffs = self.ops / "handoffs"
        self.incoming = self.handoffs / "incoming"
        self.in_progress = self.handoffs / "in_progress"
        self.blocked = self.handoffs / "blocked"
        self.done = self.handoffs / "done"
        self.logs = self.ops / "logs"
        self.tasks_yaml = self.ops / "agent_state" / "tasks.yaml"
        self.scripts = root / "scripts"
        self.dry_run = dry_run

    def load_tasks(self) -> Dict[str, Any]:
        data = load_yaml(self.tasks_yaml)
        for task in data["tasks"]:
            ensure_task_shape(task)
        return data

    def save_tasks(self, data: Dict[str, Any]) -> None:
        dump_yaml(self.tasks_yaml, data)

    def pick_next_task_index(self, tasks: List[Dict[str, Any]]) -> Optional[int]:
        for idx, task in enumerate(tasks):
            if task.get("status") == "queued" and task.get("owner") in {"claude_code", "codex"}:
                return idx
        return None

    def wrapper_for_owner(self, owner: str) -> Path:
        if owner == "claude_code":
            return self.scripts / "run_claude_task.ps1"
        if owner == "codex":
            return self.scripts / "run_codex_task.ps1"
        raise ValueError(f"Unsupported owner: {owner}")

    def task_packet_path(self, task: Dict[str, Any], bucket: Path) -> Path:
        name = f"{sanitize_name(task['task_id'])}_{sanitize_name(task.get('owner', ''))}.json"
        return bucket / name

    def write_packet(self, task: Dict[str, Any], bucket: Path) -> Path:
        packet_path = self.task_packet_path(task, bucket)
        packet_path.parent.mkdir(parents=True, exist_ok=True)
        with packet_path.open("w", encoding="utf-8") as f:
            json.dump(task, f, ensure_ascii=False, indent=2)
        return packet_path

    def run_once(self) -> bool:
        data = self.load_tasks()
        idx = self.pick_next_task_index(data["tasks"])
        if idx is None:
            return False

        task = copy.deepcopy(data["tasks"][idx])
        task_id = task["task_id"] or f"task_{idx}"
        log_path = self.logs / f"{sanitize_name(task_id)}.log"

        task["status"] = "in_progress"
        task["started_at"] = utc_now()
        data["tasks"][idx] = task
        self.save_tasks(data)

        in_progress_packet = self.write_packet(task, self.in_progress)
        append_log(log_path, f"Picked task {task_id} for owner={task['owner']}")

        wrapper = self.wrapper_for_owner(task["owner"])
        repo_path = str(task.get("repo_path") or self.root)
        summary_path = str(self.done / f"{sanitize_name(task_id)}_summary.md")

        cmd = [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(wrapper),
            "-TaskJson",
            str(in_progress_packet),
            "-RepoPath",
            repo_path,
            "-OutputSummary",
            summary_path,
        ]

        append_log(log_path, f"Running wrapper: {' '.join(cmd)}")

        if self.dry_run:
            result = subprocess.CompletedProcess(cmd, 0, stdout="DRY RUN\n", stderr="")
        else:
            result = subprocess.run(cmd, capture_output=True, text=True)

        append_log(log_path, f"Exit code: {result.returncode}")
        if result.stdout:
            append_log(log_path, f"STDOUT:\n{result.stdout.strip()}")
        if result.stderr:
            append_log(log_path, f"STDERR:\n{result.stderr.strip()}")

        data = self.load_tasks()
        task = data["tasks"][idx]
        task.setdefault("artifacts", {})
        task["artifacts"]["summary"] = summary_path
        task["finished_at"] = utc_now()

        if result.returncode == 0:
            task["status"] = "done"
            self.write_packet(task, self.done)
            append_log(log_path, f"Task {task_id} completed")
        else:
            task["status"] = "failed"
            task["artifacts"]["stderr_log"] = str(log_path)
            self.write_packet(task, self.blocked)
            append_log(log_path, f"Task {task_id} failed")

        data["tasks"][idx] = task
        self.save_tasks(data)
        return True



def main() -> int:
    parser = argparse.ArgumentParser(description="Watch tasks.yaml and dispatch queued tasks to Codex or Claude Code wrappers.")
    parser.add_argument("--root", default=".", help="Repo root containing ops/ and scripts/")
    parser.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS)
    parser.add_argument("--once", action="store_true", help="Run at most one task and exit")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    runner = TaskRunner(Path(args.root).resolve(), dry_run=args.dry_run)

    if args.once:
        ran = runner.run_once()
        return 0 if ran else 1

    while True:
        try:
            runner.run_once()
        except Exception as exc:
            log_path = runner.logs / "watcher_errors.log"
            append_log(log_path, f"Unhandled watcher error: {exc}")
        time.sleep(max(args.poll_seconds, 3))


if __name__ == "__main__":
    raise SystemExit(main())
