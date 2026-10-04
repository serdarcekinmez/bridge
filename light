#!/usr/bin/env python3
"""Small, supervised Codex -> Claude CLI bridge for Car Sales V2 (Linux/Python 3.10).

Codex owns this file and the records under workflow/runs; Claude owns application
edits. Prompts/decisions are stdin text, never shell-interpolated. Typical usage:

  python3 -B workflow/claude_bridge.py init
  python3 -B workflow/claude_bridge.py status          # compact view
  python3 -B workflow/claude_bridge.py status --full   # whole state.json (large)
  python3 -B workflow/claude_bridge.py migrate --from-root OLD_ROOT --state-sha256 HASH
  python3 -B workflow/claude_bridge.py smoke --step 1  # authorized live test
  python3 -B workflow/claude_bridge.py smoke --step 2
  python3 -B workflow/claude_bridge.py start --task-id candidate-pool-review \
      --target src/phase3_firm_universe/candidate_pool.py --serdar-message continue
  python3 -B workflow/claude_bridge.py handover      # Codex-attested project status (JSON stdin)
  python3 -B workflow/claude_bridge.py start --task-id x --target src/a.py \
      --test-target tests/test_a.py --serdar-message continue   # test file optional
  python3 -B workflow/claude_bridge.py call NEXT
  python3 -B workflow/claude_bridge.py review revise --issue timing
  python3 -B workflow/claude_bridge.py call REVISE
  python3 -B workflow/claude_bridge.py review approve
  python3 -B workflow/claude_bridge.py call BUILD --supervised
  python3 -B workflow/claude_bridge.py call VALIDATE
  python3 -B workflow/claude_bridge.py review accept
  python3 -B workflow/claude_bridge.py report  # Turkish Markdown on stdin; STOP
  python3 -B workflow/claude_bridge.py close --commit SHA  # task finished outside bridge
  python3 -B workflow/claude_bridge.py watch   # Serdar: live view in a VS Code terminal

start, call, review, recover and report require nonempty stdin. Only Codex invokes
these control commands. --serdar-message records a real user message; it is an
operator attestation, not authentication of the speaker. Claude output is data,
never a control command or an approval. Every call is one turn, never a task loop.

OUTPUT SIZE: commands print a compact state view (stage, next_commands, active
task); never the whole state.json, which grows with history. Use status --full or
jq on workflow/runs/state.json only when an inspection really needs it.

PREVENTION: Claude gets no agent, notebook, or MCP tools. NEXT/REVISE are
read-only with no shell. BUILD/FIX expose Read/Glob/Grep/Edit/Write with exact
Edit(path) allow rules for the target and its optional approved test file.
BUILD/FIX/VALIDATE may run only "python3 -m pytest" / "python -m pytest" through
Bash allow rules; any other shell command is denied (dontAsk) and a denial makes
the call UNCERTAIN. Existing settings
and denies remain in force. Broad write allows/custom hooks in existing settings
could weaken per-target confinement: this is SUPERVISED CLI use, NOT an OS sandbox.
No claim of adversarial path confinement is made. Target symlinks/hard links and
permission-pattern metacharacters are rejected before dispatch.

DETECTION: full project file hashes (excluding protected host metadata and our
runtime records) are compared after each call. This detects persistent changes,
not transient writes, outside-root writes, or who made a concurrent change. No
automatic rollback. Scope violations and uncertain calls stop the workflow.

VALIDATION: Claude writes focused tests and runs them with pytest during
BUILD/FIX, and re-runs approved checks in VALIDATE, reporting concise actual
results. Codex reviews the test code and results; it does not routinely re-run
them. The bridge itself never runs tests or production pipelines. Accept requires
a successful VALIDATE plus Codex's actual-code inspection.

LIVE VIEW: Claude runs with stream-json output. Each event (text, tool call, short
tool result) is appended as one readable line to workflow/runs/live.log while the
call runs; "watch" follows it. The final machine-readable result is still parsed
from the result event; Codex receives only a compact summary, not the stream.

Recovery: an interrupted IN_FLIGHT/UNCERTAIN call is never retried automatically.
Inspect the saved call, process/session and actual files first, then use recover
with evidence on stdin. Recovery returns to inspection, not BUILD permission.
For an uncertain first call, attest whether its UUID exists with
recover --session-state established|absent; the bridge never guesses.

SESSIONS: schema 2 keeps a separate setup_session and a new UUID in every task.
NEXT/REVISE/BUILD/FIX/VALIDATE reuse that task's UUID; report archives it. A later
task gets a bounded English handover rather than old transcripts. Read-only
status can inspect old/moved state. Only explicit migrate upgrades/rebinds idle
state: it checks the inspected state hash, old root, last successful call and
source/data identity hashes, then saves a backup. Historical call records are
never rewritten. Relocation checks are evidence, not an OS security boundary.

Official references checked with local Claude 2.1.278:
https://code.claude.com/docs/en/headless
https://code.claude.com/docs/en/permissions
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "workflow" / "runs"
CLAUDE = (os.environ.get("CLAUDE_BIN") or shutil.which("claude")
          or "/home/serdar/.local/bin/claude")
WAITING = "WAITING_FOR_SERDAR"
SCHEMA_VERSION = 2
HANDOVER = {  # Default for a fresh init only; update live state with "handover".
    "project_status": "Phase 3 accepted (firm registry, point-in-time candidate pool). "
                      "Phase 4 not started until Serdar opens it.",
    "next_target": None,
    "corrections": [],
    "notes": "Directory historical validity remains an assumption; no administrative "
             "eligibility feed exists. Car Sales V1 and Auction Priority archives are separate.",
}
STAGE_TIMEOUT = {"NEXT": 300, "REVISE": 300, "BUILD": 900, "FIX": 900, "VALIDATE": 600}
PYTEST_RULES = ["Bash(python3 -m pytest:*)", "Bash(python -m pytest:*)"]
CACHE_DIRS = {"__pycache__", ".pytest_cache"}


NEXT_COMMANDS = {  # Hint for Codex so it never needs a full status dump to know what to do.
    WAITING: ["start (after Serdar's continue)", "handover"],
    "NEXT_READY": ["call NEXT"], "REVISE_READY": ["call REVISE"],
    "PROPOSAL_REVIEW": ["review revise --issue X", "review approve"],
    "BUILD_APPROVED": ["call BUILD --supervised"], "FIX_APPROVED": ["call FIX --supervised"],
    "CODEX_INSPECTION": ["call VALIDATE", "review fix --issue X", "review accept"],
    "LEARNING_REPORT_REQUIRED": ["report"], "UNRESOLVED_REVIEW": ["report"],
    "IN_FLIGHT": ["recover (after inspection)"], "UNCERTAIN": ["recover (after inspection)"],
}


def compact_state(state):
    """Small view of state for Codex's stdout. The full state stays in state.json;
    printing it after every command filled Codex's context (and usage limits)."""
    if state is None:
        return {"stage": "UNINITIALIZED"}
    task = state.get("task")
    view = {"stage": state.get("stage"), "next_commands": NEXT_COMMANDS.get(state.get("stage"), []),
            "history_count": len(state.get("history", [])), "last_call": state.get("last_call")}
    if task:
        approval = task.get("approval")
        view["task"] = {"task_id": task.get("task_id"), "target": task.get("target"),
                        "test_target": task.get("test_target"),
                        "approved_at": approval.get("at") if approval else None,
                        "validated": task.get("validated"), "issues": task.get("issues"),
                        "blockers": task.get("blockers", [])}
    if state.get("unresolved"):
        view["unresolved"] = state["unresolved"]
    return view


def new_session():
    return {"session_id": str(uuid.uuid4()), "session_established": False}


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(path):
    if not path.exists():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def inventory(root):
    """Content/mode/link inventory. Does not follow symlinks or prevent writes."""
    out = {}
    for base, dirs, files in os.walk(root, followlinks=False):
        rel = Path(base).relative_to(root)
        dirs[:] = sorted(d for d in dirs if not (
            (rel == Path(".") and d in {".git", ".agents", ".codex"})
            or (rel == Path("workflow") and d == "runs") or d in CACHE_DIRS))
        for name in files + [d for d in dirs if (Path(base) / d).is_symlink()]:
            p = Path(base) / name
            key = p.relative_to(root).as_posix()
            st = p.lstat()
            out[key] = {"mode": st.st_mode, "content": (
                "link:" + os.readlink(p) if p.is_symlink() else digest(p))}
    return out


def checked_target(root, name, *, recorded_test=False):
    p = Path(name)
    runtime_test = recorded_test and re.fullmatch(
        r"workflow/runs/[a-z0-9_-]+/test_[a-z0-9_]+\.py", name)
    if (p.is_absolute() or not p.parts or any(x in {".", ".."} for x in p.parts)
            or re.search(r"[\s*?\[\](){}!\\]", name)
            or (p.parts[0] not in {"src", "pipeline", "tests"} and not runtime_test)):
        raise ValueError("Target must be one literal relative application path under src/pipeline/tests")
    full = root / p
    if not full.resolve().is_relative_to(root.resolve()):
        raise ValueError("Target escapes project root")
    for item in [full, *full.parents]:
        if item == root:
            break
        if item.is_symlink():
            raise ValueError("Target and parents must not be symlinks")
    if full.exists() and (not full.is_file() or full.stat().st_nlink != 1):
        raise ValueError("Target must be an ordinary file without hard links")
    return full


def response_schema(task_id, stage, target):
    props = {k: {"type": "string", "enum": [v]} for k, v in
             {"task_id": task_id, "stage": stage, "target": target}.items()}
    props.update({k: {"type": "string"} for k in ["summary", "memory_token"]})
    props.update({k: {"type": "array", "items": {"type": "string"}}
                  for k in ["validation", "blockers"]})
    return {"type": "object", "properties": props, "required": list(props),
            "additionalProperties": False}


def parse_response(stdout, code, session_id, schema):
    if code != 0:
        raise ValueError(f"Claude exit code {code}; inspect call record")
    envelope = None
    try:
        envelope = json.loads(stdout)  # --output-format json (single envelope)
    except ValueError:
        for line in reversed(stdout.splitlines()):  # stream-json: final result event
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            if isinstance(ev, dict) and ev.get("type") == "result":
                envelope = ev
                break
    if not isinstance(envelope, dict) or envelope.get("session_id") != session_id:
        raise ValueError("Missing or mismatched Claude session_id")
    if envelope.get("is_error") is not False or envelope.get("subtype") != "success":
        raise ValueError("Claude did not return a successful result envelope")
    if envelope.get("permission_denials"):
        raise ValueError("Claude reported permission denials; supervision required")
    result = envelope.get("structured_output")
    if not isinstance(result, dict) or set(result) != set(schema["required"]):
        raise ValueError("Missing/malformed structured_output fields")
    for key, rule in schema["properties"].items():
        v = result[key]
        if rule["type"] == "string" and not isinstance(v, str):
            raise ValueError(f"{key} must be a string")
        if rule["type"] == "array" and (not isinstance(v, list) or
                                                not all(isinstance(x, str) for x in v)):
            raise ValueError(f"{key} must be a string array")
        if "enum" in rule and v not in rule["enum"]:
            raise ValueError(f"{key} does not match this invocation")
    return envelope


def short(text, limit):
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[:limit - 3] + "..."


class LiveLog:
    """Readable one-line-per-event view of a streaming call. Display only:
    never parsed by the bridge, never sent back to Codex or Claude."""

    def __init__(self, path, root):
        self.path, self.root, self.tools = path, str(root) + "/", {}

    def write(self, line):
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as f:
                f.write(datetime.now().strftime("%H:%M:%S") + "  " + line + "\n")
        except OSError:
            pass  # The live view must never break a call.

    def tool(self, name, inp):
        target = (inp.get("file_path") or inp.get("path") or inp.get("pattern")
                  or inp.get("command") or "")
        return f"-> {name} {short(str(target).replace(self.root, ''), 160)}".rstrip()

    def event(self, raw):
        try:
            ev = json.loads(raw)
        except ValueError:
            return self.write("   [non-JSON output line]") if raw.strip() else None
        kind = ev.get("type")
        content = (ev.get("message") or {}).get("content") or []
        if kind == "system" and ev.get("subtype") == "init":
            self.write(f"   session {str(ev.get('session_id'))[:8]} started, model {ev.get('model')}")
        elif kind == "assistant":
            for c in content if isinstance(content, list) else []:
                if c.get("type") == "text" and c.get("text", "").strip():
                    self.write("Claude: " + short(c["text"], 300))
                elif c.get("type") == "tool_use":
                    self.tools[c.get("id")] = c.get("name")
                    self.write(self.tool(c.get("name"), c.get("input") or {}))
        elif kind == "user":
            for c in content if isinstance(content, list) else []:
                if c.get("type") != "tool_result":
                    continue
                body = c.get("content")
                if isinstance(body, list):
                    body = " ".join(x.get("text", "") for x in body if isinstance(x, dict))
                name = self.tools.get(c.get("tool_use_id"))
                if c.get("is_error"):
                    self.write("   x " + short(body or "error", 300))
                elif name == "Bash":  # Test output: its tail holds the summary.
                    body = " ".join(str(body or "").split())
                    self.write("   ok ..." + body[-300:] if len(body) > 300 else "   ok " + body)
                elif name in {"Edit", "Write"}:
                    self.write("   ok file changed")
        elif kind == "result":
            self.write(f"   result: {ev.get('subtype')}, turns {ev.get('num_turns')}, "
                       f"{round((ev.get('duration_ms') or 0) / 1000)}s")


def run_process(argv, prompt, root, timeout, on_start, on_line=None):
    """One subprocess, no retry. Streams stdout lines to on_line while running.
    Kill its process group on timeout/interruption."""
    proc = subprocess.Popen(argv, cwd=root, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, start_new_session=True)
    out, err = [], []

    def pump_out():
        for line in proc.stdout:
            out.append(line)
            if on_line:
                try:
                    on_line(line)
                except Exception:
                    pass

    readers = [threading.Thread(target=pump_out, daemon=True),
               threading.Thread(target=lambda: err.append(proc.stderr.read()), daemon=True)]
    for t in readers:
        t.start()

    def collect():
        for t in readers:
            t.join(timeout=10)
        return "".join(out), "".join(err)

    try:
        on_start(proc.pid)
        try:
            proc.stdin.write(prompt)
            proc.stdin.close()
        except BrokenPipeError:
            pass
        proc.wait(timeout=timeout)
        return (*collect(), proc.returncode, False)
    except BaseException as exc:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()
        if isinstance(exc, subprocess.TimeoutExpired):
            return (*collect(), proc.returncode, True)
        raise


class Bridge:
    """Call only while holding the CLI lock; alternate roots/executables are for tests."""

    def __init__(self, root=ROOT, executable=CLAUDE, *, inspect_only=False):
        self.root = Path(root).resolve()
        self.runtime = self.root / "workflow" / "runs"
        self.path = self.runtime / "state.json"
        self.executable = executable
        self.state = json.loads(self.path.read_text()) if self.path.exists() else None
        if self.state is not None and not inspect_only:
            self.check_current()

    def check_current(self):
        if self.state is not None and (self.state.get("project_root") != str(self.root)
                                      or self.state.get("schema_version") != SCHEMA_VERSION):
            raise ValueError("State needs explicit migration; inspect status, then use migrate")

    def save(self):
        self.check_current()
        self.state["updated_at"] = now()
        atomic_json(self.path, self.state)

    def require(self, *stages):
        self.check_current()
        if not self.state or self.state["stage"] not in stages:
            raise ValueError(f"Requires {stages}; current stage: "
                             f"{self.state['stage'] if self.state else 'UNINITIALIZED'}")

    def init(self):
        if self.state is not None:
            raise ValueError("Already initialized; use status, never replace a recorded session")
        self.state = {"schema_version": SCHEMA_VERSION, "project_root": str(self.root),
                      "stage": WAITING, "setup_session": {**new_session(), "smoke_completed": 0},
                      "handover": json.loads(json.dumps(HANDOVER)), "task": None,
                      "history": [], "last_call": None}
        self.save()

    def migrate(self, from_root, state_sha256, notes):
        """Explicit idle-state migration, under the same CLI lock as dispatch."""
        if not self.state or self.state.get("schema_version") not in {1, SCHEMA_VERSION}:
            raise ValueError("Migration supports only known schema 1 or 2 state")
        if not notes.strip() or not Path(from_root).is_absolute():
            raise ValueError("Migration needs inspection notes and the absolute previous root")
        if (self.state.get("project_root") != from_root or
                digest(self.path) != state_sha256 or
                json.loads(self.path.read_text()) != self.state):
            raise ValueError("Previous root or inspected state hash does not match; no migration")
        if self.state.get("stage") != WAITING or self.state.get("task") is not None:
            raise ValueError("Migration requires WAITING_FOR_SERDAR with no active task/call")
        if self.state["schema_version"] == SCHEMA_VERSION and from_root == str(self.root):
            raise ValueError("State is already current; no migration needed")

        relative = Path(self.state.get("last_call") or "")
        if (relative.is_absolute() or ".." in relative.parts or
                relative.parent != Path("workflow/runs/calls")):
            raise ValueError("Migration needs a recorded local call as relocation evidence")
        evidence_path = self.root / relative
        if evidence_path.is_symlink() or not evidence_path.resolve().is_relative_to(
                (self.runtime / "calls").resolve()):
            raise ValueError("Migration evidence must remain inside local call records")
        evidence = json.loads(evidence_path.read_text())
        sessions = ([self.state["session_id"]] if self.state["schema_version"] == 1 else
                    [self.state["setup_session"]["session_id"]] +
                    [t["session_id"] for t in self.state["history"]])
        if (evidence.get("status") != "SUCCESS" or evidence.get("cwd") != from_root or
                evidence.get("session_id") not in sessions):
            raise ValueError("Last call is not successful evidence for the recorded root/session")
        resolved_calls = {r.get("call") for r in self.state.get("recoveries", [])}
        for p in (self.runtime / "calls").glob("*.json"):
            if (json.loads(p.read_text()).get("status") == "IN_FLIGHT" and
                    p.relative_to(self.root).as_posix() not in resolved_calls):
                raise ValueError("An IN_FLIGHT call record must be inspected/resolved before migration")

        # Docs/Git metadata and this workflow source may legitimately have changed.
        # Verify every application/data file captured by the completed call.
        recorded = evidence.get("inventory_after", {})
        anchors = {k: v for k, v in recorded.items()
                   if Path(k).parts and Path(k).parts[0] in {"src", "pipeline", "Datasets"}}
        if not any(k.startswith("src/") for k in anchors) or not any(
                k.startswith("pipeline/") for k in anchors):
            raise ValueError("Evidence lacks application source anchors")
        for name, expected in anchors.items():
            p = Path(name)
            if p.is_absolute() or ".." in p.parts or not isinstance(expected, dict):
                raise ValueError("Malformed identity evidence")
            full = self.root / p
            if (full.is_symlink() or not full.resolve().is_relative_to(self.root) or
                    not full.is_file() or digest(full) != expected.get("content")):
                raise ValueError(f"Relocation identity mismatch: {name}; no migration")

        migrated = json.loads(json.dumps(self.state))
        if migrated["schema_version"] == 1:
            shared_id = migrated["session_id"]
            session = {key: migrated.pop(key) for key in
                       ["session_id", "session_established", "smoke_completed"]}
            for key in ["smoke_token", "last_result"]:
                if key in migrated:
                    session[key] = migrated.pop(key)
            session.update(origin="legacy_shared_session", last_call=migrated["last_call"])
            migrated["setup_session"] = session
            for task in migrated["history"]:
                task.setdefault("session_id", shared_id)
                task.setdefault("session_policy", "legacy_shared")
        migration_id = str(uuid.uuid4())
        backup = self.runtime / "migrations" / migration_id / "state.before.json"
        migrated.setdefault("migrations", []).append({
            "id": migration_id, "actor": "Codex", "at": now(), "notes": notes,
            "from_schema": self.state["schema_version"], "to_schema": SCHEMA_VERSION,
            "from_root": from_root, "to_root": str(self.root), "state_sha256": state_sha256,
            "backup": backup.relative_to(self.root).as_posix(),
            "evidence": relative.as_posix(), "evidence_sha256": digest(evidence_path),
            "identity_files_checked": len(anchors)})
        migrated.update(schema_version=SCHEMA_VERSION, project_root=str(self.root), updated_at=now())
        if digest(self.path) != state_sha256:
            raise ValueError("State changed during inspection; no migration")
        backup.parent.mkdir(parents=True, exist_ok=False)
        with backup.open("xb") as f:
            f.write(self.path.read_bytes())
            f.flush()
            os.fsync(f.fileno())
        atomic_json(self.path, migrated)
        self.state = migrated

    def compact_handover(self):
        """Bounded context from operator-reviewed outcomes, never old transcripts."""
        latest = {}
        unresolved = []
        for task in self.state["history"]:
            if task.get("outcome") in {"accepted", "accepted_external"}:
                inspection = task.get("inspection", {})
                latest.pop(task["target"], None)
                latest[task["target"]] = {
                    "task_id": task["task_id"], "target": task["target"],
                    "sha256": inspection.get("target_sha256"),
                    "summary": inspection.get("notes", "")[:1200]}
            elif task.get("outcome") == "unresolved":
                unresolved.append({"task_id": task["task_id"], "target": task["target"],
                                   "issue": task.get("unresolved", {}).get("issue"),
                                   "notes": task.get("unresolved", {}).get("notes", "")[:1200]})
        original = self.state["handover"]
        target = original.get("next_target")
        pending = bool(target) and target not in latest
        return {"project_status": original["project_status"],
                "notes": original.get("notes", ""),
                "constraints": "One application file (plus its approved test file) per task. "
                "No production execution. Car Sales V1 and Auction Priority archives are "
                "separate. Codex approves; Claude never self-approves.",
                "pending_target": target if pending else None,
                "pending_corrections": original.get("corrections", []) if pending else [],
                "accepted_files": list(latest.values())[-5:], "unresolved_tasks": unresolved[-5:]}

    def set_handover(self, text):
        """Codex-attested project status for later tasks (e.g. work accepted outside
        the bridge, phase change). JSON on stdin: project_status (required), notes."""
        self.require(WAITING)
        data = json.loads(text)
        if not isinstance(data, dict) or not str(data.get("project_status", "")).strip():
            raise ValueError("handover needs JSON with a nonempty project_status")
        self.state.setdefault("handover_history", []).append(self.state["handover"])
        self.state["handover"] = {"project_status": str(data["project_status"])[:1500],
                                  "notes": str(data.get("notes", ""))[:1500],
                                  "next_target": None, "corrections": [],
                                  "actor": "Codex", "updated_at": now()}
        self.save()

    def close_external(self, commit, notes):
        """Archive an active task that Serdar/Codex finished outside the bridge.
        Requires an attested commit; never used for in-flight or uncertain calls."""
        self.require("PROPOSAL_REVIEW", "CODEX_INSPECTION", "BUILD_APPROVED", "FIX_APPROVED",
                     "REVISE_READY", "NEXT_READY", "LEARNING_REPORT_REQUIRED", "UNRESOLVED_REVIEW")
        if not re.fullmatch(r"[0-9a-f]{7,40}", commit):
            raise ValueError("--commit must be a hex Git commit id")
        task = self.state["task"]
        task["inspection"] = {"actor": "Codex", "notes": notes, "at": now(), "commit": commit,
                              "target_sha256": digest(self.root / task["target"])}
        task.update(outcome="accepted_external", completed_at=now())
        self.state["history"].append(task)
        self.state.update(task=None, stage=WAITING)
        self.save()

    def session(self, stage):
        session = self.state["setup_session"] if stage.startswith("SMOKE") else self.state["task"]
        if session is None:
            raise ValueError("No task session exists; only Serdar's continue may start one")
        return session

    def start(self, task_id, target, serdar_message, objective, test_target=None):
        self.require(WAITING)
        if serdar_message.strip().lower() not in {"continue", "devam et"}:
            raise ValueError("A new project task requires Serdar's subsequent continue")
        if self.state["setup_session"]["smoke_completed"] != 2:
            raise ValueError("Complete both communication checks first")
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,79}", task_id):
            raise ValueError("Invalid task_id")
        if any(t["task_id"] == task_id for t in self.state["history"]):
            raise ValueError("task_id already used")
        checked_target(self.root, target)
        if test_target is not None:
            if (Path(test_target).parts[:1] != ("tests",) and not re.fullmatch(
                    r"workflow/runs/[a-z0-9_-]+/test_[a-z0-9_]+\.py", test_target)) or test_target == target:
                raise ValueError("--test-target must be under tests/ or a recorded workflow/runs test")
            checked_target(self.root, test_target, recorded_test=True)
        self.state["task"] = {**new_session(), "session_policy": "per_task",
                              "task_id": task_id, "target": target, "test_target": test_target,
                              "objective": objective,
                              "handover": self.compact_handover(),
                              "serdar_continue": {"message": serdar_message, "recorded_at": now()},
                              "approval": None, "issues": {}, "validated": False}
        self.state.pop("unresolved", None)
        self.state["stage"] = "NEXT_READY"
        self.save()

    def argv(self, stage, target, schema, test_target=None):
        write = stage in {"BUILD", "FIX"}
        tests = stage in {"BUILD", "FIX", "VALIDATE"}
        tools = "" if stage.startswith("SMOKE") else "Read,Glob,Grep"
        if write:
            tools += ",Edit,Write"
        if tests:
            tools += ",Bash,TaskOutput"
        denied = "PowerShell,Agent,NotebookEdit,mcp__*" + ("" if tests else ",Bash")
        args = [self.executable, "--print", "--output-format", "stream-json", "--verbose",
                "--json-schema",
                json.dumps(schema), "--input-format", "text", "--tools", tools,
                "--permission-mode", "dontAsk", "--permission-prompts", "none",
                "--disable-slash-commands", "--no-chrome", "--strict-mcp-config",
                "--mcp-config", '{"mcpServers":{}}', "--disallowedTools", denied]
        allowed = []
        if write:
            allowed += [f"{tool}(/{self.root / t})" for t in (target, test_target) if t
                        for tool in ("Edit", "Write")]
        if tests:
            allowed += PYTEST_RULES + ["TaskOutput"]
        if allowed:
            args += ["--allowedTools", *allowed]
        session = self.session(stage)
        args += ["--resume" if session["session_established"] else "--session-id",
                 session["session_id"]]
        return args

    def call(self, stage, message, timeout=180, supervised=False):
        smoke = stage.startswith("SMOKE")
        allowed = {"SMOKE1": WAITING, "SMOKE2": WAITING, "NEXT": "NEXT_READY",
                   "REVISE": "REVISE_READY", "BUILD": "BUILD_APPROVED",
                   "FIX": "FIX_APPROVED", "VALIDATE": "CODEX_INSPECTION"}
        if stage not in allowed:
            raise ValueError("Unknown call stage")
        self.require(allowed[stage])
        if smoke and (self.state["task"] is not None or
                      self.state["setup_session"]["smoke_completed"] != int(stage[-1]) - 1):
            raise ValueError("Smoke steps must run once, in order, outside project tasks")
        task = self.state["task"] if not smoke else {"task_id": "bridge-setup", "target": ""}
        session = self.session(stage)
        write = stage in {"BUILD", "FIX"}
        if write:
            target = checked_target(self.root, task["target"])
            approval = task["approval"]
            if not supervised or not approval or approval["target"] != task["target"]:
                raise ValueError("Explicit Codex approval and --supervised are required")
            if approval["target_sha256"] != digest(target):
                raise ValueError("Target changed since approval; inspect it and reapprove")
            if task.get("test_target") and approval.get("test_sha256") != digest(
                    checked_target(self.root, task["test_target"], recorded_test=True)):
                raise ValueError("Test file changed since approval; inspect it and reapprove")
        if stage == "VALIDATE" and self.built_changed(task):
            raise ValueError("Target/test changed since BUILD/recovery inspection; inspect before validation")
        schema = response_schema(task["task_id"], stage, task["target"])
        rules = {
            "NEXT": "Read-only: inspect the actual code and propose purpose, exact file(s), "
                    "functions, inputs/outputs and focused acceptance checks. No edits, no shell.",
            "BUILD": "Edit only the approved target (and the approved test file, if any). "
                     "Run focused tests only with 'python3 -m pytest -q -p no:cacheprovider ...'. "
                     "Put concise ACTUAL test results in validation; never claim unrun tests. "
                     "In summary list inputs, functions (name: purpose), outputs and file path.",
            "VALIDATE": "No edits. Re-run only the approved pytest checks and report concise "
                        "actual results in validation; failures go in blockers.",
        }
        rules["REVISE"], rules["FIX"] = rules["NEXT"], rules["BUILD"]
        payload = {"instruction": "You are Claude, the application engineer. Codex is the reviewer. "
                   "Reply in English using the supplied JSON schema. No self-approval, no next task, "
                   "no production run or pipeline execution. Keep summary under 250 words. "
                   + rules.get(stage, "For smoke calls use no tools.") +
                   " Non-smoke calls use an empty memory_token.",
                   "project_root": str(self.root), "task_id": task["task_id"], "stage": stage,
                   "target": task["target"], "test_target": task.get("test_target"),
                   "task": {k: v for k, v in task.items() if k != "handover"},
                   "handover": self.compact_handover() if smoke else task["handover"],
                   "message": message}
        argv = self.argv(stage, task["target"], schema, task.get("test_target"))
        live = LiveLog(self.runtime / "live.log", self.root)
        live.write(f"===== {task['task_id']} | {stage} | {task['target']}"
                   + (f" + {task['test_target']}" if task.get("test_target") else "") + " =====")
        call_id = str(uuid.uuid4())
        path = self.runtime / "calls" / f"{call_id}.json"
        before = inventory(self.root)
        record = {"call_id": call_id, "started_at": now(), "stage": stage,
                  "session_id": session["session_id"], "cwd": str(self.root),
                  "argv": argv, "request": payload, "inventory_before": before,
                  "status": "IN_FLIGHT", "supervised": supervised}
        atomic_json(path, record)
        self.state.update(stage="IN_FLIGHT", last_call=str(path.relative_to(self.root)))
        self.save()  # Durable BEFORE dispatch: restart cannot accidentally resend BUILD.

        def started(pid):
            record["pid"] = pid
            atomic_json(path, record)

        begin = time.monotonic()
        error = None
        envelope = None
        try:
            stdout, stderr, code, timed_out = run_process(
                argv, json.dumps(payload, ensure_ascii=False), self.root, timeout, started,
                live.event)
            record.update(stdout=stdout, stderr=stderr, returncode=code, timed_out=timed_out)
            if timed_out:
                raise ValueError("Timed out; outcome uncertain, no automatic retry")
            envelope = parse_response(stdout, code, session["session_id"], schema)
            if smoke and envelope["structured_output"]["memory_token"] != session["smoke_token"]:
                raise ValueError("Session continuity token mismatch")
            if smoke and envelope["structured_output"]["blockers"]:
                raise ValueError("Claude reported a setup blocker")
        except (Exception, KeyboardInterrupt) as exc:
            error = f"{type(exc).__name__}: {exc}"
        finally:
            record["elapsed_seconds"] = round(time.monotonic() - begin, 3)
            record["finished_at"] = now()
            try:
                after = inventory(self.root)
                changed = sorted(k for k in before.keys() | after.keys() if before.get(k) != after.get(k))
                permitted = {task["target"], task.get("test_target")} if write else set()
                outside = [k for k in changed if k not in permitted]
                record.update(inventory_after=after, changed_paths=changed, outside_target=outside)
                if outside:
                    error = "Scope change detected (ownership unknown); inspect without rollback"
            except Exception as exc:
                error = f"Post-call inventory failed: {exc}"
        record.update(status="UNCERTAIN" if error else "SUCCESS", error=error, response=envelope)
        atomic_json(path, record)
        live.write(f"===== {record['status']} after {record['elapsed_seconds']}s"
                   + (f" | {short(error, 200)}" if error else "") + " =====")
        if error:
            self.state["stage"] = "UNCERTAIN"
        else:
            session["session_established"] = True
            blockers = envelope["structured_output"]["blockers"]
            if smoke:
                session["smoke_completed"] += 1
                session.update(last_call=self.state["last_call"], last_result=envelope)
                self.state["stage"] = WAITING
            else:
                task["last_result"] = envelope["structured_output"]
                task["result_call"] = self.state["last_call"]
                self.state["stage"] = ("PROPOSAL_REVIEW" if stage in {"NEXT", "REVISE"}
                                       else "CODEX_INSPECTION")
                if stage in {"BUILD", "FIX"}:
                    task["validated"] = False
                    self.mark_built(task)
                if stage == "VALIDATE":
                    task["validated"] = not blockers
                    task["validation_call"] = self.state["last_call"]
                if blockers:
                    task["blockers"] = blockers
                else:
                    task.pop("blockers", None)
        self.save()
        compact = None if envelope is None else {
            "structured_output": envelope.get("structured_output"),
            **{k: envelope.get(k) for k in ["num_turns", "duration_ms", "total_cost_usd"]}}
        return {"status": record["status"], "stage": self.state["stage"],
                "call_record": str(path), "response": compact, "error": error,
                "execution": {k: record.get(k) for k in
                              ["returncode", "timed_out", "elapsed_seconds", "changed_paths", "outside_target"]}}

    def mark_built(self, task):
        task["built_sha256"] = digest(self.root / task["target"])
        if task.get("test_target"):
            task["built_test_sha256"] = digest(self.root / task["test_target"])

    def built_changed(self, task):
        return (task.get("built_sha256") != digest(self.root / task["target"]) or
                (task.get("test_target") and
                 task.get("built_test_sha256") != digest(self.root / task["test_target"])))

    def smoke(self, step, timeout):
        self.require(WAITING)
        session = self.state["setup_session"]
        if step == 1 and session["smoke_completed"] == 0:
            session["smoke_token"] = uuid.uuid4().hex
            self.save()
            message = ("Communication setup only. Remember token " + session["smoke_token"] +
                       ". Return it as memory_token. Briefly acknowledge the handover, "
                       "including corrections 1-3. Do not propose NEXT or implement anything.")
        else:
            message = ("Communication continuity check only. Return the exact memory_token from "
                       "our preceding setup turn. It is deliberately not repeated here. "
                       "Briefly confirm candidate_pool.py is NOT accepted and await Codex.")
        return self.call(f"SMOKE{step}", message, timeout)

    def review(self, decision, notes, issue=None):
        self.require("PROPOSAL_REVIEW", "CODEX_INSPECTION", "BUILD_APPROVED", "FIX_APPROVED")
        task = self.state["task"]
        prior_stage = self.state["stage"]
        proposal = prior_stage in {"PROPOSAL_REVIEW", "BUILD_APPROVED"}
        if prior_stage in {"BUILD_APPROVED", "FIX_APPROVED"} and decision != "approve":
            raise ValueError("Only explicit reapproval is allowed from an approved stage")
        if decision in {"revise", "fix"}:
            if (decision == "revise") != proposal or not issue:
                raise ValueError("revise needs a proposal; fix needs inspection; both need --issue")
            rounds = task["issues"].get(issue, 0)
            if rounds >= 2:
                task["unresolved"] = {"issue": issue, "notes": notes}
                self.state.update(stage="UNRESOLVED_REVIEW", unresolved=task["unresolved"])
                self.save()
                return
            task["issues"][issue] = rounds + 1
            task["review_notes"] = notes
            self.state["stage"] = "REVISE_READY" if proposal else "FIX_APPROVED"
            if not proposal:
                task["approval"] = self.approval(notes)
        elif decision == "approve":
            if (not proposal and prior_stage != "FIX_APPROVED") or task.get("blockers"):
                raise ValueError("Approve requires a reviewed proposal without unresolved blockers")
            task["approval"] = self.approval(notes)
            self.state["stage"] = "FIX_APPROVED" if prior_stage == "FIX_APPROVED" else "BUILD_APPROVED"
        elif decision == "accept":
            if proposal or not task["validated"] or task.get("blockers"):
                raise ValueError("Accept requires inspected code and a successful VALIDATE exchange")
            if self.built_changed(task):
                raise ValueError("Target/test changed since BUILD; inspect and validate the actual version")
            task["inspection"] = {"actor": "Codex", "notes": notes, "at": now(),
                                  "target_sha256": digest(self.root / task["target"])}
            self.state["stage"] = "LEARNING_REPORT_REQUIRED"
        else:
            raise ValueError("Unknown review decision")
        self.save()

    def approval(self, notes):
        task = self.state["task"]
        target = checked_target(self.root, task["target"])
        test = task.get("test_target")
        return {"actor": "Codex", "target": task["target"], "at": now(), "contract": notes,
                "target_sha256": digest(target), "test_target": test,
                "test_sha256": digest(checked_target(self.root, test, recorded_test=True)) if test else None,
                "reviewed_call": task["result_call"]}

    def report(self, text):
        self.require("LEARNING_REPORT_REQUIRED", "UNRESOLVED_REVIEW")
        task = self.state["task"]
        task["outcome"] = "accepted" if self.state["stage"] == "LEARNING_REPORT_REQUIRED" else "unresolved"
        task["learning_report_tr"] = text
        task["completed_at"] = now()
        if task["outcome"] == "accepted" and task["target"] == self.state["handover"]["next_target"]:
            self.state["handover"]["project_status"] = (
                "Phase 3 in progress; candidate_pool.py accepted in task " + task["task_id"] +
                ". Corrections below are historical handover requirements, now reviewed. "
                "No automatic phase transition.")
        self.state["history"].append(task)
        self.state.update(task=None, stage=WAITING)
        self.save()

    def recover(self, notes, session_state=None):
        self.require("IN_FLIGHT", "UNCERTAIN")
        task = self.state["task"]
        if task is None:
            raise ValueError("Setup call uncertain: inspect logs/session before any further live call; no automatic reset")
        if session_state not in {None, "established", "absent"}:
            raise ValueError("Unknown session-state attestation")
        if not task["session_established"] and session_state is None:
            raise ValueError("First call uncertain: inspect its UUID and supply --session-state established|absent")
        if task["session_established"] and session_state == "absent":
            raise ValueError("Cannot silently discard an established task session")
        last = json.loads((self.root / self.state["last_call"]).read_text())
        if session_state is not None:
            task["session_established"] = session_state == "established"
        self.state["recovery"] = {"actor": "Codex", "notes": notes, "at": now(),
                                  "session_state": session_state, "call": self.state["last_call"]}
        self.state.setdefault("recoveries", []).append(dict(self.state["recovery"]))
        task["approval"] = None
        task["validated"] = False
        task["result_call"] = self.state["last_call"]
        # Never restore BUILD/FIX approval. Codex must inspect and issue a new decision.
        self.state["stage"] = ("PROPOSAL_REVIEW" if last["stage"] in {"NEXT", "REVISE"}
                               else "CODEX_INSPECTION")
        if self.state["stage"] == "CODEX_INSPECTION":
            checked_target(self.root, task["target"])
            self.mark_built(task)
        self.save()


def watch(path, backlog=15):
    """Follow the live log (like tail -f). Ctrl+C to stop; stopping never affects a call."""
    print(f"Watching {path} (Ctrl+C to stop)", flush=True)
    pos = 0
    if path.exists():
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        print("\n".join(lines[-backlog:]), flush=True)
        pos = path.stat().st_size
    try:
        while True:
            if path.exists():
                size = path.stat().st_size
                if size < pos:
                    pos = 0
                if size > pos:
                    with path.open("r", encoding="utf-8", errors="replace") as f:
                        f.seek(pos)
                        sys.stdout.write(f.read())
                        sys.stdout.flush()
                        pos = f.tell()
            time.sleep(0.5)
    except KeyboardInterrupt:
        return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ["init", "report", "handover", "watch"]:
        sub.add_parser(name)
    p = sub.add_parser("status")
    p.add_argument("--full", action="store_true",
                   help="print the whole state.json (large; for migration/recovery inspection only)")
    p = sub.add_parser("close")
    p.add_argument("--commit", required=True)
    p = sub.add_parser("migrate")
    p.add_argument("--from-root", required=True)
    p.add_argument("--state-sha256", required=True)
    p = sub.add_parser("recover")
    p.add_argument("--session-state", choices=["established", "absent"])
    p = sub.add_parser("start")
    p.add_argument("--task-id", required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--serdar-message", required=True)
    p.add_argument("--test-target")
    p = sub.add_parser("call")
    p.add_argument("stage", choices=["NEXT", "REVISE", "BUILD", "FIX", "VALIDATE"])
    p.add_argument("--supervised", action="store_true")
    p.add_argument("--timeout", type=float)  # default per stage: STAGE_TIMEOUT
    p = sub.add_parser("smoke")
    p.add_argument("--step", type=int, choices=[1, 2], required=True)
    p.add_argument("--timeout", type=float, default=90)
    p = sub.add_parser("review")
    p.add_argument("decision", choices=["revise", "approve", "fix", "accept"])
    p.add_argument("--issue")
    args = parser.parse_args()
    if args.command == "call" and args.timeout is None:
        args.timeout = STAGE_TIMEOUT[args.stage]
    if hasattr(args, "timeout") and args.timeout <= 0:
        parser.error("timeout must be positive")
    message = ""
    if args.command == "watch":
        return watch(RUNTIME / "live.log")  # Read-only; takes no lock.
    if args.command in {"start", "call", "review", "report", "recover", "migrate",
                        "handover", "close"}:
        message = sys.stdin.read().strip()
        if not message:
            parser.error("Nonempty operator instructions/evidence required on stdin")
    if args.command == "status":
        bridge = Bridge(inspect_only=True)
        print(json.dumps({"state": bridge.state if args.full else compact_state(bridge.state),
                          "observed_project_root": str(ROOT),
                          "state_sha256": digest(bridge.path),
                          "migration_required": bool(bridge.state and (
                              bridge.state.get("schema_version") != SCHEMA_VERSION or
                              bridge.state.get("project_root") != str(ROOT)))},
                         ensure_ascii=False, indent=2))
        return 0  # Read-only status creates no directories or lock file.
    RUNTIME.mkdir(parents=True, exist_ok=True)
    with (RUNTIME / "bridge.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.exit(2, "Bridge already in use; do not dispatch concurrently\n")
        bridge = Bridge(inspect_only=args.command == "migrate")
        output = None
        if args.command == "init":
            bridge.init()
        elif args.command == "migrate":
            bridge.migrate(args.from_root, args.state_sha256, message)
        elif args.command == "start":
            bridge.start(args.task_id, args.target, args.serdar_message, message, args.test_target)
        elif args.command == "call":
            output = bridge.call(args.stage, message, args.timeout, args.supervised)
        elif args.command == "smoke":
            output = bridge.smoke(args.step, args.timeout)
        elif args.command == "review":
            bridge.review(args.decision, message, args.issue)
        elif args.command == "report":
            bridge.report(message)
        elif args.command == "recover":
            bridge.recover(message, args.session_state)
        elif args.command == "handover":
            bridge.set_handover(message)
        elif args.command == "close":
            bridge.close_external(args.commit, message)
        print(json.dumps(output if output is not None else compact_state(bridge.state),
                         ensure_ascii=False, indent=2))
        return 1 if output and output["status"] != "SUCCESS" else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError) as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(2)
