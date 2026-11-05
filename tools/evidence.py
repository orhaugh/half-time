#!/usr/bin/env python3
"""The release evidence of a half-time run: what ran, and what it showed.

    python3 tools/evidence.py preflight --candidate SHA --clink-src DIR --bin-dir DIR --out DIR
    python3 tools/evidence.py collect --out DIR --run-exit N --started UTC

evidence.sh calls both. preflight refuses a run whose clink is not exactly the
candidate: the checkout must be at the candidate commit with nothing changed,
and clink and clink_node must each report that commit, built clean. It
records that, the half-time commit and any local changes, and the host, in
provenance.json. collect copies what the run left under state/run into the
bundle: the scenes, every checker's output, exit code and verdicts, the
Coordinators' and Workers' logs, the submitted SQL and the protocol trace,
with every credential removed, then writes evidence.json and MANIFEST.sha256.
clink's scripts/check-half-time-evidence.py is what judges a bundle.
"""
import argparse
import datetime
import gzip
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "state" / "run"
FORMAT = "half-time-evidence/1"
CHECKERS = ("check_run", "check_round2", "check_round3", "check_riders")
DEFAULTS = {"SCENES": "1", "SPEED": "30", "DISPATCH_PARALLELISM": "1", "TRACE_CHECK": "0", "CAPTURE_JOBS": ""}

# The run's systems take demo passwords (docker-compose.yml) that run.sh prints
# and the SQL carries. None of them belongs in a bundle that may be shared.
REDACTIONS = [
    (re.compile(r"(?i)((?:password|passwd|pwd|secret|token)['\"]?)(\s*[=:]\s*)('[^']*'|\"[^\"]*\"|[^\s,;)]+)"),
     r"\1\2<redacted>"),
    (re.compile(r"(?i)(\b[a-z][a-z0-9+.-]*://[^:/\s@]+:)[^@\s/]+@"), r"\1<redacted>@"),
    (re.compile(r"\((\w[\w.-]*) / [^)]+\)"), r"(\1 / <redacted>)"),
]
# What must not survive the redaction: the one demo password that is not also
# a user or database name, and any private key.
FORBIDDEN = ("half-time-pw", "PRIVATE KEY")


def git(cwd, *args):
    return subprocess.run(["git", "-C", str(cwd), *args], capture_output=True, text=True,
                          check=True).stdout.strip()


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def output(argv):
    try:
        return subprocess.run(argv, capture_output=True, text=True, timeout=60).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def preflight(args):
    out = Path(args.out)
    src = Path(args.clink_src).resolve()
    bins = Path(args.bin_dir).resolve()
    problems = []
    try:
        candidate = git(src, "rev-parse", "--verify", f"{args.candidate}^{{commit}}")
    except subprocess.CalledProcessError:
        sys.exit(f"evidence: {args.candidate} is not a commit in {src}; refused, nothing was run")
    head = git(src, "rev-parse", "HEAD")
    changed = git(src, "status", "--porcelain")
    if head != candidate:
        problems.append(f"the clink checkout {src} is at {head}, not the candidate {candidate}")
    if changed:
        problems.append(f"the clink checkout {src} has local changes")

    binaries = {}
    for name in ("clink", "clink_node", "clink_submit_sql"):
        path = bins / name
        if not path.is_file():
            problems.append(f"{path} does not exist")
            continue
        binaries[name] = {"path": str(path), "sha256": sha256(path), "bytes": path.stat().st_size}
    if "clink" in binaries:
        try:
            build = json.loads(output([str(bins / "clink"), "--capabilities-json"]))["build"]
            binaries["clink"].update(commit=build["git_sha"], clean=bool(build["git_clean"]),
                                     version=json.loads(output([str(bins / "clink"), "--capabilities-json"]))
                                     .get("clink_version", ""))
        except (ValueError, KeyError, TypeError):
            problems.append("clink --capabilities-json did not name the commit it was built from")
    if "clink_node" in binaries:
        version = output([str(bins / "clink_node"), "--version"])
        m = re.search(r"commit ([0-9a-f]{40})(-dirty)?", version)
        if m:
            binaries["clink_node"].update(commit=m.group(1), clean=not m.group(2),
                                          version=version.splitlines()[0])
        else:
            problems.append("clink_node --version did not name the commit it was built from")
    for name in ("clink", "clink_node"):
        b = binaries.get(name, {})
        if "commit" in b and (b["commit"] != candidate or not b["clean"]):
            problems.append(f"{name} was built from {b['commit']}{'' if b['clean'] else ' with local changes'}, "
                            f"not the candidate {candidate}: build the candidate's checkout from a fresh "
                            f"configure, which is when clink records its commit")

    status = git(ROOT, "status", "--porcelain")
    diff = git(ROOT, "diff", "HEAD")
    half_time = {"commit": git(ROOT, "rev-parse", "HEAD"), "clean": not status,
                 "changes": status.splitlines()}
    if diff:
        (out / "half-time.diff").write_text(diff + "\n")
        half_time["diff"] = "half-time.diff"

    host = {
        "platform": platform.platform(), "machine": platform.machine(), "cpus": os.cpu_count(),
        "python": platform.python_version(),
        "docker": output(["docker", "version", "--format", "{{.Server.Version}}"]),
        "docker_memory_bytes": output(["docker", "info", "--format", "{{.MemTotal}}"]),
    }
    provenance = {"candidate": candidate,
                  "clink": {"source": {"path": str(src), "head": head, "clean": not changed},
                            "binaries": binaries},
                  "half_time": half_time, "host": host, "refused": problems}
    (out / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    if problems:
        for p in problems:
            print(f"evidence: {p}", file=sys.stderr)
        print("evidence: refused; nothing was run", file=sys.stderr)
        sys.exit(2)
    print(f"evidence: clink {candidate[:12]}, clink and clink_node built from it, clean; "
          f"half-time {half_time['commit'][:12]}{'' if half_time['clean'] else ' with local changes'}")


def redact(text):
    for pattern, replacement in REDACTIONS:
        text = pattern.sub(replacement, text)
    return text


def copy_text(src, dest):
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(redact(src.read_text(errors="replace")))


def read_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()] if path.exists() else []


def collect(args):
    out = Path(args.out)
    provenance = json.loads((out / "provenance.json").read_text())

    # run.log was written by tee as the run went; it is redacted in place.
    if (out / "run.log").exists():
        (out / "run.log").write_text(redact((out / "run.log").read_text(errors="replace")))
    for name in ("scenes.jsonl", "checks.tsv", "verdicts.jsonl", "jobs.tsv", "footprint.jsonl",
                 "trace-check.log", "riders.submit.out", "kitchen-operators-before-failover.json",
                 "kitchen-operators-at-the-end.json"):
        if (STATE / name).exists():
            copy_text(STATE / name, out / name)
    for path in sorted(STATE.glob("check_*.out")):
        copy_text(path, out / "checks" / path.name)
    for path in sorted(STATE.glob("*.log")):
        if path.name != "trace-check.log":
            copy_text(path, out / "logs" / path.name)
    for path in sorted(STATE.glob("*.submit.err")):
        copy_text(path, out / "logs" / path.name)
    for path in sorted(STATE.glob("*.sql")):
        copy_text(path, out / "jobs" / path.name)
    traces = sorted((STATE / "traces").glob("*.ndjson")) if (STATE / "traces").is_dir() else []
    for path in traces:
        (out / "traces").mkdir(exist_ok=True)
        with open(path, "rb") as src, gzip.open(out / "traces" / (path.name + ".gz"), "wb") as dest:
            shutil.copyfileobj(src, dest)
    world = ROOT / "state" / "world" / "manifest.json"
    if world.exists():
        shutil.copy(world, out / "world-manifest.json")
    if (STATE / "footprint.jsonl").exists():
        (out / "footprint.txt").write_text(redact(output(
            [sys.executable, "-I", str(ROOT / "tools" / "footprint.py"), "--summary",
             str(STATE / "footprint.jsonl")])) + "\n")

    checks = []
    trace_check = {"ran": False}
    for line in (out / "checks.tsv").read_text().splitlines() if (out / "checks.tsv").exists() else []:
        name, _, code = line.partition("\t")
        if name == "trace":
            trace_check = {"ran": True, "exit_code": int(code), "log": "trace-check.log"}
            continue
        checks.append({"checker": name, "exit_code": int(code), "output": f"checks/{name}.out"})
    verdicts = read_lines(out / "verdicts.jsonl")
    counts = {}
    for v in verdicts:
        c = counts.setdefault(v["guarantee"], {"held": 0, "failed": 0})
        c["held" if v["ok"] else "failed"] += 1

    env = {k: os.environ.get(k, d) for k, d in DEFAULTS.items()}
    evidence = {
        "format": FORMAT,
        "created_utc": now(),
        "candidate": provenance["candidate"],
        "clink": provenance["clink"],
        "half_time": provenance["half_time"],
        "host": provenance["host"],
        "invocation": {"command": "./evidence.sh, which runs ./run.sh", "env": env,
                       "world": json.loads(world.read_text()) if world.exists() else None},
        "run": {"exit_code": args.run_exit, "started_utc": args.started, "finished_utc": now(),
                "log": "run.log"},
        "checks": checks,
        "verdicts": {"file": "verdicts.jsonl", "counts": counts},
        "scenes": read_lines(out / "scenes.jsonl"),
        "trace_check": trace_check,
        "logs": {"coordinator": "logs/coordinator.log", "standby": "logs/coordinator-standby.log",
                 "workers": sorted(f"logs/{p.name}" for p in STATE.glob("worker-*.log"))},
        "left_out": ["state/run/checkpoints", "state/run/ha", "state/run/capture",
                     "credentials, replaced by <redacted>"],
    }
    (out / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")

    leaks = []
    for path in sorted(p for p in out.rglob("*") if p.is_file() and p.suffix != ".gz"):
        text = path.read_text(errors="replace")
        leaks += [f"{path.relative_to(out)}: {f}" for f in FORBIDDEN if f in text]
    if leaks:
        print("evidence: a credential survived the redaction, so no manifest was written:", *leaks,
              sep="\n  ", file=sys.stderr)
        sys.exit(1)
    files = sorted(p for p in out.rglob("*") if p.is_file() and p.name != "MANIFEST.sha256")
    (out / "MANIFEST.sha256").write_text(
        "".join(f"{sha256(p)}  {p.relative_to(out).as_posix()}\n" for p in files))
    held = sum(c["held"] for c in counts.values())
    failed = sum(c["failed"] for c in counts.values())
    print(f"evidence: {out} (run exit {args.run_exit}; {held} verdicts held, {failed} failed; "
          f"scenes {', '.join(s['scene'] + ' ' + s['status'] for s in evidence['scenes']) or 'none recorded'})")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="step", required=True)
    p = sub.add_parser("preflight")
    p.add_argument("--candidate", required=True)
    p.add_argument("--clink-src", required=True)
    p.add_argument("--bin-dir", required=True)
    p.add_argument("--out", required=True)
    c = sub.add_parser("collect")
    c.add_argument("--out", required=True)
    c.add_argument("--run-exit", type=int, required=True)
    c.add_argument("--started", required=True)
    args = ap.parse_args()
    (preflight if args.step == "preflight" else collect)(args)


if __name__ == "__main__":
    main()
