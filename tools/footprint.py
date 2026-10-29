#!/usr/bin/env python3
"""Sample what the run costs: clink's processes and every container.

    python3 tools/footprint.py --out state/run/footprint.jsonl        sample every 5 s until stopped
    python3 tools/footprint.py --summary state/run/footprint.jsonl    the figures

clink's figures come from its own /metrics (resident memory and CPU per
process); the containers' from docker stats. The summary gives each clink
process's peak resident memory and mean CPU, and the same for each
container, so the engine can be set beside the systems it feeds.
"""
import argparse
import json
import subprocess
import sys
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

CLINK = {"coordinator": 8081, "coordinator-standby": 8084, "worker-1": 18082, "worker-2": 18083}


def clink_sample(name, port):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as resp:
            text = resp.read().decode()
    except Exception:
        return None
    out = {"process": name}
    for line in text.splitlines():
        if line.startswith("clink_process_resident_memory_bytes "):
            out["rss_bytes"] = int(float(line.split()[-1]))
        elif line.startswith("clink_process_cpu_percent "):
            out["cpu_pct"] = float(line.split()[-1])
    return out if "rss_bytes" in out else None


PROJECT = Path(__file__).resolve().parent.parent.name  # the compose project: this directory's name


def docker_sample():
    """Every container of this compose project, and none of anything else on the machine."""
    try:
        out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"],
                             capture_output=True, text=True, timeout=20).stdout
    except Exception:
        return []
    rows = []
    for line in out.splitlines():
        if not line.strip():
            continue
        s = json.loads(line)
        if not s["Name"].startswith(PROJECT + "-"):
            continue
        rows.append({"container": s["Name"], "cpu_pct": float(s["CPUPerc"].rstrip("%") or 0),
                     "mem_bytes": parse_size(s["MemUsage"].split("/")[0])})
    return rows


def parse_size(text):
    text = text.strip()
    units = {"B": 1, "KiB": 1024, "MiB": 1024 ** 2, "GiB": 1024 ** 3, "kB": 1000, "MB": 1000 ** 2, "GB": 1000 ** 3}
    for unit, mult in sorted(units.items(), key=lambda kv: -len(kv[0])):
        if text.endswith(unit):
            return int(float(text[:-len(unit)]) * mult)
    return int(float(text))


def sample_loop(path, every):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        while True:
            at = int(time.time())
            for name, port in CLINK.items():
                s = clink_sample(name, port)
                if s:
                    s["at"] = at
                    f.write(json.dumps(s) + "\n")
            for s in docker_sample():
                s["at"] = at
                f.write(json.dumps(s) + "\n")
            f.flush()
            time.sleep(every)


def summary(path):
    clink, containers = defaultdict(list), defaultdict(list)
    for line in open(path):
        s = json.loads(line)
        if "process" in s:
            clink[s["process"]].append(s)
        else:
            containers[s["container"]].append(s)
    mib = 1024 ** 2
    print("clink, from its own /metrics (resident memory peak and mean, CPU mean of a core):")
    total_peak = 0
    for name, rows in clink.items():
        rss = [r["rss_bytes"] for r in rows]
        cpu = [r.get("cpu_pct", 0.0) for r in rows]
        total_peak += max(rss)
        print(f"    {name:<22} peak {max(rss) / mib:7.0f} MiB  mean {sum(rss) / len(rss) / mib:7.0f} MiB  "
              f"cpu {sum(cpu) / len(cpu):6.1f} %  ({len(rows)} samples)")
    print(f"    {'every clink process':<22} peak {total_peak / mib:7.0f} MiB (sum of peaks)")
    print("the systems it feeds, from docker stats (memory peak, CPU mean):")
    ctotal = 0
    for name, rows in sorted(containers.items(), key=lambda kv: -max(r["mem_bytes"] for r in kv[1])):
        mem = [r["mem_bytes"] for r in rows]
        cpu = [r["cpu_pct"] for r in rows]
        ctotal += max(mem)
        print(f"    {name:<22} peak {max(mem) / mib:7.0f} MiB  cpu {sum(cpu) / len(cpu):6.1f} %")
    print(f"    {'every container':<22} peak {ctotal / mib:7.0f} MiB (sum of peaks)")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", help="append samples to this file until stopped")
    ap.add_argument("--every", type=float, default=5.0)
    ap.add_argument("--summary", help="print the figures of this sample file")
    args = ap.parse_args()
    if args.summary:
        summary(Path(args.summary))
    elif args.out:
        try:
            sample_loop(Path(args.out), args.every)
        except KeyboardInterrupt:
            pass
    else:
        sys.exit("footprint: give --out or --summary")


if __name__ == "__main__":
    main()
