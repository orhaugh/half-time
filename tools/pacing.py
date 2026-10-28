"""Shared by the feeders: play a file of timed records at a multiple of real time.

The night is 180 minutes long; SPEED=30 plays it in six. Every record carries
an event time in epoch milliseconds, and each feeder sends it when the clock
reaches it, so the streams stay in step with each other and with the match.
"""
import json
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORLD = ROOT / "state" / "world"
RUN = ROOT / "state" / "run"


def manifest():
    return json.loads((WORLD / "manifest.json").read_text())


def records(name):
    with open(WORLD / name) as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


class Clock:
    """Maps event time onto wall time: first_ms plays at start_wall_s."""

    def __init__(self, start_wall_s, first_ms, speed):
        self.start, self.first, self.speed = start_wall_s, first_ms, speed

    def wait_until(self, ts_ms):
        due = self.start + (ts_ms - self.first) / (1000.0 * self.speed)
        delay = due - time.time()
        if delay > 0:
            time.sleep(delay)

    def minute(self, ts_ms, t0_ms):
        return (ts_ms - t0_ms) // 60_000


def progress(name, minute):
    """The match minute the feeder has reached, for run.sh."""
    RUN.mkdir(parents=True, exist_ok=True)
    tmp = RUN / f"{name}.progress.tmp"
    tmp.write_text(str(minute))
    tmp.replace(RUN / f"{name}.progress")
