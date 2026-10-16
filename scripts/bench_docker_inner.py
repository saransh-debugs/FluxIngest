#!/usr/bin/env python3
"""Runs inside the Docker image. Two measurements:

1. bench_pipeline with the real Parquet sink vs the null sink (no network).
2. UDP: loadgen at full speed into 1 and 2 shards; how much arrives,
   how much the kernel drops.
"""
from __future__ import annotations

import json
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

B = Path(sys.argv[1])
which = sys.argv[2]

if which == "pipeline":
    for sink in ("null", "parquet"):
        out = subprocess.run([str(B / "bench_pipeline"), "--seconds=4", "--series=10000", f"--sink={sink}",
                              "--out=/tmp/flux-bench"], capture_output=True, text=True, check=True).stdout
        print(out, end="", flush=True)
elif which.startswith("udp"):
    shards = int(which[3:])
    root = Path(tempfile.mkdtemp())
    stats = root / "stats.json"
    srv = subprocess.Popen([str(B / "fluxingest"), "--port=19000", f"--out={root}/data", f"--shards={shards}",
                            "--window=60", "--stats-interval=0", f"--stats-json={stats}", "--no-fsync"],
                           stderr=subprocess.DEVNULL)
    time.sleep(0.5)
    summary = root / "lg.json"
    subprocess.run([str(B / "flux_loadgen"), "--port=19000", "--samples=0", "--duration=5", "--sockets=8",
                    "--series=10000", f"--summary-json={summary}"], check=True, stdout=subprocess.DEVNULL)
    time.sleep(0.5)
    srv.send_signal(signal.SIGINT)
    srv.wait(timeout=60)
    s = json.loads(stats.read_text())
    g = json.loads(summary.read_text())
    sent_rate = g["samples"] / g["elapsed_s"]
    got = s["samples_offered"]
    print(f"udp shards={shards}: loadgen sent {sent_rate/1e6:.2f}M samples/s; "
          f"server took {got / g['elapsed_s'] / 1e6:.2f}M samples/s "
          f"({100 * got / g['samples']:.1f}%), kernel dropped {s['kernel_drops']} of {g['packets']} packets, "
          f"ring dropped {s['dropped_ring']}, written {s['written']}")
