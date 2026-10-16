#!/usr/bin/env python3
"""End-to-end checks: the real fluxingest binary, real UDP, real Parquet files.

Runs inside the Docker image (scripts/e2e.sh). Every scenario checks the
"no sample goes missing" rule:

    loadgen samples = samples the server parsed + samples in kernel-dropped packets
    samples offered = written + dropped_ring + dropped_batch + dropped_write_error
    rows in Parquet = written
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

BUILD = Path(sys.argv[1] if len(sys.argv) > 1 else "build/docker-release")
SERVER = BUILD / "fluxingest"
LOADGEN = BUILD / "flux_loadgen"
SMALL_FS = os.environ.get("FLUX_SMALL_DIR")

passed: list[str] = []
failed: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    (passed if cond else failed).append(name)
    mark = "ok  " if cond else "FAIL"
    print(f"    [{mark}] {name}" + (f"  ({detail})" if detail else ""), flush=True)


@dataclass
class Server:
    out: Path
    port: int
    args: list[str] = field(default_factory=list)
    proc: subprocess.Popen | None = None
    stats_path: Path = Path()
    log_path: Path = Path()

    def start(self) -> None:
        self.stats_path = self.out.parent / f"stats-{self.port}.json"
        self.log_path = self.out.parent / f"server-{self.port}.log"
        cmd = [str(SERVER), f"--port={self.port}", f"--out={self.out}", f"--stats-json={self.stats_path}",
               "--stats-interval=1", *self.args]
        self.proc = subprocess.Popen(cmd, stderr=open(self.log_path, "w"))
        time.sleep(0.5)  # allocate + bind
        assert self.proc.poll() is None, self.log_path.read_text()

    def rss_mib(self) -> float:
        assert self.proc is not None
        for line in Path(f"/proc/{self.proc.pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
        return 0.0

    def stop(self, sig: int = signal.SIGINT, timeout: float = 30) -> tuple[int, dict]:
        assert self.proc is not None
        self.proc.send_signal(sig)
        code = self.proc.wait(timeout=timeout)
        stats = json.loads(self.stats_path.read_text()) if self.stats_path.exists() else {}
        return code, stats


def loadgen(port: int, **kw: object) -> dict:
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        path = f.name
    args = [str(LOADGEN), f"--port={port}", f"--summary-json={path}"]
    args += [f"--{k.replace('_', '-')}={v}" for k, v in kw.items()]
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL)
    return json.loads(Path(path).read_text())


def parquet_rows(out: Path) -> int:
    files = list(out.glob("*.parquet"))
    if not files:
        return 0
    return duckdb.sql(f"SELECT count(*) FROM read_parquet('{out}/*.parquet')").fetchone()[0]


def tmp_files(out: Path) -> list[Path]:
    return list(out.glob("*.tmp"))


def check_conservation(s: dict, gen: dict, out: Path) -> None:
    spp = gen["samples_per_packet"]
    check("every sent packet was received or counted as a kernel drop",
          s["datagrams"] + s["kernel_drops"] == gen["packets"],
          f"received {s['datagrams']} + kernel drops {s['kernel_drops']} vs sent {gen['packets']}")
    check("every received sample was offered or rejected for cardinality",
          s["samples_offered"] + s["cardinality_rejected"] == s["datagrams"] * spp,
          f"{s['samples_offered']} + {s['cardinality_rejected']} vs {s['datagrams'] * spp}")
    accounted = s["written"] + s["dropped_ring"] + s["dropped_batch"] + s["dropped_write_error"]
    check("offered = written + every drop bucket", s["samples_offered"] == accounted,
          f"offered {s['samples_offered']} vs {accounted}")
    rows = parquet_rows(out)
    check("rows in Parquet = written", rows == s["written"], f"{rows} vs {s['written']}")
    check("no .tmp files left", not tmp_files(out))


def scenario_basic(root: Path) -> None:
    print("\n== basic: 2 shards, 2 s windows, 2M samples at 400k/s")
    out = root / "basic"
    srv = Server(out, 18089, ["--shards=2", "--window=2"])
    srv.start()
    gen = loadgen(18089, samples=2_000_000, rate=400_000, series=1000, sockets=8)
    time.sleep(1)
    code, s = srv.stop()
    check("exit code 0", code == 0, str(code))
    check("clean shutdown", s.get("clean_shutdown") is True)
    check_conservation(s, gen, out)
    check("no drops at this rate", s["dropped_ring"] + s["dropped_batch"] + s["dropped_write_error"] == 0)
    files = list(out.glob("*.parquet"))
    check("several files (one per shard per window)", len(files) >= 4, f"{len(files)} files")
    check("both shards got traffic",
          any(f.name.startswith("shard0") for f in files) and any(f.name.startswith("shard1") for f in files))
    distinct = duckdb.sql(f"SELECT count(DISTINCT tags) FROM read_parquet('{out}/*.parquet')").fetchone()[0]
    check("all 1000 series present", distinct == 1000, str(distinct))
    metrics = sorted(r[0] for r in duckdb.sql(
        f"SELECT DISTINCT metric FROM read_parquet('{out}/*.parquet')").fetchall())
    check("metric names are measurement.field", metrics == ["loadgen.f0", "loadgen.f1"], str(metrics))


def scenario_overload(root: Path) -> None:
    print("\n== overload: disk 200 ms per row group, sender at full speed for 6 s")
    out = root / "overload"
    srv = Server(out, 18090, ["--debug-write-delay-ms=200", "--batch-rows=8192", "--batches=3",
                              "--ring=65536", "--window=60"])
    srv.start()
    rss: list[float] = []
    summary = root / "overload-loadgen.json"
    lg = subprocess.Popen([str(LOADGEN), "--port=18090", "--samples=0", "--duration=6", "--sockets=4",
                           f"--summary-json={summary}"], stdout=subprocess.DEVNULL)
    while lg.poll() is None:
        rss.append(srv.rss_mib())
        time.sleep(0.2)
    gen = json.loads(summary.read_text())
    time.sleep(0.5)
    code, s = srv.stop()
    check("exit code 0", code == 0, str(code))
    check_conservation(s, gen, out)
    check("writer fell behind, batches were dropped", s["dropped_batch"] > 0, f"dropped_batch={s['dropped_batch']}")
    check("but data kept being written", s["written"] > 0, f"written={s['written']}")
    steady = rss[len(rss) // 4:]
    growth = max(steady) - min(steady) if steady else 0
    check("memory stays flat under overload (< 16 MiB growth)", growth < 16,
          f"RSS {min(rss):.0f}-{max(rss):.0f} MiB, growth after warm-up {growth:.1f} MiB")


def scenario_disk_full(root: Path) -> None:
    if not SMALL_FS:
        print("\n== disk full: SKIPPED (no FLUX_SMALL_DIR)")
        return
    print("\n== disk full: output on a 1 MiB tmpfs")
    out = Path(SMALL_FS) / "e2e"
    shutil.rmtree(out, ignore_errors=True)
    srv = Server(out, 18091, ["--window=1", "--batch-rows=65536", "--zstd=1"])
    # stats/log next to /small would eat the space; keep them in root.
    srv.stats_path = root / "diskfull-stats.json"
    srv.log_path = root / "diskfull.log"
    srv.args.append(f"--stats-json={srv.stats_path}")
    srv.proc = subprocess.Popen([str(SERVER), "--port=18091", f"--out={out}", "--stats-interval=1", *srv.args],
                                stderr=open(srv.log_path, "w"))
    time.sleep(0.5)
    gen = loadgen(18091, samples=3_000_000, rate=1_000_000, series=200_000, sockets=4)
    time.sleep(2)
    check("still running after the disk filled up", srv.proc.poll() is None)
    code, s = srv.stop()
    check("exit code 0", code == 0, str(code))
    check("write failures counted", s["files_failed"] > 0 and s["dropped_write_error"] > 0,
          f"files_failed={s['files_failed']} dropped_write_error={s['dropped_write_error']}")
    check_conservation(s, gen, out)
    shutil.rmtree(out, ignore_errors=True)


def scenario_crash(root: Path) -> None:
    print("\n== crash: kill -9 with a file still open, then restart")
    out = root / "crash"
    # Small batches so row groups reach the writer and a file is open when we
    # crash. (Samples still in an unsealed batch are lost on a crash; that's
    # the documented trade-off for a lossy pipeline.)
    srv = Server(out, 18092, ["--window=60", "--batch-rows=10000"])
    srv.start()
    loadgen(18092, samples=200_000, rate=200_000)
    time.sleep(0.5)
    assert srv.proc is not None
    srv.proc.kill()
    srv.proc.wait()
    tmps = tmp_files(out)
    check("open file was still a .tmp at the crash", len(tmps) >= 1, f"{len(tmps)} .tmp files")
    check("no half-written .parquet visible to readers", not list(out.glob("*.parquet")))
    srv2 = Server(out, 18093, ["--window=60"])
    srv2.start()
    log = srv2.log_path.read_text()
    code, _ = srv2.stop()
    check("restart removed the leftover .tmp", not tmp_files(out) and "removed" in log)
    check("restart exit code 0", code == 0, str(code))


def scenario_cardinality(root: Path) -> None:
    print("\n== cardinality: 1000 series sent, server allows 100 tag sets")
    out = root / "card"
    srv = Server(out, 18094, ["--max-tagsets=100", "--window=60"])
    srv.start()
    gen = loadgen(18094, samples=200_000, rate=200_000, series=1000)
    time.sleep(0.5)
    code, s = srv.stop()
    check("exit code 0", code == 0, str(code))
    check("exactly 100 tag sets interned", s["tagsets_interned"] == 100, str(s["tagsets_interned"]))
    check("samples of the other 900 series rejected", s["cardinality_rejected"] > 0,
          f"rejected={s['cardinality_rejected']}")
    check_conservation(s, gen, out)


def main() -> int:
    if not SERVER.exists():
        print(f"build first: {SERVER} not found")
        return 2
    root = Path(tempfile.mkdtemp(prefix="flux-e2e-"))
    try:
        scenario_basic(root)
        scenario_overload(root)
        scenario_disk_full(root)
        scenario_crash(root)
        scenario_cardinality(root)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    print(f"\n{len(passed)} checks passed, {len(failed)} failed")
    for f in failed:
        print(f"  FAILED: {f}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
