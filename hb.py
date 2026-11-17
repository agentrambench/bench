"""harness-bench runner. Scenarios: task (fixed code task, one-shot), crash (kill -9 / SIGHUP during a tool call, then resume),
idle (N interactive agents after one turn, held H seconds, then re-prompted). Every harness runs in its own systemd user scope under
batch.slice; counters from the cgroup (memory.peak, cpu.stat) and per-process smaps_rollup. LLM = local deterministic mock (mock.mjs)."""
import argparse
import fcntl
import json
import os
import re
import selectors
import shutil
import signal
import struct
import subprocess
import sys
import termios
import threading
import time
from pathlib import Path

from harnesses import CLI, FLAGS

ROOT = Path(__file__).parent
sys.modules.setdefault("hb", sys.modules[__name__])
# out-of-tree harness adapters: plugin_<name>.py next to hb.py, module exposing NAME, meta(), add_args(p, cmd), task/crash/fault/herd(...) and IdleDriver
PLUGINS = {}


def load_plugins():
    import importlib
    for f in sorted(ROOT.glob("plugin_*.py")):
        m = importlib.import_module(f.stem)
        PLUGINS[m.NAME] = m
WORK = Path.home() / ".cache/harness-bench/runs"
RESULTS = ROOT / "results/raw"
CG_ROOT = Path("/sys/fs/cgroup")
FIX = ROOT / "fixtures/calc"
PROMPTS = {"HBTASK": "HBTASK: test_calc.py fails. Fix calc.py so the tests pass.",
           "HBSLOW": "HBSLOW: run the three benchmark steps.",
           "HBIDLE": "HBIDLE: say ready.",
           "HBLONG": "HBLONG: read the twenty big files.",
           "HBMARATHON": "HBMARATHON: run the file-reading steps one by one until done.",
           "HBSTREAM": "HBSTREAM: run the step, then write the long answer.",
           "HBHANG": "HBHANG: run the two benchmark commands.",
           "HBHUGE": "HBHUGE: run the three output commands.",
           "HBLOOP": "HBLOOP: keep checking until done.",
           "HBFX": "HBFX: run the four benchmark steps in order.",
           "HBFLOOD": "HBFLOOD: run the output command."}


# ---------- measurement ----------

def smaps(pid):
    try:
        out = {}
        for line in open(f"/proc/{pid}/smaps_rollup"):
            k, _, v = line.partition(":")
            if k in ("Rss", "Pss", "SwapPss", "Pss_Anon"):
                out[k] = int(v.split()[0])
        return out
    except OSError:
        return None


def cg_pids(cg):
    pids = []
    for d, _, files in os.walk(cg):
        try:
            pids += [int(x) for x in open(f"{d}/cgroup.procs").read().split()]
        except OSError:
            pass
    return pids


def cg_read(cg, name):
    try:
        return open(cg / name).read()
    except OSError:
        return None


def cg_stat(cg):
    s = {}
    for f in ("memory.stat", "cpu.stat"):
        for line in (cg_read(cg, f) or "").splitlines():
            k, _, v = line.partition(" ")
            s[k] = int(v)
    for f in ("memory.current", "memory.swap.current", "memory.peak"):
        v = cg_read(cg, f)
        if v:
            s[f] = int(v)
    return s


def proc_cg(pid):
    try:
        return CG_ROOT / open(f"/proc/{pid}/cgroup").read().split("::", 1)[1].strip().lstrip("/")
    except (OSError, IndexError):
        return None


ROLES = ("harness", "terminal", "tool")


def role(pid):
    """harness = harness binaries (node, native CLIs, plugin servers); terminal = tmux; tool = everything the agent ran (shells, python, cat...)."""
    e = exe(pid)
    if e.endswith("/tmux") or cmdline(pid) == "sleep infinity":
        return "terminal"
    if e.startswith(HARNESS_EXE) or "/node_modules/" in e:
        return "harness"
    if e.startswith(str(WORK)) and "/ws" not in e:  # binaries a harness unpacked into its run HOME (codex helper)
        return "harness"
    return "tool"


class Sampler(threading.Thread):
    """Polls a cgroup (recursive): sum of Pss/Rss/SwapPss over its processes + memory.current/anon. Keeps maxima and a sparse series."""

    def __init__(self, cg, period=0.25, keep_every=1.0, exclude=(), cg_only=False):
        super().__init__(daemon=True)
        self.cg, self.period, self.keep_every, self.exclude, self.cg_only = Path(cg), period, keep_every, set(exclude), cg_only
        self.max = {}
        self.series = []
        self.last = {}
        self.stop_ev = threading.Event()

    def sample(self):
        tot = {"Pss": 0, "Rss": 0, "SwapPss": 0, "Pss_Anon": 0, "nproc": 0}
        for r in ROLES:
            tot.update({f"Pss_{r}": 0, f"Pss_Anon_{r}": 0, f"SwapPss_{r}": 0, f"n_{r}": 0})
        for pid in ([] if self.cg_only else cg_pids(self.cg)):
            if pid in self.exclude:
                continue
            m = smaps(pid)
            if m:
                tot["nproc"] += 1
                r = role(pid)
                tot[f"n_{r}"] += 1
                for k, v in m.items():
                    tot[k] += v
                    if k in ("Pss", "Pss_Anon", "SwapPss"):
                        tot[f"{k}_{r}"] += v
        s = cg_stat(self.cg)
        tot.update(cg_current=s.get("memory.current", 0), cg_anon=s.get("anon", 0), cg_file=s.get("file", 0),
                   cg_swap=s.get("memory.swap.current", 0), cpu_usec=s.get("usage_usec", 0), t=time.time())
        if self.cg_only:
            tot["nproc"] = 1
        return tot

    def run(self):
        last_keep = 0
        while not self.stop_ev.is_set():
            tot = self.sample()
            if tot["nproc"]:
                self.last = tot
                for k, v in tot.items():
                    if k != "t":
                        self.max[k] = max(self.max.get(k, 0), v)
                if tot["t"] - last_keep >= self.keep_every:
                    self.series.append(tot)
                    last_keep = tot["t"]
            self.stop_ev.wait(self.period)

    def stop(self):
        self.stop_ev.set()
        if self.is_alive():
            self.join(5)


SS_USER = re.compile(r'pid=(\d+)')
SS_BYTES = re.compile(r'bytes_(acked|received|sent):(\d+)')


class NetCensus(threading.Thread):
    """Non-loopback TCP connections of a cgroup's processes, from `ss -tinp` every period: peer → max bytes sent/received.
    Lower bound: connections opened and closed between two samples are missed."""

    def __init__(self, cg, period=0.5):
        super().__init__(daemon=True)
        self.cg, self.period = Path(cg), period
        self.socks = {}
        self.stop_ev = threading.Event()

    def run(self):
        while not self.stop_ev.is_set():
            pids = set(cg_pids(self.cg))
            try:
                out = subprocess.run(["ss", "-tinpH"], capture_output=True, text=True, timeout=5).stdout.splitlines()
            except subprocess.TimeoutExpired:
                out = []
            for i, line in enumerate(out):
                if line.startswith(("\t", " ")) or "users:" not in line:
                    continue
                f = line.split()
                local, peer = f[3], f[4]
                if peer.startswith(("127.", "[::1]", "[::ffff:127.")):
                    continue
                if not pids & {int(x) for x in SS_USER.findall(line)}:
                    continue
                info = out[i + 1] if i + 1 < len(out) and out[i + 1].startswith(("\t", " ")) else ""
                b = {k: int(v) for k, v in SS_BYTES.findall(info)}
                cur = self.socks.setdefault((local, peer), {"peer": peer, "sent": 0, "received": 0, "first": time.time()})
                cur["sent"] = max(cur["sent"], b.get("acked", b.get("sent", 0)))
                cur["received"] = max(cur["received"], b.get("received", 0))
            self.stop_ev.wait(self.period)

    def stop(self):
        self.stop_ev.set()
        if self.is_alive():
            self.join(10)

    def totals(self):
        return {"sent": sum(v["sent"] for v in self.socks.values()), "received": sum(v["received"] for v in self.socks.values()), "connections": len(self.socks)}

    def summary(self):
        peers = {}
        for v in self.socks.values():
            host = v["peer"].rsplit(":", 1)[0]
            p = peers.setdefault(host, {"connections": 0, "sent": 0, "received": 0})
            p["connections"] += 1
            p["sent"] += v["sent"]
            p["received"] += v["received"]
        return {"connections": len(self.socks), "bytes_sent": sum(v["sent"] for v in self.socks.values()),
                "bytes_received": sum(v["received"] for v in self.socks.values()), "peers": peers}


def scoped(unit, argv, statfile, slice_="batch-hb.slice", mem_max="12G", env=None, **popen):
    if env is not None:
        argv = ["/usr/bin/env", "-i", *[f"{k}={v}" for k, v in env.items()], *argv]
    cmd = ["systemd-run", "--user", "--scope", "--quiet", f"--slice={slice_}", f"--unit={unit}", "-p", f"MemoryMax={mem_max}",
           "--", str(ROOT / "bin/cgrun"), str(statfile), *argv]
    return subprocess.Popen(cmd, **popen)


def wait_cg(pid, unit, timeout=30):
    t0 = time.time()
    while time.time() - t0 < timeout:
        cg = proc_cg(pid)
        if cg and cg.name == f"{unit}.scope":
            return cg
        time.sleep(0.01)
    raise TimeoutError(f"cgroup for {unit}")


def read_stat(path):
    out = {}
    try:
        for line in open(path):
            k, _, v = line.partition(" ")
            out[k] = int(v)
    except (OSError, ValueError):
        pass
    return out


def children(pid):
    try:
        return [int(x) for x in open(f"/proc/{pid}/task/{pid}/children").read().split()]
    except OSError:
        return []


def descendants(pid):
    out = []
    for c in children(pid):
        out += [c, *descendants(c)]
    return out


def cmdline(pid):
    try:
        return open(f"/proc/{pid}/cmdline").read().replace("\0", " ").strip()
    except OSError:
        return ""


# ---------- mock ----------

class Mock:
    def __init__(self, out, port, ttft=1700, tps=120, bodies=True, ttft_per_ktok=0):
        self.out, self.port = Path(out), port
        self.out.mkdir(parents=True, exist_ok=True)
        env = {**os.environ, "HB_OUT": str(self.out), "HB_PORT": str(port), "HB_TTFT_MS": str(ttft), "HB_TPS": str(tps), "HB_BODIES": "1" if bodies else "0", "HB_TTFT_PER_KTOK_MS": str(ttft_per_ktok)}
        self.proc = subprocess.Popen(["node", str(ROOT / "mock.mjs")], env=env, stdout=open(self.out / "mock.log", "w"), stderr=subprocess.STDOUT)
        self.log = self.out / "requests.jsonl"
        t0 = time.time()
        while not (self.out / "mock.log").read_text().startswith("hb mock"):
            if time.time() - t0 > 10 or self.proc.poll() is not None:
                raise RuntimeError("mock did not start")
            time.sleep(0.05)

    def url(self, tag):
        return f"http://127.0.0.1:{self.port}/t/{tag}"

    def records(self, tag=None):
        if not self.log.exists():
            return []
        rs, by_n = [], {}
        for l in open(self.log):
            if not l.strip():
                continue
            r = json.loads(l)
            if "tag" in r:
                rs.append(r)
                by_n[r["n"]] = r
            elif r.get("n") in by_n:
                by_n[r["n"]].update({k: v for k, v in r.items() if k != "n"})
        return [r for r in rs if tag is None or r.get("tag") == tag]

    def wait(self, pred, timeout, tag=None, poll=0.1):
        t0 = time.time()
        while time.time() - t0 < timeout:
            rs = [r for r in self.records(tag) if pred(r)]
            if rs:
                return rs[0]
            time.sleep(poll)
        return None

    def stop(self):
        self.proc.terminate()
        self.proc.wait(5)


def finals(recs, scenario):
    return [r for r in recs if r.get("reply") == "text" and r.get("scenario") == scenario and not r.get("gave_up")]


def token_summary(recs):
    main = [r for r in recs if r.get("api") in ("anthropic", "responses", "chat", "gemini")]
    turns = [r for r in main if r.get("scenario")]
    aux = [r for r in main if not r.get("scenario")]
    first = turns[0] if turns else {}
    return {"requests": len(main), "turn_requests": len(turns), "aux_requests": len(aux), "count_token_calls": sum(r.get("api") == "anthropic_count" for r in recs),
            "input_bytes": sum(r["bytes"] for r in main), "est_input_tokens": sum(r.get("est_input", 0) for r in main),
            "est_output_tokens": sum(r.get("out_tokens", 0) for r in main), "est_cached_tokens": sum(r.get("est_cached", 0) for r in main),
            "est_uncached_tokens": sum(r.get("est_input", 0) - r.get("est_cached", 0) for r in main),
            "aux_bytes": sum(r["bytes"] for r in aux), "gave_up": sum(1 for r in main if r.get("gave_up")),
            "first_request_bytes": first.get("bytes"), "first_request_est_tokens": first.get("est_input"),
            "first_sys_chars": first.get("sys_chars"), "first_tools_n": first.get("tools_n"), "first_tools_chars": first.get("tools_chars"),
            "cache_marks_first": first.get("cache_marks"), "injected_429": sum(1 for r in recs if r.get("injected")),
            "aux_kinds": sorted({(r.get("model") or "?") + ":" + str(r.get("tools_n")) for r in aux})}


# ---------- workspace ----------

def workspace(path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    for f in FIX.iterdir():
        shutil.copy2(f, path / f.name)
    return path


def shuffled(names, run):
    import random
    xs = list(names)
    random.Random(run).shuffle(xs)
    return xs


def runid(prefix):
    """Creates RESULTS/<prefix>-<timestamp>[-k]: parallel runs started in the same second get separate dirs (names stay short: tmux socket paths)."""
    ts = time.strftime('%Y%m%d-%H%M%S')
    RESULTS.mkdir(parents=True, exist_ok=True)
    for k in range(100):
        name = f"{prefix}-{ts}" + (f"-{k}" if k else "")
        try:
            (RESULTS / name).mkdir()
            return name
        except FileExistsError:
            continue
    raise RuntimeError("runid: no free name")


def save(out, name, data):
    out.mkdir(parents=True, exist_ok=True)
    (out / name).write_text(json.dumps(data, indent=1, default=str))


def sha256(path):
    import hashlib
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()[:16]
    except OSError:
        return None


def meta():
    from harnesses import CLI as H, FLAGS
    git = lambda *a: subprocess.run(["git", *a], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    slices = subprocess.run(["systemctl", "--user", "list-units", "batch-hb*.slice", "--no-legend", "--plain"], capture_output=True, text=True).stdout.split()
    pm = {}
    for p in PLUGINS.values():
        pm.update(p.meta())
    return {"git_commit": git("rev-parse", "--short", "HEAD"), "git_dirty": bool(git("status", "--porcelain", "--", "hb.py", "mock.mjs", "harnesses.py", "bin/cgrun", *[f"plugin_{k}.py" for k in PLUGINS])),
            "mock_sha256": sha256(ROOT / "mock.mjs"), **{k: v for k, v in pm.items() if k != "versions"},
            "versions": {**pm.get("versions", {}), **{k: v.version for k, v in H.items()}}, "flags": FLAGS, "argv": sys.argv,
            "hb_slices_active": [x for x in slices if x.endswith(".slice")], "started": time.strftime("%Y-%m-%dT%H:%M:%S%z")}


def host_info():
    mem = {l.split(":")[0]: int(l.split()[1]) for l in open("/proc/meminfo") if l.split(":")[0] in ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree")}
    return {"loadavg": open("/proc/loadavg").read().split()[:3], "meminfo_kib": mem, "nproc": os.cpu_count(), "kernel": os.uname().release}


def guard(need_gib, wait_s=float(os.environ.get("HB_GUARD_WAIT_H", 4)) * 3600):
    """Wait (up to wait_s) until the host has the RAM headroom and a load below 12; then refuse."""
    t0 = time.time()
    while True:
        avail = int(next(l for l in open("/proc/meminfo") if l.startswith("MemAvailable")).split()[1]) / 2**20
        load = float(open("/proc/loadavg").read().split()[1])
        if avail >= need_gib + 8 and load <= float(os.environ.get("HB_MAX_LOAD", 12)):
            return
        if time.time() - t0 > wait_s:
            sys.exit(f"refuse: MemAvailable {avail:.1f} GiB (need {need_gib}+8), load5 {load} (max 12) after {wait_s}s")
        print(f"guard: waiting (MemAvailable {avail:.1f} GiB, load5 {load})", file=sys.stderr, flush=True)
        time.sleep(60)


# ---------- scenario: task ----------

def task_cli(h, mock, out, run):
    tag = f"{h.name}-task-{run}"
    base = WORK / tag
    if base.exists():
        shutil.rmtree(base)
    ws = workspace(base / "ws")
    home = warm_home(h, base, mock, tag, ws)
    plant_canaries(ws, home)
    unit = f"hb-{tag}-{os.getpid()}"
    stat = base / "cg.stat"
    t0 = time.time()
    p = scoped(unit, h.run(PROMPTS["HBTASK"]), stat, cwd=ws, env={**h.env(home), "HB_SECRET": CANARIES["env_var"]}, stdin=subprocess.DEVNULL, stdout=open(base / "stdout", "w"), stderr=subprocess.STDOUT)
    cg = wait_cg(p.pid, unit)
    s = Sampler(cg)
    net = NetCensus(cg)
    s.start()
    net.start()
    try:
        rc = p.wait(300)
    except subprocess.TimeoutExpired:
        subprocess.run(["systemctl", "--user", "kill", "-s", "KILL", f"{unit}.scope"])
        p.wait(30)
        rc = "timeout"
    wall = time.time() - t0
    s.stop()
    net.stop()
    return {**task_result(h.name, run, tag, mock, t0, wall, rc, read_stat(stat), s, ws), "net": net.summary()}


def warm_home(h, base, mock, tag, ws):
    """Fresh HOME, then one unmeasured warm-up turn in another workspace (first-run downloads, caches, migrations), tagged <tag>-warm."""
    home = base / "home"
    wws = workspace(base / "warm-ws")
    h.prepare(home, mock.url(tag + "-warm"), [wws, ws])
    subprocess.run(h.run(PROMPTS["HBIDLE"]), cwd=wws, env=h.env(home), stdin=subprocess.DEVNULL, stdout=open(base / "warm.out", "w"), stderr=subprocess.STDOUT, timeout=180)
    h.prepare(home, mock.url(tag), [wws, ws])
    return home


CANARIES = {"workspace_dotenv": "HBCANARY-DOTENV-7f3a", "home_aws": "HBCANARY-AWS-91c2", "env_var": "HBCANARY-ENV-5d0e"}


def plant_canaries(ws, home):
    (ws / ".env").write_text(f"API_TOKEN={CANARIES['workspace_dotenv']}\n")
    (home / ".aws").mkdir(parents=True, exist_ok=True)
    (home / ".aws/credentials").write_text(f"[default]\naws_secret_access_key = {CANARIES['home_aws']}\n")


def canary_hits(mock, tag):
    import gzip
    hits = {k: 0 for k in CANARIES}
    for r in mock.records(tag):
        f = mock.out / "bodies" / f"{r['n']:06d}.json.gz"
        if f.exists():
            body = gzip.open(f).read().decode("utf8", "replace")
            for k, v in CANARIES.items():
                hits[k] += v in body
    return hits


def task_result(name, run, tag, mock, t0, wall, rc, st, s, ws):
    recs = mock.records(tag)
    main = [r for r in recs if r.get("api") in ("anthropic", "responses", "chat", "gemini")]
    turn = [r for r in main if r.get("scenario")]
    fin = finals(recs, "HBTASK")
    return {"harness": name, "run": run, "tag": tag, "rc": rc, "wall_s": round(wall, 2),
            "success": "return a + b" in (ws / "calc.py").read_text() and subprocess.run(["python3", "test_calc.py"], cwd=ws, capture_output=True).returncode == 0,
            "t_first_request_s": round(main[0]["t"] - t0, 3) if main else None, "t_first_turn_request_s": round(turn[0]["t"] - t0, 3) if turn else None,
            "t_final_request_s": round(fin[0]["t"] - t0, 3) if fin else None,
            "t_done_s": round(fin[0]["t_end"] - t0, 3) if fin and fin[0].get("t_end") else None,
            "harness_gaps_ms": [round((b["t"] - a["t_end"]) * 1000) for a, b in zip(turn, turn[1:]) if a.get("t_end")],
            "leftover_procs": st.get("leftover_procs"),
            "cg_memory_peak_mib": round(st["memory_peak"] / 2**20, 1) if "memory_peak" in st else None, "cpu_s": round(st["usage_usec"] / 1e6, 2) if "usage_usec" in st else None,
            "cpu_user_s": round(st.get("user_usec", 0) / 1e6, 2), "cpu_system_s": round(st.get("system_usec", 0) / 1e6, 2),
            "max_pss_anon_mib": round(s.max.get("Pss_Anon", 0) / 1024, 1),
            "max_pss_mib": round(s.max.get("Pss", 0) / 1024, 1), "max_rss_mib": round(s.max.get("Rss", 0) / 1024, 1),
            "max_anon_mib": round(s.max.get("cg_anon", 0) / 2**20, 1), "max_nproc": s.max.get("nproc", 0), "canary_in_requests": canary_hits(mock, tag), **token_summary(recs)}


def cmd_task(a):
    out = RESULTS / runid("task")
    mock = Mock(out / "mock", a.port, a.ttft, a.tps, ttft_per_ktok=a.ttft_per_ktok)
    rows, m, done = [], meta(), False
    try:
        for run in range(a.runs):
            for name in shuffled(a.harness, run):
                r = PLUGINS[name].task(mock, out, run) if name in PLUGINS else task_cli(CLI[name], mock, out, run)
                print(json.dumps({k: r.get(k) for k in ("harness", "run", "success", "wall_s", "t_first_request_s", "cg_memory_peak_mib", "max_anon_mib", "cpu_s", "requests", "est_input_tokens")}), flush=True)
                rows.append(r)
        done = True
    finally:
        mock.stop()
        save(out, "task.json", {"meta": m, "complete": done, "host": host_info(), "ttft_ms": a.ttft, "ttft_per_ktok_ms": a.ttft_per_ktok, "tps": a.tps, "rows": rows})
    print(out)


# ---------- scenario: crash ----------
# Modes (same rule for every harness):
#   kill9  SIGKILL every process whose executable is the harness itself (harness binaries, node running a harness, a plugin's server processes);
#          tool processes (bash, python, cat) are left alone, as after a real harness crash.
#   scope  SIGKILL everything in the harness scope, tools included (OOM kill / reboot analogue).
#   term   the harness's terminal dies: TUI in a private tmux, tmux server killed (server-style plugins: their attached client runs in that tmux).
#   stream kill9 while the final answer is streaming (no tool running).
#   sigterm graceful shutdown mid-turn: SIGTERM to the kill9 victims, up to 10 s to exit, then SIGKILL; resume shows whether the turn was saved.
# Then: CLIs = one-shot resume ("continue"), i.e. user action; server-style plugins restart their server on the same state, no user input.

HARNESS_EXE = (str(ROOT / "harnesses"), "/usr/bin/node", str(ROOT / "bin"))


def exe(pid):
    try:
        return os.path.realpath(f"/proc/{pid}/exe")
    except OSError:
        return ""


def harness_procs(cg, extra=()):
    out = []
    for pid in cg_pids(cg):
        e = exe(pid)
        if e.startswith(HARNESS_EXE + tuple(extra)):
            out.append(pid)
    return out


def tool_pid(cg, timeout=15):
    t0 = time.time()
    while time.time() - t0 < timeout:
        for pid in cg_pids(cg):
            if cmdline(pid).startswith("python3 slow_step.py"):
                return pid
        time.sleep(0.05)
    return None


def read_text(path):
    try:
        return open(path).read()
    except OSError:
        return ""


def kill_all(pids, sig=signal.SIGKILL):
    for x in pids:
        try:
            os.kill(x, sig)
        except ProcessLookupError:
            pass


def crash_point(mock, tag, mode, timeout=120):
    if mode == "stream":
        return mock.wait(lambda x: x.get("reply") == "text" and x.get("scenario") == "HBSTREAM", timeout, tag)
    return mock.wait(lambda x: x.get("step") == "TWO-DONE", timeout, tag)


def resume_facts(after, t_kill, t_res, scenario):
    main = [x for x in after if x.get("api") in ("anthropic", "responses", "chat", "gemini") and not x.get("side")]
    turn = [x for x in main if x.get("scenario") or "continue" in (x.get("users_tail") or "")]
    first = turn[0] if turn else (main[0] if main else {})
    fin = [x for x in main if x.get("reply") == "text" and x.get("scenario") == scenario]
    return {"resume_requests": len(main),
            "resume_prompt_recovered": first.get("scenario") == scenario,
            "n_tool_results_in_resume_request": first.get("n_outputs"),
            "resume_markers": first.get("markers"),
            "partial_answer_chars_kept": first.get("assistant_chars"),
            "slow_step_reissued": any(x.get("step") == "TWO-DONE" for x in after),
            "final_requested_again": bool(fin),
            "kill_to_final_s": round(fin[0]["t"] - t_kill, 2) if fin else None,
            "resume_to_final_s": round(fin[0]["t"] - t_res, 2) if fin and t_res else None}


def crash_cli(h, mock, run, mode):
    scenario = "HBSTREAM" if mode == "stream" else "HBSLOW"
    tag = f"{h.name}-crash{mode}-{run}"
    base = WORK / tag
    if base.exists():
        shutil.rmtree(base)
    ws = workspace(base / "ws")
    home = warm_home(h, base, mock, tag, ws)
    unit = f"hb-{tag}-{os.getpid()}"
    r = {"harness": h.name, "run": run, "mode": mode, "tag": tag}
    tmux = None
    if mode == "term":
        sl = f"batch-hbcrash{os.getpid()}.slice"
        tmux = Tmux(base, sl, "8G")
        tmux.tui("a000", ws, h.env(home), h, PROMPTS[scenario])
        cg = slice_cg(sl)
        p = None
    else:
        p = scoped(unit, h.run(PROMPTS[scenario]), base / "cg.stat", cwd=ws, env=h.env(home), stdin=subprocess.DEVNULL, stdout=open(base / "stdout", "w"), stderr=subprocess.STDOUT)
        cg = wait_cg(p.pid, unit)
    if not crash_point(mock, tag, mode):
        stop_all(p, unit, tmux, cg, home=home)
        return {**r, "error": "crash point never reached"}
    sleeper = tool_pid(cg) if mode != "stream" else None
    time.sleep(3 if mode != "stream" else 5)
    t_kill = time.time()
    if mode == "term":
        r["killed"] = "tmux server (terminal)"
        tmux.tm("kill-server", check=False)
    elif mode == "scope":
        r["killed"] = "whole scope"
        subprocess.run(["systemctl", "--user", "kill", "-s", "KILL", f"{unit}.scope"])
    elif mode == "sigterm":
        victims = harness_procs(cg, [str(home)])
        r["killed"] = [cmdline(x)[:60] for x in victims]
        kill_all(victims, signal.SIGTERM)
        alive = lambda x: (lambda st: bool(st) and st.rsplit(")", 1)[-1].split()[0] != "Z")(read_text(f"/proc/{x}/stat"))
        while time.time() - t_kill < 10 and any(alive(x) for x in victims):
            time.sleep(0.1)
        left = [x for x in victims if alive(x)]
        r["exited_on_sigterm"] = not left
        r["sigterm_exit_s"] = None if left else round(time.time() - t_kill, 2)
        kill_all(left)
    else:
        victims = harness_procs(cg, [str(home)])
        r["killed"] = [cmdline(x)[:60] for x in victims]
        kill_all(victims)
    time.sleep(2)
    r["tool_survived_2s"] = bool(sleeper and os.path.exists(f"/proc/{sleeper}"))
    t_wait = time.time()
    fin = None
    while time.time() - t_wait < 40:
        fin = next((x for x in mock.records(tag) if x["t"] > t_kill and x.get("reply") == "text" and x.get("scenario") == scenario), None)
        if fin or not harness_procs(cg, [str(home)]):
            break
        time.sleep(0.2)
    r["turn_completed_without_user"] = bool(fin)
    r["harness_alive_after_fault"] = bool(harness_procs(cg, [str(home)]))
    stop_all(p, unit, None, cg, keep_tool=True, home=home)
    n_before = len(mock.records(tag))
    t_res = time.time()
    if not fin:
        p2 = scoped(unit + "-r", h.resume("continue"), base / "cg2.stat", cwd=ws, env=h.env(home), stdin=subprocess.DEVNULL, stdout=open(base / "stdout2", "w"), stderr=subprocess.STDOUT)
        try:
            r["resume_rc"] = p2.wait(150)
        except subprocess.TimeoutExpired:
            subprocess.run(["systemctl", "--user", "kill", "-s", "KILL", f"{unit}-r.scope"])
            r["resume_rc"] = "timeout"
        r["user_action_needed"] = True
    else:
        r["user_action_needed"] = False
        r["kill_to_final_s"] = round(fin["t"] - t_kill, 2)
    after = mock.records(tag)[n_before:]
    if not fin:
        r.update(resume_facts(after, t_kill, t_res, scenario))
    r["double_execution"] = bool(r["tool_survived_2s"] and r.get("slow_step_reissued"))
    if sleeper and os.path.exists(f"/proc/{sleeper}"):
        os.kill(sleeper, signal.SIGKILL)
    if tmux:
        subprocess.run(["systemctl", "--user", "stop", f"batch-hbcrash{os.getpid()}.slice"], capture_output=True)
    return r


def stop_all(p, unit, tmux, cg, keep_tool=False, home=None):
    if tmux:
        tmux.tm("kill-server", check=False)
    kill_all(harness_procs(cg, [str(home)] if home else []))
    if p:
        try:
            p.wait(20)
        except subprocess.TimeoutExpired:
            if not keep_tool:
                subprocess.run(["systemctl", "--user", "kill", "-s", "KILL", f"{unit}.scope"])
            p.wait(10)


def slice_or(p, unit):
    return proc_cg(p.pid) or CG_ROOT


def cmd_crash(a):
    out = RESULTS / runid("crash")
    mock = Mock(out / "mock", a.port, a.ttft, a.tps, ttft_per_ktok=a.ttft_per_ktok)
    rows, m, done = [], meta(), False
    try:
        for run in range(a.runs):
            for mode in a.mode:
                for name in shuffled(a.harness, run):
                    r = PLUGINS[name].crash(mock, run, mode) if name in PLUGINS else crash_cli(CLI[name], mock, run, mode)
                    print(json.dumps(r), flush=True)
                    rows.append(r)
        done = True
    finally:
        mock.stop()
        save(out, "crash.json", {"meta": m, "complete": done, "host": host_info(), "rows": rows})
    print(out)


# ---------- scenario: faults (quota, auth, network) ----------

FAULTS = {
    "429x3": {"kind": 429, "after": 1, "count": 3}, "429-60s": {"kind": 429, "after": 1, "window_s": 60},
    "529x3": {"kind": 529, "after": 1, "count": 3}, "500x3": {"kind": 500, "after": 1, "count": 3},
    "401-60s": {"kind": 401, "after": 1, "window_s": 60},
    "reset-30s": {"kind": "reset", "after": 1, "window_s": 30}, "hang-30s": {"kind": "hang", "after": 1, "window_s": 30},
    "truncate": {"kind": "truncate", "after": 1, "count": 1}, "stall-60s": {"kind": "stall", "after": 1, "count": 1, "window_s": 60},
    "trickle": {"kind": "trickle", "after": 1, "count": 1}, "sse-error": {"kind": "sse_error", "after": 1, "count": 1},
    "malformed": {"kind": "malformed", "after": 1, "count": 1},
    "tool-hang": {"scenario": "HBHANG", "timeout": 420}, "tool-huge": {"scenario": "HBHUGE", "timeout": 300},
    "loop-3min": {"scenario": "HBLOOP", "timeout": 180},
}
# edition 2026-12: harder faults on the HBFX task (4 steps, each appends one line to sidefx.log), graded (fault_grade)
FX = {"scenario": "HBFX"}
FAULTS.update({
    "503-5min": {**FX, "kind": 503, "after": 1, "window_s": 300, "timeout": 720},
    "503-15min": {**FX, "kind": 503, "after": 1, "window_s": 900, "timeout": 1920},
    "529-5min": {**FX, "kind": 529, "after": 1, "window_s": 300, "timeout": 720},
    "429-5min-noRA": {**FX, "kind": 429, "after": 1, "window_s": 300, "retry_after": False, "timeout": 720},
    "429-RA3600": {**FX, "kind": 429, "after": 1, "count": 1, "retry_after": 3600, "timeout": 600},
    "auth-5min": {**FX, "kind": 401, "after": 1, "window_s": 300, "timeout": 720},
    "stall-5min": {**FX, "kind": "hang", "after": 1, "window_s": 330, "timeout": 780},
    "flap-3min": {**FX, "kind": "flap", "after": 1, "window_s": 180, "timeout": 480},
    "compound": {**FX, "kind": "seq", "seq": ["429", "reset", "truncate"], "after": 1, "count": 3, "timeout": 300},
    "html200": {**FX, "kind": "html200", "after": 1, "count": 3, "timeout": 300},
    "empty200": {**FX, "kind": "empty200", "after": 1, "count": 3, "timeout": 300},
    "413": {**FX, "kind": 413, "after": 1, "count": 1, "timeout": 300},
    "overflow": {**FX, "kind": "overflow", "after": 3, "timeout": 400},
    "truncate-tool": {**FX, "kind": "truncate_tool", "after": 1, "count": 1, "timeout": 300},
    "max-tokens-tool": {**FX, "kind": "max_tokens_tool", "after": 1, "count": 1, "timeout": 300},
    "bad-tool-call": {**FX, "kind": "bad_tool_call", "after": 1, "count": 1, "timeout": 300},
    "unknown-tool": {**FX, "kind": "unknown_tool", "after": 1, "count": 1, "timeout": 300},
    "dup-events": {**FX, "kind": "dup_events", "after": 1, "count": 1, "timeout": 300},
    "reorder": {**FX, "kind": "reorder", "after": 1, "count": 1, "timeout": 300},
    "tool-flood": {"scenario": "HBFLOOD", "timeout": 300},
    # disk full (ENOSPC, needs HB_ENOSPC_DIR): tool = workspace on the full filesystem, the steps' appends fail (success = turn finished);
    # session = HOME (harness config, sessions, logs) on it, workspace elsewhere (success = all four steps)
    "enospc-tool": {**FX, "enospc": "tool", "timeout": 300},
    "enospc-session": {**FX, "enospc": "session", "timeout": 300},
})
# 2026-12: the twelve 2026-11 provider faults also run on HBFX and are graded (one graded set; their 2026-11 HBTASK rows are not carried)
for _k in ["429x3", "429-60s", "529x3", "500x3", "401-60s", "reset-30s", "hang-30s", "truncate", "stall-60s", "trickle", "sse-error", "malformed"]:
    FAULTS[_k] = {**FX, **FAULTS[_k], "timeout": 300}
FAULTS_2026_11 = ["429x3", "429-60s", "529x3", "500x3", "401-60s", "reset-30s", "hang-30s", "truncate", "stall-60s", "trickle", "sse-error", "malformed", "tool-hang", "tool-huge", "loop-3min"]
FAULTS_2026_12 = list(FAULTS)  # graded run of edition 2026-12: basic + hard
FAULTS_NOT_IN_TOTALS = ["429-RA3600", "loop-3min"]


def fault_grade(done_alone, continues, dup, wasted):
    """1.0 alone, 0.6 after one "continue", 0.3 after two, else 0; -0.2 if a side effect ran twice; -0.1 per 10 wasted requests; floor 0."""
    base = 1.0 if done_alone else {1: 0.6, 2: 0.3}.get(continues, 0.0)
    return round(max(0.0, base - (0.2 if dup else 0) - 0.1 * (wasted // 10)), 2)


def sidefx(ws):
    f = ws / "sidefx.log"
    lines = [l.strip() for l in f.read_text().splitlines() if l.strip()] if f.exists() else []
    return {"lines": len(lines), "steps": sorted(set(lines)), "duplicates": len(lines) - len(set(lines))}


def ctl(mock, body):
    import urllib.request
    req = urllib.request.Request(f"http://127.0.0.1:{mock.port}/t/x/ctl", data=json.dumps(body).encode(), headers={"content-type": "application/json"})
    urllib.request.urlopen(req, timeout=5).read()


def fault_facts(recs, t0, sc):
    main = [r for r in recs if r.get("api") in ("anthropic", "responses", "chat", "gemini")]
    inj = [r for r in main if r.get("injected")]
    ok = [r for r in main if not r.get("injected")]
    after = [r for r in ok if inj and r["t"] > inj[-1]["t"]]
    span = (inj[-1]["t"] - inj[0]["t"]) if len(inj) > 1 else 0
    fin = finals(recs, sc)
    out = {"requests": len(main), "injected": len(inj), "fault_span_s": round(span, 1),
           "requests_per_min_during_fault": round(len(inj) / span * 60, 1) if span else None,
           "gaps_between_failed_s": [round(b["t"] - a["t"], 1) for a, b in zip(inj, inj[1:])][:12],
           "recovered_request_after_last_fault_s": round(after[0]["t"] - (inj[-1].get("t_end") or inj[-1]["t"]), 1) if after and inj else None,
           "final_reached": bool(fin), "t_final_s": round(fin[0]["t"] - t0, 1) if fin else None,
           "est_input_tokens": sum(r.get("est_input", 0) for r in main)}
    seq = [r for r in main if str(r.get("step") or "").startswith("seq")]
    if seq:
        nxt = lambda r: next((x for x in main if x["n"] > r["n"]), None)
        out["tool_return_s"] = [round(nxt(r)["t"] - r["t_end"], 1) if nxt(r) and r.get("t_end") else None for r in seq][:6]
        out["next_request_kib"] = [round(nxt(r)["bytes"] / 1024) if nxt(r) else None for r in seq][:6]
    return out


def tests_pass(ws):
    return "return a + b" in (ws / "calc.py").read_text() and subprocess.run(["python3", "test_calc.py"], cwd=ws, capture_output=True).returncode == 0


ENOSPC_DIR = Path(os.environ.get("HB_ENOSPC_DIR", Path.home() / ".cache/harness-bench/enospc"))


def fault_cli(h, mock, run, prof):
    where = FAULTS[prof].get("enospc")
    if not where:
        return _fault_cli(h, mock, run, prof)
    # needs a small dedicated filesystem (setup: 64 MB ext4 loop, -m 0, owned by the user, mounted at HB_ENOSPC_DIR); one run at a time
    if not os.path.ismount(ENOSPC_DIR):
        return {"harness": h.name, "run": run, "fault": prof, "error": f"{ENOSPC_DIR} is not a mounted small filesystem"}
    WORK.mkdir(parents=True, exist_ok=True)
    with open(WORK / "enospc.lock", "w") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        clear = lambda: [shutil.rmtree(x) if x.is_dir() else x.unlink() for x in ENOSPC_DIR.iterdir() if x.name != "lost+found"]
        clear()
        try:
            return _fault_cli(h, mock, run, prof, small=ENOSPC_DIR, where=where)
        finally:
            clear()


def fill(fs):
    """Ballast files until the filesystem is full, so the next real write fails with ENOSPC."""
    v = os.statvfs(fs)
    fd = os.open(fs / "ballast", os.O_CREAT | os.O_WRONLY)
    try:
        os.posix_fallocate(fd, 0, max(4096, v.f_bavail * v.f_frsize - 65536))
    except OSError:
        pass
    finally:
        os.close(fd)
    with open(fs / "ballast2", "wb", buffering=0) as f:
        try:
            while True:
                f.write(b"\0" * 1024)
        except OSError:
            pass
    v = os.statvfs(fs)
    return v.f_bavail * v.f_frsize


def _fault_cli(h, mock, run, prof, small=None, where=None):
    spec = {k: v for k, v in FAULTS[prof].items() if k != "enospc"}
    sc, tmo = spec.pop("scenario", "HBTASK"), spec.pop("timeout", 240)
    tag = f"{h.name}-fault-{prof}-{run}"
    base = WORK / tag
    if base.exists():
        shutil.rmtree(base)
    ws = workspace((small if where == "tool" else base) / "ws")
    if sc == "HBFX":
        for f in (ROOT / "fixtures/long").glob("big_0[1-4].txt"):
            shutil.copy2(f, ws / f.name)
    hbase = small / tag if where == "session" else base
    hbase.mkdir(parents=True, exist_ok=True)
    base.mkdir(parents=True, exist_ok=True)
    try:
        home = warm_home(h, hbase, mock, tag, ws)
    except OSError as e:
        return {"harness": h.name, "run": run, "fault": prof, "error": f"warm-up on the small filesystem failed: {e}"}
    free_left = fill(small) if small else None
    if spec:
        ctl(mock, {"tag": tag, **spec})
    unit = f"hb-{tag}-{os.getpid()}"
    t0 = time.time()
    p = scoped(unit, h.run(PROMPTS[sc]), base / "cg.stat", cwd=ws, env=h.env(home), stdin=subprocess.DEVNULL, stdout=open(base / "stdout", "w"), stderr=subprocess.STDOUT)
    cg = wait_cg(p.pid, unit)
    smp = Sampler(cg, period=0.5)
    smp.start()
    try:
        rc = p.wait(tmo)
    except subprocess.TimeoutExpired:
        subprocess.run(["systemctl", "--user", "kill", "-s", "KILL", f"{unit}.scope"])
        p.wait(30)
        rc = "timeout"
    smp.stop()
    st_ = read_stat(base / "cg.stat")
    out_tail = (base / "stdout").read_text(errors="replace")[-300:]
    if where == "tool":
        fx_done = lambda: bool(finals(mock.records(tag), "HBFX"))
    elif sc == "HBFX":
        fx_done = lambda: bool(finals(mock.records(tag), "HBFX")) and {f"FX-{k}" for k in (1, 2, 3, 4)} <= set(sidefx(ws)["steps"])
    elif sc == "HBLOOP":  # success = the harness ends the runaway turn itself before the timeout; no continues
        fx_done = lambda: rc != "timeout"
    else:
        fx_done = lambda: bool(finals(mock.records(tag), sc))
    r = {"harness": h.name, "run": run, "fault": prof, "scenario": sc, "rc": rc, "wall_s": round(time.time() - t0, 1),
         "success_without_user": tests_pass(ws) if sc == "HBTASK" else fx_done(), "stdout_tail": out_tail,
         "max_anon_mib": round(smp.max.get("cg_anon", 0) / 2**20), "cg_memory_peak_mib": round(st_.get("memory_peak", 0) / 2**20), "cpu_s": round(st_.get("usage_usec", 0) / 1e6, 1),
         "leftover_procs": st_.get("leftover_procs"), **fault_facts(mock.records(tag), t0, sc)}
    if small:
        r.update(enospc=where, enospc_free_bytes=free_left, enospc_in_output=bool(re.search(r"no space left|ENOSPC", (base / "stdout").read_text(errors="replace"), re.I)))
    if sc == "HBTASK":
        ctl(mock, {"tag": tag})
    if sc == "HBTASK" and not r["success_without_user"]:
        n0 = len(mock.records(tag))
        p2 = subprocess.run(["timeout", "240", *h.resume("continue")], cwd=ws, env=h.env(home), stdin=subprocess.DEVNULL, capture_output=True, text=True)
        after = mock.records(tag)[n0:]
        first = next((x for x in after if x.get("api") in ("anthropic", "responses", "chat", "gemini") and not x.get("side")), {})
        r.update(resume_rc=p2.returncode, success_after_resume=tests_pass(ws), resume_prompt_recovered=first.get("scenario") == "HBTASK",
                 n_tool_results_in_resume_request=first.get("n_outputs"))
    if sc != "HBTASK":
        recs = [x for x in mock.records(tag) if x.get("api") in ("anthropic", "responses", "chat", "gemini") and not x.get("side")]
        inj = [x for x in recs if x.get("injected")]
        win = FAULTS[prof].get("window_s")
        # fault end: window profiles = first injected request + window; counted = end of the last injected response
        fault_end = (inj[0]["t"] + win) if inj and win else max((x.get("t_end") or x["t"]) for x in inj) if inj else None
        ok_after = [x for x in recs if inj and not x.get("injected") and x["n"] > inj[-1]["n"] and x.get("scenario") == sc and x.get("t_end")]
        r["recovery_s"] = round(max(0.0, ok_after[0]["t_end"] - fault_end), 1) if ok_after and fault_end else None
        # allowance: counted faults = count; windows = one request per step of a 1, 2, 4 ... 60 s backoff over the fault span, plus the first
        if inj and win:
            span, k, t, step = fault_end - inj[0]["t"], 1, 0.0, 1.0
            while t + step <= span:
                t, k, step = t + step, k + 1, min(60.0, step * 2)
            allow = k
        else:
            allow = FAULTS[prof].get("count") or (1 if inj else 0)  # overflow: the first rejection is unavoidable
        wasted = max(0, len(inj) - allow)
        r.update(fault_end_s=round(fault_end - t0, 1) if fault_end else None, wasted_allowance=allow)
        if win and inj:  # the user presses "continue" once the outage is over, as in real use; overflow stays armed during the continues
            time.sleep(max(0.0, fault_end - time.time()))
        if FAULTS[prof].get("kind") != "overflow":
            ctl(mock, {"tag": tag})
        cont = 0
        while not fx_done() and cont < 2 and sc != "HBLOOP":
            cont += 1
            subprocess.run(["timeout", "240", *h.resume("continue")], cwd=ws, env=h.env(home), stdin=subprocess.DEVNULL, capture_output=True, text=True)
        ctl(mock, {"tag": tag})
        fx = sidefx(ws)
        recs = [x for x in mock.records(tag) if x.get("api") in ("anthropic", "responses", "chat", "gemini") and not x.get("side")]
        done = fx_done()
        r.update(continues_used=cont if done else None, done_after_continues=done, sidefx=fx, wasted_requests=wasted,
                 fault_na=any(x.get("fault_na") for x in recs),
                 grade=fault_grade(r["success_without_user"], cont if done else 99, fx["duplicates"] > 0, wasted))
    return r


def cmd_fault(a):
    out = RESULTS / runid("fault")
    mock = Mock(out / "mock", a.port, a.ttft, a.tps, ttft_per_ktok=a.ttft_per_ktok)
    rows, m, done = [], meta(), False
    try:
        for run in range(a.runs):
            for prof in a.profile:
                for name in shuffled(a.harness, run):
                    r = PLUGINS[name].fault(mock, run, prof) if name in PLUGINS else fault_cli(CLI[name], mock, run, prof)
                    print(json.dumps(r), flush=True)
                    rows.append(r)
        done = True
    finally:
        mock.stop()
        save(out, "fault.json", {"meta": m, "complete": done, "host": host_info(), "profiles": FAULTS, "rows": rows})
    print(out)


# ---------- scenario: herd (fleet behind one shared quota) ----------

def herd_facts(recs, n, t0, done_agents):
    main = [r for r in recs if r.get("api") in ("anthropic", "responses", "chat", "gemini")]
    rej = [r for r in main if r.get("injected") == "quota"]
    fin = {}
    for r in finals(recs, "HBTASK"):
        if r.get("agent"):
            fin.setdefault(r["agent"], r["t"] - t0)
    times = sorted(fin.values())
    jain = (sum(times) ** 2 / (len(times) * sum(t * t for t in times))) if times else None
    return {"agents": n, "tasks_done": len(done_agents), "final_requests_agents": len(fin), "requests": len(main), "rejected_429": len(rej),
            "accepted": len(main) - len(rej), "wasted_ratio": round(len(rej) / len(main), 3) if main else None,
            "t_all_done_s": round(times[-1], 1) if len(times) == n else None, "t_median_done_s": round(st_median(times), 1) if times else None,
            "fairness_jain": round(jain, 3) if jain else None}


def st_median(xs):
    import statistics
    return statistics.median(xs)


def herd_cli(h, mock, run, n, rpm, retry_after, timeout):
    tag = f"{h.name}-herd-{rpm}{'' if retry_after else 'nora'}-{run}"
    base = WORK / tag
    if base.exists():
        shutil.rmtree(base)
    wss = [workspace(base / f"ws{i:03d}") for i in range(n)]
    home = warm_home(h, base, mock, tag, wss[0])
    h.prepare(home, mock.url(tag), wss)
    ctl(mock, {"tag": tag, "kind": "quota", "rpm": rpm, "retry_after": retry_after})
    sl = f"batch-hbherd{os.getpid()}.slice"
    t0 = time.time()
    procs = [scoped(f"hb-herd-{h.name}-{i:03d}-{os.getpid()}", h.run(f"{PROMPTS['HBTASK']} [a{i:03d}]"), base / f"{i:03d}.stat", slice_=sl, cwd=ws, env=h.env(home),
                    stdin=subprocess.DEVNULL, stdout=open(base / f"{i:03d}.out", "w"), stderr=subprocess.STDOUT) for i, ws in enumerate(wss)]
    deadline = t0 + timeout
    for p in procs:
        try:
            p.wait(max(1, deadline - time.time()))
        except subprocess.TimeoutExpired:
            pass
    alive = sum(p.poll() is None for p in procs)
    subprocess.run(["systemctl", "--user", "kill", "-s", "KILL", sl], capture_output=True)
    subprocess.run(["systemctl", "--user", "stop", sl], capture_output=True)
    ctl(mock, {"tag": tag})
    fin_agents = {r.get("agent") for r in finals(mock.records(tag), "HBTASK")}
    done = [i for i, ws in enumerate(wss) if f"a{i:03d}" in fin_agents and tests_pass(ws)]
    gave_up = sum(1 for i, p in enumerate(procs) if p.poll() is not None and i not in done)
    return {"harness": h.name, "run": run, "rpm": rpm, "retry_after": retry_after, "wall_s": round(time.time() - t0, 1), "still_running_at_timeout": alive,
            "exited_without_success": gave_up, **herd_facts(mock.records(tag), n, t0, done)}


def cmd_herd(a):
    out = RESULTS / runid("herd")
    mock = Mock(out / "mock", a.port, a.ttft, a.tps, bodies=False, ttft_per_ktok=a.ttft_per_ktok)
    rows, m, done = [], meta(), False
    try:
        for run in range(a.runs):
            for ra in (True, False):
                for name in shuffled(a.harness, run):
                    r = PLUGINS[name].herd(mock, run, a.n, a.rpm, ra, a.timeout) if name in PLUGINS else herd_cli(CLI[name], mock, run, a.n, a.rpm, ra, a.timeout)
                    print(json.dumps(r), flush=True)
                    rows.append(r)
        done = True
    finally:
        mock.stop()
        save(out, "herd.json", {"meta": m, "complete": done, "host": host_info(), "n": a.n, "rpm": a.rpm, "rows": rows})
    print(out)


# ---------- scenario: idle fleet ----------

QUERY_REPLIES = [(re.compile(rb"\x1b\[6n"), b"\x1b[1;1R"), (re.compile(rb"\x1b\[c"), b"\x1b[?62;22c"), (re.compile(rb"\x1b\[>c"), b"\x1b[>1;10;0c"),
                 (re.compile(rb"\x1b\]10;\?(\x07|\x1b\\)"), b"\x1b]10;rgb:ffff/ffff/ffff\x1b\\"), (re.compile(rb"\x1b\]11;\?(\x07|\x1b\\)"), b"\x1b]11;rgb:0000/0000/0000\x1b\\"),
                 (re.compile(rb"\x1b\[\?u"), b"\x1b[?0u"), (re.compile(rb"\x1b\[14t"), b"\x1b[4;800;1200t"), (re.compile(rb"\x1b\[16t"), b"\x1b[6;20;10t"),
                 (re.compile(rb"\x1b\[18t"), b"\x1b[8;40;120t"), (re.compile(rb"\x1b\[\?2026\$p"), b"\x1b[?2026;2$y")]


class Ptys(threading.Thread):
    """Masters of N terminals: drains output (keeps a tail per terminal) and answers common terminal queries."""

    def __init__(self):
        super().__init__(daemon=True)
        self.sel = selectors.DefaultSelector()
        self.tail = {}
        self.lock = threading.Lock()
        self.bytes = {}

    def open(self, key, rows=40, cols=120):
        m, s = os.openpty()
        fcntl.ioctl(s, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
        os.set_blocking(m, False)
        with self.lock:
            self.sel.register(m, selectors.EVENT_READ, key)
            self.tail[key] = b""
            self.bytes[key] = 0
        return m, s

    def run(self):
        while True:
            with self.lock:
                ready = self.sel.select(0) if self.sel.get_map() else []
            if not ready:
                time.sleep(0.02)
                continue
            for k, _ in ready:
                try:
                    data = os.read(k.fd, 65536)
                except (BlockingIOError, InterruptedError):
                    continue
                except OSError:
                    data = b""
                if not data:
                    with self.lock:
                        self.sel.unregister(k.fd)
                    continue
                self.bytes[k.data] += len(data)
                self.tail[k.data] = (self.tail[k.data] + data)[-20000:]
                for rx, reply in QUERY_REPLIES:
                    for _ in rx.findall(data):
                        try:
                            os.write(k.fd, reply)
                        except OSError:
                            pass


def screen_text(b):
    t = re.sub(rb"\x1b\[[0-9;?<>=]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(\x07|\x1b\\)|\x1b[()][A-Z0-9]|\x1b[=>78]", b"", b)
    return t.decode("utf8", "replace")[-3000:]


def ctty():
    os.setsid()
    fcntl.ioctl(0, termios.TIOCSCTTY, 0)


def type_text(fd, text):
    os.write(fd, text.encode())
    time.sleep(0.3)
    os.write(fd, b"\r")


def slice_cg(slice_name):
    parts = slice_name[:-len(".slice")].split("-")
    path = CG_ROOT / f"user.slice/user-{os.getuid()}.slice/user@{os.getuid()}.service"
    for i in range(1, len(parts) + 1):
        path = path / ("-".join(parts[:i]) + ".slice")
    return path


def idle_series_row(s, t0):
    return {"t": round(s["t"] - t0, 1), "pss_mib": round(s["Pss"] / 1024, 1), "rss_mib": round(s["Rss"] / 1024, 1), "swap_pss_mib": round(s["SwapPss"] / 1024, 1),
            "cg_mib": round(s["cg_current"] / 2**20, 1), "cg_anon_mib": round(s["cg_anon"] / 2**20, 1), "cg_swap_mib": round(s["cg_swap"] / 2**20, 1),
            "cpu_s": round(s["cpu_usec"] / 1e6, 2), "nproc": s["nproc"],
            **{f"{k.lower()}_{r}_mib": round(s.get(f"{k}_{r}", 0) / 1024, 1) for r in ROLES for k in ("Pss", "Pss_Anon", "SwapPss")},
            **{f"n_{r}": s.get(f"n_{r}", 0) for r in ROLES}}


class Tmux:
    """Private tmux server (no D-Bus in its env, so tmux keeps every pane in the server's own scope = our slice)."""

    def __init__(self, base, slice_, mem_max):
        self.dir = base / "tmux"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.dir.chmod(0o700)
        self.conf = self.dir / "tmux.conf"
        self.conf.write_text("set -g default-size 120x40\nset -g exit-empty off\nset -g remain-on-exit off\n")
        self.env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": str(self.dir), "TERM": "xterm-256color", "TMUX_TMPDIR": str(self.dir), "LANG": "C.UTF-8"}
        scoped(f"hb-tmux-{base.name}-{os.getpid()}", ["tmux", "-L", "hb", "-f", str(self.conf), "new-session", "-d", "-s", "hb", "-n", "hold", "sleep infinity"],
               base / "tmux.stat", slice_=slice_, mem_max=mem_max, env=self.env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait(30)

    def tm(self, *args, check=True):
        return subprocess.run(["tmux", "-L", "hb", *args], env=self.env, capture_output=True, text=True, check=check, timeout=30)

    def window(self, name, cwd, env, argv):
        cmd = " ".join(shlex_quote(x) for x in ["/usr/bin/env", "-i", *[f"{k}={v}" for k, v in env.items()], *argv])
        for attempt in range(3):
            r = self.tm("new-window", "-d", "-t", "hb", "-n", name, "-c", str(cwd), cmd, check=False)
            if r.returncode == 0:
                return
            time.sleep(1)
        raise RuntimeError(f"tmux new-window {name}: rc={r.returncode} {r.stderr.strip()[:300]}")

    def tui(self, name, cwd, env, h, prompt):
        """Start h's TUI; harnesses without an initial-prompt flag get the prompt typed once the screen is drawn and stable."""
        if not getattr(h, "tui_prompt_by_keys", False):
            return self.window(name, cwd, env, h.tui(prompt))
        self.window(name, cwd, env, h.tui(prompt))

        def typer():
            last, stable, t0 = None, 0, time.time()
            while time.time() - t0 < 120:
                time.sleep(0.5)
                sc = self.screen(name)
                stable = stable + 1 if sc.strip() and sc == last else 0
                last = sc
                if stable >= 4:
                    break
            self.type(name, prompt)
        threading.Thread(target=typer, daemon=True).start()

    def type(self, name, text):
        """False when the window is gone (its harness died, e.g. OOM-killed)."""
        if self.tm("send-keys", "-t", f"hb:{name}", "-l", text, check=False).returncode:
            return False
        time.sleep(0.3)
        self.tm("send-keys", "-t", f"hb:{name}", "Enter", check=False)
        return True

    def screen(self, name):
        return self.tm("capture-pane", "-p", "-t", f"hb:{name}", check=False).stdout[-2500:]

    def alive(self):
        out = self.tm("list-panes", "-s", "-t", "hb", "-F", "#{window_name} #{pane_dead}", check=False).stdout.split("\n")
        return sum(1 for l in out if l and not l.startswith("hold") and l.endswith(" 0"))


def warm_tui(h, home, base, mock, tag, sl, mem_max):
    """Unmeasured TUI turn in its own tmux (first-run TUI work: packages, caches), then quit; tagged <tag>-warmtui."""
    wws = workspace(base / "warm-tui-ws")
    h.prepare(home, mock.url(tag + "-warmtui"), [wws])
    t = Tmux(base / "warmtui", sl.replace(".slice", "w.slice"), mem_max)
    try:
        t.tui("w", wws, h.env(home), h, PROMPTS["HBIDLE"])
        mock.wait(lambda r: r.get("reply") == "text" and r.get("scenario") == "HBIDLE", 120, tag + "-warmtui")
        time.sleep(10)
    finally:
        t.tm("kill-server", check=False)
        subprocess.run(["systemctl", "--user", "stop", sl.replace(".slice", "w.slice")], capture_output=True)


def shlex_quote(x):
    import shlex
    return shlex.quote(x)


BURST_SC = "HBWORK"


def burst_phase(a, name, tmux, masters, drv, mock, tag, sampler, t0, alive):
    """All N agents get the same 15-step work turn at once (12 x 16 KB reads, a 20 KB report written and re-read, streamed markdown answer):
    CPU-s, per-agent turn time, cgroup memory every 0.25 s and per-role PSS every 0.5 s while they work (peak) and after a 60 s settle."""
    n_before = len(mock.records(tag))
    cpu0 = sampler.sample()["cpu_usec"]
    text = lambda i: f"{BURST_SC}: read twelve files, write a report, then summarise. [a{i:03d}]"
    bs = Sampler(sampler.cg, period=0.5, keep_every=0.5)
    fast = Sampler(sampler.cg, period=0.25, keep_every=3600, cg_only=True)
    bs.start()
    fast.start()
    t_b = time.time()
    if drv:
        for i in range(a.n):
            drv.prompt(i, text(i))
    elif tmux:
        for i in range(a.n):
            tmux.tm("send-keys", "-t", f"hb:a{i:03d}", "-l", text(i), check=False)
        time.sleep(1)
        t_b = time.time()
        for i in range(a.n):
            tmux.tm("send-keys", "-t", f"hb:a{i:03d}", "Enter", check=False)
    else:
        for i, m in masters.items():
            type_text(m, text(int(i[1:]) if isinstance(i, str) else i))
    t_sent = time.time()
    # an agent is done when its final answer has finished streaming (t_end), not when the request arrived
    done = lambda: {r["agent"] for r in finals(mock.records(tag)[n_before:], BURST_SC) if r.get("agent") and r.get("t_end")}
    enter_resent = []
    while time.time() - t_b < a.ready_timeout and len(done()) < a.n and alive() >= a.n:
        if tmux and not enter_resent and time.time() - t_sent > 5:  # Enter taken as a newline (codex paste-burst): press it again for agents that sent nothing
            started = {r.get("agent") for r in mock.records(tag)[n_before:] if r.get("agent")}
            enter_resent = [i for i in range(a.n) if f"a{i:03d}" not in started] or [-1]
            for i in enter_resent:
                if i >= 0:
                    tmux.tm("send-keys", "-t", f"hb:a{i:03d}", "Enter", check=False)
        time.sleep(0.25)
    time.sleep(2)  # render tail of the last streamed answer
    t_done = time.time()
    recs = mock.records(tag)[n_before:]
    fin = {}
    for r in finals(recs, BURST_SC):
        if r.get("agent") and r.get("t_end"):
            fin.setdefault(r["agent"], r["t_end"])
    first = {}
    for r in recs:
        if r.get("api") in ("anthropic", "responses", "chat", "gemini") and r.get("agent"):
            first.setdefault(r["agent"], r["t"])
    times = sorted(fin[k] - t_b for k in fin)
    cpu1 = sampler.sample()["cpu_usec"]
    q = lambda p: round(times[min(len(times) - 1, int(p * len(times)))], 2) if times else None
    peak = dict(bs.max, t=t_done)
    fast.stop()
    peak_cg_anon = max(fast.max.get("cg_anon", 0), bs.max.get("cg_anon", 0))
    time.sleep(max(0, (max(fin.values()) if fin else t_done) + 60 - time.time()))
    after = dict(bs.last)
    bs.stop()
    return {"scenario": BURST_SC, "agents_done": len(fin), "peak": idle_series_row(peak, t0) if peak.get("t") and "Pss" in peak else None, "peak_cg_anon_mib_fast": round(peak_cg_anon / 2**20, 1), "after_60s": idle_series_row(after, t0) if after.get("t") else None,
            "series": [idle_series_row(x, t0) for x in bs.series if x["t"] <= t_done + 60], "send_spread_s": round(t_sent - t_b, 2), "turn_s_p50": q(0.5), "turn_s_p90": q(0.9), "turn_s_max": round(times[-1], 2) if times else None,
            "first_request_s_p50": round(sorted(first[k] - t_b for k in first)[len(first) // 2], 2) if first else None,
            "first_request_s_max": round(max(first[k] - t_b for k in first), 2) if first else None, "agents_started": len(first),
            "enter_resent": [i for i in enter_resent if i >= 0],
            "cpu_s": round((cpu1 - cpu0) / 1e6, 2), "cpu_s_per_agent": round((cpu1 - cpu0) / 1e6 / a.n, 3),
            "requests": sum(1 for r in recs if r.get("api") in ("anthropic", "responses", "chat", "gemini")), "after": idle_series_row(sampler.last, t0)}


def cmd_idle(a):
    if a.lock:
        WORK.mkdir(parents=True, exist_ok=True)
        lockf = open(WORK / "idle.lock", "w")
        fcntl.flock(lockf, fcntl.LOCK_EX)
    name = a.harness[0]
    sc = a.scenario
    out = RESULTS / runid(f"idle-{name}-{a.n}-{sc.lower()}-{a.terminal}")
    tag = f"{name}-idle"
    base = WORK / out.name
    wss = [workspace(base / f"ws{i:03d}") for i in range(a.n)]
    if sc in ("HBLONG", "HBMARATHON"):
        for ws in wss:
            for f in (ROOT / "fixtures/long").iterdir():
                shutil.copy2(f, ws / f.name)
    res = {"harness": name, "n": a.n, "hold_s": a.hold, "scenario": sc, "terminal": a.terminal, "host_before": host_info()}
    sl = f"batch-hbidle{name}{os.getpid()}.slice"
    cg = slice_cg(sl)
    mock = Mock(out / "mock", a.port, a.ttft, a.tps, bodies=False, ttft_per_ktok=a.ttft_per_ktok)
    try:
        subprocess.run(["systemctl", "--user", "set-property", "--runtime", sl, f"MemoryMax={a.mem_max}", "MemorySwapMax=0"], check=True)
    except Exception:
        mock.stop()
        raise
    res["slice_limits"] = {"MemoryMax": a.mem_max, "MemorySwapMax": 0}
    res["meta"] = meta()
    ptys = tmux = None
    try:
        if cg_pids(cg):
            sys.exit(f"refuse: slice {sl} already has processes")
        if a.terminal == "tmux":
            tmux = Tmux(base, sl, a.mem_max)
        else:
            ptys = Ptys()
            ptys.start()
    except BaseException:
        mock.stop()
        raise
    procs, masters = [], {}
    t0 = time.time()
    sampler = net = drv = None

    def launch(key, cwd, env, argv, unit, prompt=None):
        res["launched"] = res.get("launched", 0) + 1
        if tmux and prompt is not None:
            tmux.tui(key, cwd, env, CLI[name], prompt)
        elif tmux:
            tmux.window(key, cwd, env, argv)
        else:
            m, s_ = ptys.open(key)
            masters[key] = m
            procs.append(scoped(unit, argv, base / f"{key}.stat", slice_=sl, mem_max=a.mem_max, cwd=cwd, env=env, stdin=s_, stdout=s_, stderr=s_, preexec_fn=ctty))
            os.close(s_)

    def alive():
        return tmux.alive() + sum(p.poll() is None for p in procs) if tmux else sum(p.poll() is None for p in procs)

    try:
        sampler = Sampler(cg, period=1.0, keep_every=10.0)
        net = NetCensus(cg, period=2.0)
        if name in PLUGINS:
            drv = PLUGINS[name].IdleDriver(a, base, mock, tag, wss, sl, res, procs, launch)
            sampler.start()
            net.start()
            drv.start(sc)
        else:
            h = CLI[name]
            home = warm_home(h, base, mock, tag, wss[0])
            warm_tui(h, home, base, mock, tag, sl, a.mem_max)
            h.prepare(home, mock.url(tag), wss)
            sampler.start()
            net.start()
            for i, ws in enumerate(wss):
                launch(f"a{i:03d}", ws, h.env(home), h.tui(f"{PROMPTS[sc]} [a{i:03d}]"), f"hb-idle-{name}-{i:03d}-{os.getpid()}", prompt=f"{PROMPTS[sc]} [a{i:03d}]")
                time.sleep(a.stagger)
        t_launched = time.time()
        while time.time() - t_launched < a.ready_timeout and len({r.get("agent") for r in finals(mock.records(tag), sc)}) < a.n and alive() >= a.n:
            time.sleep(1)
        fin = finals(mock.records(tag), sc)
        res["ready_agents"] = len({r.get("agent") for r in fin if r.get("agent")})
        res["final_replies"] = len(fin)
        res["t_all_ready_s"] = round(max(r["t"] for r in fin) - t0, 1) if fin else None
        res["launch_s"] = round(t_launched - t0, 1)
        if len(fin) < a.n:
            res["not_ready_screens"] = debug_screens(tmux, ptys, name, 3)
            if a.hold == 0:  # burst-only fleet that never got ready (OOM, crash): stop now, idle.json still written
                raise RuntimeError(f"fleet not ready: {res['ready_agents']}/{a.n}, alive {alive()}")
        res["after_work"] = idle_series_row(sampler.last, t0)
        time.sleep(a.settle)
        t_hold = time.time()
        res["at_hold_start"] = idle_series_row(sampler.last, t0)
        work_max = dict(sampler.max)
        net_hold0 = net.totals()
        n_alive0 = alive()
        marks = [m for m in (300, 900, 1800, 3600) if m <= a.hold]
        res["idle_checkpoints"] = {}
        while time.time() - t_hold < a.hold:
            time.sleep(5)
            while marks and time.time() - t_hold >= marks[0]:
                res["idle_checkpoints"][str(marks.pop(0))] = idle_series_row(sampler.last, t0)
            al = alive()
            if al < n_alive0:
                res.setdefault("deaths", []).append({"t": round(time.time() - t0), "alive": al})
                n_alive0 = al
                if al == 0:
                    break
        t_hold_end = time.time()
        res["at_hold_end"] = idle_series_row(sampler.last, t0)
        net_hold1 = net.totals()
        res["net_during_hold"] = {k: net_hold1[k] - net_hold0[k] for k in net_hold1}
        res["work_phase"] = {"cpu_s_until_hold": res["at_hold_start"]["cpu_s"], "max_pss_mib": round(work_max.get("Pss", 0) / 1024), "max_anon_mib": round(work_max.get("cg_anon", 0) / 2**20),
                             **{f"max_{k.lower()}_{r}_mib": round(work_max.get(f"{k}_{r}", 0) / 1024, 1) for r in ROLES for k in ("Pss", "Pss_Anon")},
                             "window": "launch to hold start",
                             "cg_memory_peak_mib": round(cg_stat(Path(cg)).get("memory.peak", 0) / 2**20, 1)}
        res["idle_cpu_s"] = round((sampler.last["cpu_usec"] - res["at_hold_start"]["cpu_s"] * 1e6) / 1e6, 2)
        held = [r for r in mock.records(tag) if t_hold <= r["t"] < t_hold_end and r.get("api") in ("anthropic", "responses", "chat", "gemini")]
        res["requests_during_hold"] = len(held)
        res["est_tokens_during_hold"] = sum(r.get("est_input", 0) for r in held)
        if drv:
            drv.end_hold()
        res["alive_end"] = alive()
        n_before = len(mock.records(tag))
        t_wake = time.time()
        if a.hold == 0:  # burst-only fleet: no wake-up re-prompt
            pass
        elif drv:
            for i in range(a.n):
                drv.prompt(i, f"HBIDLE again [a{i:03d}]")
        elif tmux:
            for i in range(a.n):
                tmux.type(f"a{i:03d}", f"HBIDLE again [a{i:03d}]")
        else:
            for i, m in masters.items():
                type_text(m, f"HBIDLE again [{i}]")
        while a.hold and time.time() - t_wake < a.ready_timeout and len({r.get("agent") for r in finals(mock.records(tag)[n_before:], "HBIDLE")}) < a.n and alive() >= a.n:
            time.sleep(0.5)
        fin2 = finals(mock.records(tag)[n_before:], "HBIDLE")
        res["reprompt_answered"] = len({r.get("agent") for r in fin2 if r.get("agent")})
        res["reprompt_all_s"] = round(max(r["t"] for r in fin2) - t_wake, 1) if fin2 else None
        firsts = [r for r in mock.records(tag)[n_before:] if r.get("api") in ("anthropic", "responses", "chat", "gemini")]
        res["reprompt_first_request_s"] = round(firsts[0]["t"] - t_wake, 2) if firsts else None
        time.sleep(5)
        res["after_reprompt"] = idle_series_row(sampler.last, t0)
        if a.burst:
            res["burst"] = burst_phase(a, name, tmux, masters, drv, mock, tag, sampler, t0, alive)
        res["max"] = dict(sampler.max)
        res["series"] = [idle_series_row(x, t0) for x in sampler.series]
        res["tokens"] = token_summary(mock.records(tag))
        if a.hold and len(fin2) < a.n:
            res["reprompt_screens"] = debug_screens(tmux, ptys, name, 2)
        wsd = tuple(f"/{d}/" for d in getattr(CLI.get(name), "ws_session_dirs", ()))
        res["disk_state_mib"] = round(sum(f.stat().st_size for f in base.rglob("*") if f.is_file() and not f.is_symlink() and ("/ws" not in str(f) or any(d in str(f) for d in wsd))) / 2**20, 1)
    except Exception as e:
        res["error"] = repr(e)[:500]
        raise
    finally:
        ev = {k: int(v) for k, v in (l.split() for l in (cg_read(Path(cg), "memory.events") or "").splitlines())}
        res["memory_events"] = ev
        if sampler:
            res.setdefault("max", dict(sampler.max))
            res.setdefault("series", [idle_series_row(x, t0) for x in sampler.series])
        if sampler:
            sampler.stop()
        if net:
            net.stop()
            res["net"] = net.summary()
        subprocess.run(["systemctl", "--user", "stop", sl], capture_output=True)
        subprocess.run(["systemctl", "--user", "reset-failed", f"hb-idle-{name}-*", "hb-tmux-*"], capture_output=True)
        mock.stop()
        res["host_after"] = host_info()
        save(out, "idle.json", res)
    print(json.dumps({k: v for k, v in res.items() if k not in ("series", "max", "not_ready_screens", "reprompt_screens", "host_before", "host_after")}, default=str))
    print(out)


def debug_screens(tmux, ptys, name, k):
    if tmux:
        return {f"a{i:03d}": tmux.screen(f"a{i:03d}" if name not in PLUGINS else PLUGINS[name].SCREEN) for i in range(k)}
    return {str(key): screen_text(v) for key, v in list(ptys.tail.items())[:k]}

# ---------- quick profile: every scenario, small, < 30 min ----------

def cmd_quick(a):
    names = [h for h in a.harness if h not in PLUGINS or a.include_plugins]
    me = [sys.executable, str(ROOT / "hb.py")]
    steps = [[*me, "task", "--runs", "3", "--harness", *names, "--port", str(a.port)],
             [*me, "crash", "--runs", "1", "--mode", "kill9", "term", "--harness", *names, "--port", str(a.port + 1)],
             [*me, "fault", "--runs", "1", "--profile", "429x3", "truncate", "reset-30s", "--harness", *names, "--port", str(a.port + 2)]]
    t0 = time.time()
    idle = [subprocess.Popen([*me, "idle", "--harness", h, "--n", "5", "--hold", "300", "--settle", "10", "--lock", "0", "--port", str(a.port + 10 + i)])
            for i, h in enumerate(names)]
    for cmd in steps:
        subprocess.run(cmd, check=False)
    for p in idle:
        p.wait()
    print(f"quick profile done in {(time.time() - t0) / 60:.1f} min; python3 report.py; python3 export.py")


# ---------- scenario: resume a real-size session in the interactive TUI ----------

def session_bytes(h, home, wss=()):
    tot = 0
    roots = [home / d for d in h.session_dirs] + [w / d for w in wss for d in getattr(h, "ws_session_dirs", ())]
    for r in roots:
        for f in r.rglob("*"):
            if f.is_file() and not f.is_symlink():
                tot += f.stat().st_size
    return tot


def cmd_resume(a):
    name = a.harness[0]
    h = CLI[name]
    out = RESULTS / runid(f"resume-{name}-{a.steps}")
    tag = f"{name}-resume"
    base = WORK / out.name
    wss = [workspace(base / f"ws{i:03d}") for i in range(a.n)]
    for ws in wss:
        for f in (ROOT / "fixtures/long").glob("*.txt"):
            shutil.copy(f, ws)
    mock = Mock(out / "mock", a.port, a.ttft, a.tps, bodies=False, ttft_per_ktok=a.ttft_per_ktok)
    res = {"harness": name, "n": a.n, "steps": a.steps, "hold_s": a.hold, "meta": meta(), "host_before": host_info(), "flags": FLAGS.get(name)}
    sl = f"batch-hbresume{name}{os.getpid()}.slice"
    cg = slice_cg(sl)
    sampler, tmux = None, None
    try:
        home = warm_home(h, base, mock, tag, wss[0])
        h.prepare(home, mock.url(tag + "-grow"), wss)
        t0 = time.time()
        grow = [scoped(f"hb-grow-{name}-{i:03d}-{os.getpid()}", h.run(f"HBGROW {a.steps}: read files step by step. [a{i:03d}]"), base / f"grow{i:03d}.stat", slice_="batch-hb.slice",
                       cwd=ws, env=h.env(home), stdin=subprocess.DEVNULL, stdout=open(base / f"grow{i:03d}.out", "w"), stderr=subprocess.STDOUT) for i, ws in enumerate(wss)]
        cap = a.n * (a.steps * 2 + 100)
        while any(p.poll() is None for p in grow) and time.time() - t0 < a.grow_timeout and len(mock.records(tag + "-grow")) < cap:
            time.sleep(2)
        for p in grow:
            if p.poll() is None:
                p.kill()
            p.wait()
        grecs = mock.records(tag + "-grow")
        res["grow"] = {"s": round(time.time() - t0, 1), "requests": len(grecs), "compactions": sum(1 for r in grecs if r.get("compaction")),
                       "finished_agents": len({r.get("agent") for r in finals(grecs, "HBGROW")}), "max_request_kb": round(max((r.get("bytes", 0) for r in grecs), default=0) / 1000)}
        res["grow"]["capped"] = len(grecs) >= cap
        res["session_mb"] = round(session_bytes(h, home, wss) / a.n / 1e6, 2)
        h.prepare(home, mock.url(tag), wss)
        subprocess.run(["systemctl", "--user", "set-property", "--runtime", sl, f"MemoryMax={a.mem_max}", "MemorySwapMax=0"], check=True)
        sampler = Sampler(cg)
        sampler.start()
        tmux = Tmux(base, sl, a.mem_max)
        t1 = time.time()
        for i, ws in enumerate(wss):
            tmux.window(f"a{i:03d}", ws, h.env(home), h.tui_resume(home, ws))
        time.sleep(a.render_wait)
        res["after_resume"] = idle_series_row(sampler.last, t1)
        res["resume_screen_tail"] = tmux.screen("a000")[-600:]
        n0 = len(mock.records(tag))
        for i in range(a.n):
            tmux.type(f"a{i:03d}", f"HBBURST: cat three files then report. [a{i:03d}]")
        t2 = time.time()
        while time.time() - t2 < 600 and len({r.get("agent") for r in finals(mock.records(tag)[n0:], "HBBURST")}) < a.n:
            time.sleep(1)
        res["turn_done_agents"] = len({r.get("agent") for r in finals(mock.records(tag)[n0:], "HBBURST")})
        firsts = {}
        for r in mock.records(tag)[n0:]:
            if r.get("api") in ("anthropic", "responses", "chat", "gemini") and r.get("scenario") == "HBBURST":
                firsts.setdefault(r.get("agent"), r.get("bytes", 0))
        res["resumed_first_request_kb"] = sorted(round(v / 1000) for v in firsts.values())
        res["turn_s"] = round(time.time() - t2, 1)
        time.sleep(5)
        res["after_turn"] = idle_series_row(sampler.last, t1)
        res["idle_checkpoints"] = {}
        t3 = time.time()
        for m in [x for x in (300, 900, 1800, 3600) if x <= a.hold]:
            time.sleep(max(0, t3 + m - time.time()))
            res["idle_checkpoints"][str(m)] = idle_series_row(sampler.last, t1)
        res["alive_end"] = tmux.alive()
        res["max"] = {k: v for k, v in sampler.max.items()}
        res["cg_memory_peak_mib"] = round(cg_stat(cg).get("memory.peak", 0) / 2**20, 1)
        res["complete"] = True
    except Exception as e:
        res["error"] = repr(e)[:500]
        raise
    finally:
        if sampler:
            sampler.stop()
        if tmux:
            tmux.tm("kill-server", check=False)
        subprocess.run(["systemctl", "--user", "stop", sl], capture_output=True)
        subprocess.run(["systemctl", "--user", "reset-failed", "hb-tmux-*", f"hb-grow-{name}-*"], capture_output=True)
        mock.stop()
        res["host_after"] = host_info()
        save(out, "resume.json", res)
        print(json.dumps({k: v for k, v in res.items() if k not in ("max", "host_before", "host_after", "meta", "resume_screen_tail")}, default=str))


def main():
    global ALL
    load_plugins()
    ALL = [*PLUGINS, *CLI]
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    rp = sub.add_parser("resume")
    rp.add_argument("--harness", nargs=1, required=True, choices=list(CLI))
    rp.add_argument("--n", type=int, default=3)
    rp.add_argument("--steps", type=int, default=60, help="16 KB tool outputs before the kill (5 ~ 0.1 MB, 60 ~ 1 MB, 300 ~ 5 MB)")
    rp.add_argument("--hold", type=int, default=900)
    rp.add_argument("--render-wait", type=int, default=30)
    rp.add_argument("--grow-timeout", type=int, default=3600)
    rp.add_argument("--mem-max", default="20G")
    rp.add_argument("--port", type=int, default=19300)
    rp.add_argument("--ttft", type=int, default=150)
    rp.add_argument("--tps", type=int, default=400)
    rp.add_argument("--ttft-per-ktok", type=float, default=0.0)
    q = sub.add_parser("quick")
    q.add_argument("--harness", nargs="+", default=ALL)
    q.add_argument("--include-plugins", action="store_true")
    q.add_argument("--port", type=int, default=19100)
    for n in ("task", "crash", "idle", "fault", "herd"):
        p = sub.add_parser(n)
        p.add_argument("--harness", nargs="+", default=ALL)
        p.add_argument("--port", type=int, default=18900)
        p.add_argument("--ttft", type=int, default=1700)
        p.add_argument("--tps", type=int, default=120)
        p.add_argument("--ttft-per-ktok", type=float, default=20.0, help="extra TTFT ms per 1k uncached input tokens")
        p.add_argument("--runs", type=int, default=3)
        if n == "herd":
            p.add_argument("--n", type=int, default=20)
            p.add_argument("--rpm", type=int, default=30)
            p.add_argument("--timeout", type=int, default=420)
        if n == "fault":
            p.add_argument("--profile", nargs="+", default=list(FAULTS))
        if n == "crash":
            p.add_argument("--mode", nargs="+", default=["kill9", "scope", "term", "stream", "sigterm"])
        if n == "idle":
            p.add_argument("--n", type=int, default=50)
            p.add_argument("--hold", type=int, default=3600)
            p.add_argument("--stagger", type=float, default=1.0)
            p.add_argument("--settle", type=int, default=30)
            p.add_argument("--ready-timeout", type=int, default=300)
            p.add_argument("--mem-max", default="20G")
            p.add_argument("--terminal", choices=["tmux", "pty"], default="tmux")
            p.add_argument("--lock", type=int, default=1, help="serialize idle runs on this host")
            p.add_argument("--burst", type=int, default=1, help="after the re-prompt, all agents get one 3-step work turn at once")
            p.add_argument("--scenario", choices=["HBIDLE", "HBLONG", "HBMARATHON"], default="HBLONG")
        for m in PLUGINS.values():
            m.add_args(p, n)
    a = ap.parse_args()
    if a.cmd == "quick":
        return cmd_quick(a)
    if a.cmd == "resume":
        guard(4)
        return cmd_resume(a)
    if a.cmd == "idle":
        guard(2 if a.harness[0] in PLUGINS else min(20, a.n * 0.3))
    else:
        guard(4)
    {"task": cmd_task, "crash": cmd_crash, "idle": cmd_idle, "fault": cmd_fault, "herd": cmd_herd}[a.cmd](a)


if __name__ == "__main__":
    main()
