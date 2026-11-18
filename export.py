"""Public results export (schema agentrambench/1) → results/public/agentrambench.json. Only harnesses defined in harnesses.py are exported (plugins never).
One entry per (harness, metric, n_agents): runs, median, min, max, values, source run dir. Newest complete run per scenario."""
import argparse
import json
import os
import platform
import re
import statistics as st
import subprocess
import time
from pathlib import Path

from harnesses import CLI, FLAGS

ROOT = Path(__file__).parent
RAW = ROOT / "results/raw"
OUT = ROOT / "results/public/agentrambench.json"
NAMES = {"claude": "Claude Code", "codex": "Codex CLI", "opencode": "opencode", "pi": "Pi", "crush": "Crush", "goose": "Goose", "gemini": "Gemini CLI"}
# harness -> one line, while its numbers are partial or about to be replaced; drop the entry once complete
PROVISIONAL = {}
RAW_ARCHIVE = "https://github.com/agentrambench/bench/releases/download/edition-{e}/raw-{e}.tar.zst"
HNOTES = {"goose": "line-based interactive CLI, not full-screen; its developer tools run in-process (no separate MCP process observed in 1.59.1)",
          "gemini": "Gemini API format (streamGenerateContent); usage statistics left at default (on) and counted as non-model traffic; model router left at default",
          "crush": "measured as the Go binary (the npm package adds a Node wrapper process)"}
OFFLINE = {"cursor-cli": ("Cursor CLI", "requires a Cursor account; model traffic goes through Cursor's backend, a local endpoint is unreachable"),
           "amp": ("Amp", "model traffic goes only through Amp's backend (no local base-URL override)"),
           "warp": ("Warp", "agent runs against Warp's cloud backend (no local base-URL override)"),
           "kiro": ("Kiro / Amazon Q CLI", "model traffic goes only through the AWS-hosted backend (no local base-URL override)")}
NOT_MEASURABLE = {}

FAULT_LABELS = {"429x3": "429 ×3", "429-60s": "429 for 60 s", "529x3": "529 overloaded ×3", "500x3": "500 ×3", "401-60s": "401 invalid key for 60 s",
                "reset-30s": "connection reset for 30 s", "hang-30s": "server silent for 30 s", "truncate": "stream cut mid-answer",
                "stall-60s": "stream stalls 60 s", "trickle": "stream trickles 1 event / 5 s", "sse-error": "provider error event mid-stream",
                "malformed": "malformed stream event", "tool-hang": "command never returns", "tool-huge": "50 MB / 10 MB / binary command output",
                "loop-3min": "model never ends its turn (3 min)",
                "503-5min": "503 for 5 min", "503-15min": "503 for 15 min", "529-5min": "529 overloaded for 5 min", "429-5min-noRA": "429 for 5 min, no Retry-After",
                "429-RA3600": "429 with Retry-After 3600 s", "auth-5min": "401 for 5 min, then the same key works", "stall-5min": "no first byte for 5 min",
                "flap-3min": "connections cut 5 s of every 20 s for 3 min", "compound": "429, then reset, then stream cut", "html200": "200 OK with an HTML proxy page",
                "empty200": "200 OK with an empty body", "413": "413 request too large", "overflow": "context overflow (provider's real 400 error)",
                "truncate-tool": "stream cut inside a half-written tool call", "max-tokens-tool": "max_tokens inside a tool call", "bad-tool-call": "malformed tool-call JSON from the model",
                "unknown-tool": "model calls an unknown tool", "dup-events": "duplicated stream events", "reorder": "out-of-order stream events", "tool-flood": "command prints 500 MB",
                "quota-429": "terminal quota or spend limit returned as 429 (insufficient_quota / enforced_spend_limit_reached / per-day quota)",
                "refusal-200": "safety refusal returned as HTTP 200 (refusal / content_filter / SAFETY)", "keepalive-stall": "stream sends only keepalive pings for 5 min",
                "quiet-stream": "healthy stream silent for 90 s, then completes", "utf8": "invalid and split UTF-8 in tool output and stream",
                "pipe-hold": "background children keep the tool's stdout open", "index-lock": "stale .git/index.lock in the workspace repo",
                "enospc-tool": "disk full in the workspace", "enospc-session": "disk full under HOME (harness config, sessions, logs)"}
NOTES = {
    "task_anon_mib": "max over 4 Hz samples of cgroup memory.stat anon (private, not shared, no page cache), harness + its tool processes",
    "task_rss_mib": "sum of RSS over the harness processes; counts shared library pages in full",
    "task_cpu_s": "cgroup cpu.stat usage_usec for the whole run",
    "idle_anon_mib_per_agent": "cgroup anon at the end of the hold / N, for the (N, hold_s) of the row; median over runs of that cell; TUIs in one tmux (counted); each agent first read ~320 KB of tool output",
    "idle_cpu_s_per_agent_hour": "cgroup CPU consumed during the hold / N / hours",
    "idle_net_out_kib_per_agent_hour": "bytes sent on non-loopback TCP sockets during the hold, ss sampling every 2 s (lower bound)",
    "first_request_kib": "bytes of the first model request body",
    "task_uncached_ktok": "request bytes / 4 minus an idealized prefix cache (1 KiB granularity); estimate, not a tokenizer",
    "task_canary_leaks": "count of requests containing a planted secret string",
    "fleet_agents_per_gib": "1024 / idle private MiB per agent (terminal included); rank only results with comparable=true (same N and hold for every harness, see fleet_comparable)",
    "after_work_anon_mib_per_agent": "cgroup anon / N right after every agent's work turn (20 x 16 KB tool outputs + ~3 KB streamed markdown report)",
    "harness_anon_mib_per_agent": "sum of smaps_rollup Pss_Anon over harness processes / N at the end of the hold",
    "terminal_anon_mib_per_agent": "Pss_Anon of the private tmux server / N; a GUI terminal costs more (kitty measured at ~29 MiB per window on the same host)",
    "tool_children_anon_mib": "peak Pss_Anon of every process that is neither harness nor terminal (shells, python, cat...), whole fleet",
    "burst_turn_inflation": "mock latency is constant, so this is harness-side CPU and scheduling only, not model contention",
    "burst_work_turn_inflation": "mock latency is constant, so this is harness-side CPU and scheduling only, not model contention",
    "burst_peak_anon_mib_per_agent": "cgroup anon sampled every 0.25 s while all N agents run a 15-step turn (12 x 16 KB reads, a 20 KB report written and re-read, streamed markdown answer) until the last answer has finished streaming (+2 s), max / N; per-role Pss_Anon every 0.5-0.8 s (lower bound for short-lived tool processes)",
    "resumed_anon_mib_per_agent": "session grown headless to session_mb (16 KB tool outputs), harness killed, resumed with its own --continue in a tmux TUI; read 30 s after resume",
    "marathon_idle_anon_mib": "one agent; 300 x 16 KB tool outputs; the harness's own default compaction (count in compactions); read after 5 min idle",
    "fleet_max_agents_reached": "largest N where all agents finished their first turn and stayed alive through the hold",
}
REPRODUCE = {"command": "git clone https://github.com/agentrambench/bench && cd bench && ./setup.sh && python3 hb.py quick && python3 export.py", "minutes": 30, "repo": "https://github.com/agentrambench/bench"}
EDITION = "2026-12"
# first run timestamp of each edition; older pinned runs are carried (rows marked carried_from), except what the edition re-measures
EDITION_START = {"2026-11": "00000000-000000", "2026-12": "20261117-134000"}
RUN_TS = re.compile(r"(\d{8}-\d{6})")

METRICS = {
    "task_anon_mib": ("resources", "Private RAM, one agent doing the fixed task (peak)", "MiB", "lower", True),
    "task_rss_mib": ("resources", "RSS peak, fixed task (includes shared pages)", "MiB", "lower", False),
    "task_cpu_s": ("resources", "CPU time for the fixed task", "s", "lower", True),
    "task_first_request_s": ("resources", "Start-up: launch to first model request", "s", "lower", False),
    "idle_anon_mib_per_agent": ("resources", "Private RAM per agent at the end of the idle hold (incl. terminal)", "MiB", "lower", True),
    "idle_cpu_s_per_agent_hour": ("resources", "CPU per idle agent per hour", "s/h", "lower", True),
    "idle_net_out_kib_per_agent_hour": ("governance", "Non-model network upload per idle agent per hour (lower bound)", "KiB/h", "lower", False),
    "disk_state_mib": ("resources", "On-disk state + caches for the whole fleet", "MiB", "lower", False),
    "first_request_kib": ("tokens", "First model request size (system prompt + tools)", "KiB", "lower", True),
    "task_uncached_ktok": ("tokens", "Estimated uncached input tokens for the fixed task (bytes/4, ideal prefix cache)", "ktok", "lower", True),
    "task_requests": ("tokens", "Model requests for the fixed task (incl. auxiliary)", "", "lower", False),
    "task_net_out_kib": ("governance", "Non-model network upload during the fixed task (lower bound)", "KiB", "lower", False),
    "task_canary_leaks": ("governance", "Planted secrets (.env, ~/.aws, env var) found in model requests", "", "lower", False),
    "fleet_agents_per_gib": ("fleet", "Idle agents per GiB of RAM (private RAM)", "agents/GiB", "higher", False),
    "after_work_anon_mib_per_agent": ("resources", "Private RAM per agent right after its work turn (incl. terminal)", "MiB", "lower", False),
    "idle_300s_anon_mib_per_agent": ("resources", "Private RAM per agent 5 min after work (incl. terminal)", "MiB", "lower", False),
    "idle_3600s_anon_mib_per_agent": ("resources", "Private RAM per agent 60 min after work (incl. terminal)", "MiB", "lower", False),
    "harness_anon_mib_per_agent": ("resources", "Private RAM per idle agent, harness processes only (Pss_Anon)", "MiB", "lower", False),
    "terminal_anon_mib_per_agent": ("resources", "Private RAM per idle agent, terminal share (tmux, Pss_Anon)", "MiB", "lower", False),
    "tool_children_anon_mib": ("resources", "Peak private RAM of tool child processes (whole fleet)", "MiB", "lower", False),
    "work_peak_anon_mib_per_agent": ("resources", "Peak private RAM per agent from launch to the start of the idle hold, harness only (Pss_Anon)", "MiB", "lower", False),
    "burst_cpu_s_per_agent": ("resources", "CPU per agent when all N agents run a 3-step turn at once", "s", "lower", False),
    "burst_peak_anon_mib_per_agent": ("resources", "Peak private RAM per agent while all N agents work at once (incl. terminal and tools)", "MiB", "lower", True),
    "burst_work_cpu_s_per_agent": ("resources", "CPU per agent when all N agents run the same 15-step work turn at once (night runs only)", "s", "lower", False),
    "burst_work_turn_s_p50": ("fleet", "Median 15-step work-turn time when all N agents work at once (mock model, constant latency)", "s", "lower", False),
    "burst_work_turn_s_p90": ("fleet", "p90 15-step work-turn time when all N agents work at once", "s", "lower", False),
    "burst_work_turn_inflation": ("fleet", "15-step work-turn time inflation vs one agent alone (p50 at N / p50 at N=1)", "x", "lower", False),
    "burst_peak_terminal_anon_mib_per_agent": ("resources", "Peak private RAM per agent while all N agents work at once, terminal processes only (tmux, PTY)", "MiB", "lower", False),
    "burst_peak_tool_anon_mib_per_agent": ("resources", "Peak private RAM per agent while all N agents work at once, tool/command processes only", "MiB", "lower", False),
    "burst_peak_harness_anon_mib_per_agent": ("resources", "Peak private RAM per agent while all N agents work at once, harness only (Pss_Anon)", "MiB", "lower", False),
    "burst_after_anon_mib_per_agent": ("resources", "Private RAM per agent 60 s after all N agents finished the work turn", "MiB", "lower", False),
    "burst_turn_s_p50": ("fleet", "Median turn time when all N agents work at once (mock model, constant latency)", "s", "lower", False),
    "burst_turn_s_p90": ("fleet", "p90 turn time when all N agents work at once", "s", "lower", False),
    "burst_turn_inflation": ("fleet", "Turn-time inflation vs one agent alone (p50 at N / p50 at N=1)", "x", "lower", False),
    "resumed_anon_mib_per_agent": ("resources", "Private RAM per agent after resuming a real-size session in the TUI", "MiB", "lower", False),
    "resumed_after_turn_anon_mib_per_agent": ("resources", "Private RAM per agent after one turn in a resumed real-size session", "MiB", "lower", False),
    "resumed_idle_300s_anon_mib_per_agent": ("resources", "Private RAM per agent 5 min after a turn in a resumed real-size session", "MiB", "lower", False),
    "resumed_peak_anon_mib_per_agent": ("resources", "Peak private RAM per agent, resumed real-size session (resume + turn + idle)", "MiB", "lower", False),
    "marathon_idle_anon_mib": ("resources", "Private RAM, one agent idle after 300 tool turns (~1M tokens, default compaction)", "MiB", "lower", False),
    "marathon_peak_anon_mib": ("resources", "Peak private RAM during 300 tool turns (~1M tokens, default compaction)", "MiB", "lower", False),
    "burst_max_agents_reached": ("fleet", "Largest fleet (of 1, 10, 25, 50) where all agents started and finished the work turn at once", "agents", "higher", False),
    "fleet_max_agents_reached": ("fleet", "Largest fleet that started and survived the hold (tried up to 50)", "agents", "higher", False),
}


def pinned(x, h):
    """Only runs of the exact harness version setup.sh installs (drops e.g. pi measured on the incomplete system install)."""
    return ((x.get("meta") or {}).get("versions") or {}).get(h) == CLI[h].version


def newest(prefix, name, h, keep=lambda r: True):
    """Per harness: the complete run with the most rows for h that pass keep (quick-profile runs are smaller); newest on ties."""
    best = (None, None, 0)
    for d in sorted(RAW.glob(prefix + "*")):
        f = d / name
        if f.exists():
            x = json.loads(f.read_text())
            k = sum(1 for r in x.get("rows", []) if r.get("harness") == h and keep(r))
            if x.get("complete") and x.get("meta") and pinned(x, h) and k and k >= best[2]:
                best = (d, x, k)
    return best[0], best[1]


def entry(h, metric, vals, n, src):
    vals = [v for v in vals if isinstance(v, (int, float))]
    if not vals:
        return None
    return {"harness": h, "metric": metric, "n_agents": n, "runs": len(vals), "median": round(st.median(vals), 3), "min": round(min(vals), 3),
            "max": round(max(vals), 3), "values": [round(v, 3) for v in vals], "source": src}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--final", action="store_true", help="mark the edition final (publishable); default preliminary")
    ap.add_argument("--bench-commit", help="public runner commit to record (its runner files must equal this tree's)")
    ap.add_argument("--edition", default=EDITION, choices=list(EDITION_START))
    args = ap.parse_args()
    ed12 = args.edition >= "2026-12"
    res, sources, versions, fault_na = [], {}, {}, {}
    by_h = {}

    def src(scen, h, d):
        by_h.setdefault(scen, {})[h] = d.name

    for h in CLI:
        d, t = newest("task-", "task.json", h)
        if t:
            src("task", h, d)
            versions.update({k: v for k, v in t["meta"]["versions"].items() if k == h})
            rs = [r for r in t["rows"] if r["harness"] == h]
            add = lambda m, vs: res.append(entry(h, m, vs, 1, d.name))
            add("task_anon_mib", [r.get("max_anon_mib") for r in rs])
            add("task_rss_mib", [r.get("max_rss_mib") for r in rs])
            add("task_cpu_s", [r.get("cpu_s") for r in rs])
            add("task_first_request_s", [r.get("t_first_request_s") for r in rs])
            add("first_request_kib", [r["first_request_bytes"] / 1024 for r in rs])
            add("task_uncached_ktok", [r.get("est_uncached_tokens", 0) / 1000 for r in rs])
            add("task_requests", [r.get("requests") for r in rs])
            add("task_net_out_kib", [r.get("net", {}).get("bytes_sent", 0) / 1024 for r in rs])
            add("task_canary_leaks", [sum((r.get("canary_in_requests") or {}).values()) for r in rs if "canary_in_requests" in r])
        for modes in (("kill9", "scope", "term", "stream"), ("sigterm",)):
            d, c = newest("crash-", "crash.json", h, lambda r: r.get("mode") in modes)
            if not c:
                continue
            src("crash" if "kill9" in modes else "crash_sigterm", h, d)
            for mode in modes:
                rs = [r for r in c["rows"] if r["harness"] == h and r.get("mode") == mode and "error" not in r]
                if not rs:
                    continue
                METRICS.setdefault(f"crash_{mode}_recovered_alone", ("reliability", f"Crash ({mode}): turn finished without user action", "rate", "higher", mode == "kill9"))
                METRICS.setdefault(f"crash_{mode}_double_execution", ("reliability", f"Crash ({mode}): interrupted command left running and re-run", "rate", "lower", False))
                res.append(entry(h, f"crash_{mode}_recovered_alone", [0.0 if r.get("user_action_needed") else 1.0 for r in rs], 1, d.name))
                res.append(entry(h, f"crash_{mode}_double_execution", [1.0 if r.get("double_execution") else 0.0 for r in rs], 1, d.name))
                if mode == "sigterm":
                    METRICS.setdefault("crash_sigterm_clean_exit", ("reliability", "Crash (sigterm): harness exited within 10 s of SIGTERM", "rate", "higher", False))
                    res.append(entry(h, "crash_sigterm_clean_exit", [1.0 if r.get("exited_on_sigterm") else 0.0 for r in rs], 1, d.name))
                    METRICS.setdefault("crash_sigterm_turn_saved", ("reliability", "Crash (sigterm): the interrupted turn is in the resumed session (task prompt sent again on \"continue\")", "rate", "higher", False))
                    res.append(entry(h, "crash_sigterm_turn_saved", [1.0 if (not r.get("user_action_needed") or r.get("resume_prompt_recovered")) else 0.0 for r in rs], 1, d.name))
        # graded faults: every complete graded run of this edition counts (one run per process: run-faults.sh)
        gruns = []
        for gd in sorted(RAW.glob("fault-*")):
            ts = RUN_TS.findall(gd.name)
            if ed12 and (gd / "fault.json").exists() and ts and max(ts) >= EDITION_START[args.edition]:
                gx = json.loads((gd / "fault.json").read_text())
                if gx.get("meta") and gx.get("complete") and any(r.get("harness") == h and "grade" in r for r in gx.get("rows", [])):
                    gruns.append((gd, gx))
        if gruns:
            d = type("Runs", (), {"name": ",".join(gd.name for gd, _ in gruns)})()
            f = {"profiles": list(dict.fromkeys(p for _, gx in gruns for p in gx["profiles"])), "rows": [r for _, gx in gruns for r in gx["rows"]]}
            src("fault", h, d)
            for prof in f["profiles"]:
                rs = [r for r in f["rows"] if r["harness"] == h and r["fault"] == prof and "grade" in r]
                if not rs:
                    continue
                if any(r.get("fault_na") for r in rs):
                    fault_na.setdefault(h, []).append(prof)
                    continue
                lab = FAULT_LABELS.get(prof, prof)
                for suf, label, unit, better, vals in (
                        ("score", "graded score (1 alone, 0.6 / 0.3 after 1 / 2 continues, -0.2 duplicate side effect, -0.1 per 10 wasted requests)", "score", "higher", [r["grade"] for r in rs]),
                        ("done_alone", "finished without user action", "rate", "higher", [1.0 if r.get("success_without_user") else 0.0 for r in rs]),
                        ("recovery_s", "end of fault to the next completed task response, before any continue (window faults: window end); empty when the harness did not recover alone", "s", "lower", [r.get("recovery_s") for r in rs]),
                        ("wasted_requests", "failed requests beyond one per step of a 1, 2, 4 ... 60 s backoff over the fault (counted faults: beyond the count)", "requests", "lower", [r.get("wasted_requests") for r in rs]),
                        ("dup_side_effects", "steps executed twice (sidefx.log)", "count", "lower", [(r.get("sidefx") or {}).get("duplicates", 0) for r in rs]),
                        *((("stopped_fast", "stopped within the allowed requests and 60 s", "rate", "higher", [1.0 if r.get("stopped_fast") else 0.0 for r in rs]),
                           ("exit_nonzero", "headless exit code non-zero", "rate", "higher", [1.0 if r.get("exit_nonzero") else 0.0 for r in rs])) if "stopped_fast" in rs[0] else ()),
                        *((("aborted_healthy", "killed or re-sent the healthy slow stream", "rate", "lower", [1.0 if r.get("aborted_healthy") else 0.0 for r in rs]),) if "aborted_healthy" in rs[0] else ())):
                    METRICS.setdefault(f"fault_{prof}_{suf}", ("reliability", f"Fault: {lab}: {label}", unit, better, suf == "score" and prof in ("429x3", "truncate")))
                    res.append(entry(h, f"fault_{prof}_{suf}", vals, 1, d.name))
            # headless honesty: a run that did not finish alone should exit non-zero (timeouts excluded: the harness never ended)
            hon = [0.0 if r["rc"] == 0 else 1.0 for r in f["rows"] if r["harness"] == h and "grade" in r and not r.get("success_without_user") and r.get("rc") not in ("timeout", None) and not r.get("fault_na")]
            METRICS.setdefault("headless_exit_honest", ("reliability", "Headless: share of unfinished fault runs that exit non-zero (exit 0 = reported success)", "rate", "higher", False))
            if hon:  # one value: the share (a median of 0/1 runs would hide it)
                res.append(dict(entry(h, "headless_exit_honest", [round(sum(hon) / len(hon), 3)], 1, d.name), unfinished_runs=len(hon)))
        d, f = newest("fault-", "fault.json", h, lambda r: "grade" not in r)
        if f and not ed12:
            src("fault", h, d)
            for prof in f["profiles"]:
                if prof == "loop-3min":
                    NOTES.setdefault(f"fault_{prof}_done_alone", "success = the harness ends the runaway turn by itself before the 3-min timeout (step cap or loop detection: Crush and Gemini CLI stop after a few identical tool calls); in_totals=false")
                METRICS.setdefault(f"fault_{prof}_done_alone", ("reliability", f"Fault: {FAULT_LABELS.get(prof, prof)}: finished without user action", "rate", "higher", prof in ("429x3", "truncate")))
                rs = [r for r in f["rows"] if r["harness"] == h and r["fault"] == prof]
                if prof == "loop-3min":
                    ok = [1.0 if r.get("rc") is not None and r.get("wall_s", 999) < f["profiles"][prof].get("timeout", 180) - 5 else 0.0 for r in rs]
                else:
                    ok = [1.0 if (r.get("success_without_user") if r.get("scenario", "HBTASK") == "HBTASK" else r.get("final_reached")) else 0.0 for r in rs]
                res.append(entry(h, f"fault_{prof}_done_alone", ok, 1, d.name))
        d, hd = newest("herd-", "herd.json", h)
        if hd:
            src("herd", h, d)
            for ra in (True, False):
                key = f"herd_tasks_done_{'retry_after' if ra else 'no_retry_after'}"
                METRICS.setdefault(key, ("reliability", f"Shared quota ({hd['rpm']} req/min, {hd['n']} agents{'' if ra else ', no retry-after'}): share of tasks finished", "rate", "higher", ra))
                NOTES.setdefault(key, "per run: tasks finished / agents; median over runs")
                rs = [r for r in hd["rows"] if r["harness"] == h and r["retry_after"] == ra]
                res.append(entry(h, key, [r["tasks_done"] / r["agents"] for r in rs], hd["n"], d.name))
    for scen, m in by_h.items():
        sources[scen] = max(set(m.values()), key=list(m.values()).count)
    for h in CLI:
        groups, ramp, bramp = {}, [], []
        for d in sorted(RAW.glob(f"idle-{h}-*")):
            p = d / "idle.json"
            if not p.exists():
                continue
            x = json.loads(p.read_text())
            if not pinned(x, h):
                continue
            if x.get("meta") and x.get("scenario") == "HBLONG" and x["n"] >= 10 and x.get("hold_s"):
                ok = x.get("ready_agents") == x["n"] and x.get("alive_end") is not None and not x.get("deaths")
                ev = x.get("memory_events", {})
                why = "ok" if ok else (f"OOM-killed at the {x.get('slice_limits', {}).get('MemoryMax', '?')} cap" if ev.get("oom_kill") else
                                       "agents died during hold" if x.get("deaths") else f"{x.get('ready_agents')}/{x['n']} agents ready")
                ramp.append((x["n"], ok, d.name, why))
            if x.get("meta") and x.get("scenario") == "HBLONG" and x.get("hold_s") == 0:  # burst-only fleets: did all N start and finish the work turn
                b = x.get("burst") or {}
                ok = b.get("scenario") == "HBWORK" and b.get("agents_done") == x["n"]
                ev = x.get("memory_events", {})
                why = "ok" if ok else (f"OOM-killed at the {x.get('slice_limits', {}).get('MemoryMax', '?')} cap" if ev.get("oom_kill") else
                                       f"{b.get('agents_done', x.get('ready_agents'))}/{x['n']} agents finished")
                bramp.append((x["n"], ok, d.name, why))
            if not x.get("meta") or "at_hold_end" not in x or x.get("ready_agents") != x["n"] or x.get("scenario") != "HBLONG":
                continue
            groups.setdefault((x["n"], x["hold_s"]), []).append((d.name, x))
        for (n, hold), xs in groups.items():
            src = ",".join(name for name, _ in xs)
            per = [x["at_hold_end"]["cg_anon_mib"] / n for _, x in xs]
            add = lambda m, vs: res.append(dict(entry(h, m, vs, n, src) or {}, hold_s=hold) if vs else None)
            aw = [x for _, x in xs if "after_work" in x]
            if hold:  # burst-only fleets (--hold 0) have no idle hold: no idle cells
                add("idle_anon_mib_per_agent", per)
                add("idle_cpu_s_per_agent_hour", [x["idle_cpu_s"] / n / (hold / 3600) for _, x in xs])
                add("idle_net_out_kib_per_agent_hour", [x.get("net_during_hold", {}).get("sent", 0) / 1024 / n / (hold / 3600) for _, x in xs])
                add("disk_state_mib", [x.get("disk_state_mib") for _, x in xs])
                add("fleet_agents_per_gib", [1024 / v for v in per if v > 0])
                add("harness_anon_mib_per_agent", [x["at_hold_end"]["pss_anon_harness_mib"] / n for x in aw])
                add("terminal_anon_mib_per_agent", [x["at_hold_end"]["pss_anon_terminal_mib"] / n for x in aw])
                add("after_work_anon_mib_per_agent", [x["after_work"]["cg_anon_mib"] / n for x in aw])
                add("tool_children_anon_mib", [x["work_phase"]["max_pss_anon_tool_mib"] for x in aw])

            def work_peak(x):
                if x["work_phase"].get("window"):
                    return x["work_phase"]["max_pss_anon_harness_mib"]
                hs = x["at_hold_start"]["t"]
                return max([r.get("pss_anon_harness_mib", 0) for r in x.get("series", []) if r["t"] <= hs] + [x["after_work"].get("pss_anon_harness_mib", 0)])
            if hold:
                add("work_peak_anon_mib_per_agent", [work_peak(x) / n for x in aw])
                for cp in ("300", "3600"):
                    add(f"idle_{cp}s_anon_mib_per_agent", [x["idle_checkpoints"][cp]["cg_anon_mib"] / n for x in aw if cp in x.get("idle_checkpoints", {})])
            # burst counts only if all N finished, started within 5 s of each other, and the fleet never hit its memory cap (reclaim distorts time/CPU)
            capped = lambda x: any((x.get("memory_events") or {}).get(k) for k in ("max", "oom_kill"))
            bs = [x["burst"] for x in aw if x.get("burst", {}).get("agents_done") == n and (x["burst"].get("first_request_s_max") or 0) <= 5 and not (x["burst"].get("scenario") == "HBWORK" and capped(x))]
            # 2026-11 burst = 3-step HBBURST turn (burst_cpu/turn_*); 2026-12 = 15-step HBWORK turn, own names so published meanings never change
            old = [b for b in bs if b.get("scenario", "HBBURST") == "HBBURST"]
            bs = [b for b in bs if b.get("scenario") == "HBWORK"]
            if not ed12:  # superseded by burst_work_* in 2026-12
                add("burst_cpu_s_per_agent", [b["cpu_s_per_agent"] for b in old])
                add("burst_turn_s_p50", [b["turn_s_p50"] for b in old])
                add("burst_turn_s_p90", [b["turn_s_p90"] for b in old])
            # CPU only from night runs (21:00-06:00): daytime background load inflated it 1.5-1.8x while RAM and turn time matched
            night = {id(x["burst"]) for name, x in xs if "burst" in x and RUN_TS.findall(name) and (int(RUN_TS.findall(name)[-1][9:11]) >= 21 or int(RUN_TS.findall(name)[-1][9:11]) < 6)}
            add("burst_work_cpu_s_per_agent", [b["cpu_s_per_agent"] for b in bs if id(b) in night])
            pk = [b for b in bs if b.get("peak")]
            add("burst_peak_anon_mib_per_agent", [b.get("peak_cg_anon_mib_fast", b["peak"]["cg_anon_mib"]) / n for b in pk])
            for r in ("harness", "terminal", "tool"):
                add(f"burst_peak_{r}_anon_mib_per_agent", [b["peak"][f"pss_anon_{r}_mib"] / n for b in pk])
            add("burst_after_anon_mib_per_agent", [b["after_60s"]["cg_anon_mib"] / n for b in pk if b.get("after_60s")])
            add("burst_work_turn_s_p50", [b["turn_s_p50"] for b in bs])
            add("burst_work_turn_s_p90", [b["turn_s_p90"] for b in bs])
        for pre in ("burst_", "burst_work_"):
            base1 = next((r["median"] for r in res if r and r["harness"] == h and r["metric"] == pre + "turn_s_p50" and r["n_agents"] == 1), None)
            if base1:
                for r in [r for r in res if r and r["harness"] == h and r["metric"] == pre + "turn_s_p50" and r["n_agents"] > 1]:
                    res.append(dict(entry(h, pre + "turn_inflation", [v / base1 for v in r["values"]], r["n_agents"], r["source"]), hold_s=r["hold_s"]))
        for d in sorted(RAW.glob(f"resume-{h}-*")):
            p = d / "resume.json"
            if not p.exists():
                continue
            x = json.loads(p.read_text())
            if not pinned(x, h):
                continue
            stopped_at_compaction = x["grow"].get("finished_agents") == 0 and x["grow"].get("compactions", 0) >= x["n"] and not x["grow"].get("capped")
            if not x.get("complete") or x.get("turn_done_agents") != x["n"] or (x["grow"].get("finished_agents") != x["n"] and not stopped_at_compaction):
                continue
            n = x["n"]
            for key, val in (("resumed_anon_mib_per_agent", x["after_resume"]["cg_anon_mib"] / n), ("resumed_after_turn_anon_mib_per_agent", x["after_turn"]["cg_anon_mib"] / n),
                             ("resumed_idle_300s_anon_mib_per_agent", x["idle_checkpoints"].get("300", {}).get("cg_anon_mib", 0) / n or None),
                             ("resumed_peak_anon_mib_per_agent", max([x["max"].get("cg_anon", 0) / 2**20] + [x[k]["cg_anon_mib"] for k in ("after_resume", "after_turn") if x.get(k)]) / n)):
                if val:
                    e = entry(h, key, [val], n, d.name)
                    e.update(session_mb=x["session_mb"], steps=x["steps"], compactions=x["grow"]["compactions"], **({"grow_note": "headless run exited after its own auto-compaction before reaching the step count; the compacted session was resumed"} if stopped_at_compaction else {}))
                    res.append(e)
        for d in sorted(RAW.glob(f"idle-{h}-1-hbmarathon-*")):
            p = d / "idle.json"
            if not p.exists():
                continue
            x = json.loads(p.read_text())
            if not pinned(x, h):
                continue
            if not x.get("meta") or "at_hold_end" not in x:
                continue
            recs = [json.loads(l) for l in open(d / "mock/requests.jsonl") if '"tag"' in l]
            recs = [r for r in recs if r.get("tag", "").endswith("-idle")]
            if not any(r.get("step") == "seq299" for r in recs):
                continue
            extra = {"turns": 300, "compaction_requests": sum(1 for r in recs if r.get("compaction") and r.get("bytes", 0) > 50000), "max_request_kb": round(max(r.get("bytes", 0) for r in recs) / 1000)}
            for key, val in (("marathon_idle_anon_mib", x["at_hold_end"]["cg_anon_mib"]), ("marathon_peak_anon_mib", x["work_phase"]["max_anon_mib"])):
                e = entry(h, key, [val], 1, d.name)
                e.update(extra)
                res.append(e)
        for key, rp in (("fleet_max_agents_reached", ramp), ("burst_max_agents_reached", bramp)):
            if not rp:
                continue
            best = max((n for n, ok, _, _ in rp if ok), default=0)
            fails = sorted((n, why) for n, ok, _, why in rp if not ok and n > best)
            e = entry(h, key, [best], 200, ",".join(name for _, _, name, _ in rp))
            if e and fails:
                e["stop_reason"] = f"N={fails[0][0]}: {fails[0][1]}"
            res.append(e)
    res = [r for r in res if r and r["harness"] in CLI]
    start = EDITION_START[args.edition]
    later = [v for e, v in EDITION_START.items() if e > args.edition]
    if later:  # re-exporting an older edition: ignore runs made for later ones
        res = [r for r in res if not any(t >= min(later) for t in RUN_TS.findall(r["source"]))]
    for r in res:
        ts = RUN_TS.findall(r["source"])
        if ts and max(ts) < start:
            r["carried_from"] = max(e for e, s0 in EDITION_START.items() if s0 <= max(ts))
    fleet = [r for r in res if r["metric"].startswith(("idle_", "fleet_agents_per_gib", "disk_state"))]
    cells = {}
    for r in fleet:
        cells.setdefault((r["n_agents"], r["hold_s"]), set()).add(r["harness"])
    with_fleet = {r["harness"] for r in fleet}
    common = [k for k, hs in cells.items() if hs >= with_fleet]
    comparable = max(common, key=lambda k: (k[1] >= 3600, k[0], k[1])) if common else None
    for r in fleet:
        r["comparable"] = (r["n_agents"], r["hold_s"]) == comparable
    git = lambda *a: subprocess.run(["git", *a], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    mem = int(next(l for l in open("/proc/meminfo") if l.startswith("MemTotal")).split()[1]) / 2**20
    cpu = next((l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")), "")
    doc = {
        "schema": "agentrambench/1",
        "status": "final" if args.final else "preliminary",
        "reproduce": REPRODUCE,
        "raw_archive": RAW_ARCHIVE.format(e=args.edition),
        "edition": args.edition,
        "frozen": time.strftime("%Y-%m-%d"),
        "generated": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "bench_commit": args.bench_commit or git("rev-parse", "--short", "HEAD"),
        **({"bench_private_commit": git("rev-parse", "--short", "HEAD")} if args.bench_commit else {}),
        "llm": "local deterministic mock (Anthropic Messages / OpenAI Responses / Gemini generateContent); no real model",
        "machine": {"cpu": cpu, "cores": os.cpu_count(), "ram_gib": round(mem, 1), "kernel": platform.release(), "label": "dev desktop"},
        "sources": sources,
        "sources_by_harness": by_h,
        "fleet_comparable": {"n_agents": comparable[0], "hold_s": comparable[1]} if comparable else None,
        "harnesses": [{"id": h, "name": NAMES[h], "version": CLI[h].version, "status": "measured", "non_default_flags": FLAGS[h], **({"note": HNOTES[h]} if h in HNOTES else {}), **({"provisional": PROVISIONAL[h]} if h in PROVISIONAL else {})} for h in CLI if any(r["harness"] == h for r in res)]
                     + [{"id": k, "name": k, "status": "not_measurable", "reason": v} for k, v in NOT_MEASURABLE.items()]
                     + [{"id": k, "name": v[0], "status": "not_runnable_offline", "reason": v[1]} for k, v in OFFLINE.items()],
        "metrics": [{"id": k, "axis": v[0], "label": v[1], "unit": v[2], "better": v[3], "headline": v[4], **({"note": NOTES[k]} if k in NOTES else {}), **({"in_totals": not any(k.startswith(f"fault_{p}_") for p in ("loop-3min", "429-RA3600"))} if k.startswith("fault_") else {})} for k, v in METRICS.items()],
        "fault_labels": FAULT_LABELS,
        **({"fault_na": fault_na} if ed12 else {}),
        "results": res,
    }
    assert all(r["harness"] in CLI for r in res) and all(h["id"] in CLI or h["status"] != "measured" for h in doc["harnesses"])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(doc, indent=1))
    print(OUT, len(res), "results")


if __name__ == "__main__":
    main()
