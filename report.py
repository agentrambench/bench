"""Summarize results/raw → plain-text tables (stdout). Newest complete task/crash/fault run; idle runs grouped by harness, scenario and mode.
Memory leads with private (anon) RAM; RSS includes shared pages, cgroup peak includes page cache."""
import json
import statistics as st
from pathlib import Path

RAW = Path(__file__).parent / "results/raw"
ORDER = ["claude", "codex", "crush", "gemini", "goose", "opencode", "pi"]
rank = lambda h: ORDER.index(h) if h in ORDER else -1


def newest(prefix, name, min_rows=1):
    for d in sorted(RAW.glob(prefix + "*"), reverse=True):
        f = d / name
        if f.exists():
            data = json.loads(f.read_text())
            if data.get("complete") and data.get("meta") and len(data.get("rows", [])) >= min_rows:
                return d, data
    return None, None


def med(xs, digits=1):
    xs = [x for x in xs if isinstance(x, (int, float))]
    if not xs:
        return "-"
    m = st.median(xs)
    return f"{m:.{digits}f} ({min(xs):.{digits}f}-{max(xs):.{digits}f})" if len(xs) > 1 and max(xs) != min(xs) else f"{m:.{digits}f}"


def frac(rs, key):
    xs = [r.get(key) for r in rs if r.get(key) is not None]
    return f"{sum(bool(x) for x in xs)}/{len(xs)}" if xs else "-"


def table(head, rows):
    w = [max(len(str(r[i])) for r in [head, *rows]) for i in range(len(head))]
    return "\n".join("  ".join(str(c).ljust(w[i]) for i, c in enumerate(r)) for r in [head, *rows])


def task(out):
    d, t = newest("task-", "task.json", 10)
    if not t:
        return
    out.append(f"S1 fixed code task, mock LLM (TTFT {t['ttft_ms']} ms + {t.get('ttft_per_ktok_ms', 0)} ms/k uncached tokens, {t['tps']} tok/s); median (min-max); {d.name} @ {t['meta']['git_commit']}")
    rows = []
    for h in ORDER:
        rs = [r for r in t["rows"] if r["harness"] == h]
        if rs:
            rows.append([h, f"{sum(r['success'] for r in rs)}/{len(rs)}", med([r.get("max_anon_mib") for r in rs]), med([r["max_rss_mib"] for r in rs]),
                         med([r.get("cg_memory_peak_mib") for r in rs]), med([r.get("cpu_s") for r in rs], 2), med([r["t_first_request_s"] for r in rs], 2),
                         med([r.get("t_done_s") for r in rs]), med([r["requests"] for r in rs], 0), med([r["first_request_bytes"] / 1024 for r in rs]),
                         med([r.get("est_uncached_tokens", 0) / 1000 for r in rs]), med([r.get("aux_requests") for r in rs], 0),
                         med([st.median(r["harness_gaps_ms"]) if r.get("harness_gaps_ms") else None for r in rs], 0), med([r.get("leftover_procs") for r in rs], 0),
                         med([r.get("net", {}).get("bytes_sent", 0) / 1024 for r in rs])])
    out.append(table(["harness", "ok", "anon MiB", "RSS MiB", "cg peak MiB", "CPU s", "1st req s", "done s", "requests", "1st req KiB", "uncached ktok~",
                      "aux req", "gap ms", "leftover", "net out KiB"], rows))


def crash(out):
    d, c = newest("crash-", "crash.json", 10)
    if not c:
        return
    out.append(f"\nS3 crash during a 25 s tool call (stream: during the final answer); {d.name} @ {c['meta']['git_commit']}")
    rows = []
    for h in ORDER:
        for mode in ("kill9", "scope", "term", "stream"):
            rs = [r for r in c["rows"] if r["harness"] == h and r.get("mode") == mode and "error" not in r]
            if rs:
                rows.append([h, mode, f"{sum(not r.get('user_action_needed') for r in rs)}/{len(rs)}", frac(rs, "resume_prompt_recovered"),
                             frac(rs, "slow_step_reissued"), frac(rs, "tool_survived_2s"), frac(rs, "double_execution"),
                             med([r.get("kill_to_final_s") for r in rs]), med([r.get("partial_answer_chars_kept") for r in rs], 0)])
    out.append(table(["harness", "fault", "no user action", "prompt kept", "slow step redone", "tool survived", "double exec", "fault→final s", "partial kept"], rows))


def fault(out):
    d, f = newest("fault-", "fault.json", 10)
    if not f:
        return
    out.append(f"\nS4 provider/network faults injected after the first request; {d.name} @ {f['meta']['git_commit']}")
    rows = []
    for prof in f["profiles"]:
        for h in ORDER:
            rs = [r for r in f["rows"] if r["harness"] == h and r["fault"] == prof]
            if rs:
                rows.append([prof, h, frac(rs, "success_without_user"), med([r.get("injected") for r in rs], 0), med([r.get("requests_per_min_during_fault") for r in rs]),
                             med([r.get("recovered_request_after_last_fault_s") for r in rs]), med([r.get("wall_s") for r in rs]), frac(rs, "success_after_resume")])
    out.append(table(["fault", "harness", "done alone", "failed req", "req/min in fault", "recovery s", "wall s", "done after resume"], rows))


def idle(out):
    groups = {}
    for d in sorted(RAW.glob("idle-*")):
        f = d / "idle.json"
        if not f.exists():
            continue
        x = json.loads(f.read_text())
        if not x.get("meta") or "at_hold_end" not in x or x.get("ready_agents") != x["n"] or x["n"] < 10 or not x.get("hold_s"):
            continue
        mode = x.get("mode_label", "TUI")
        groups.setdefault((x["harness"], x["n"], x.get("scenario"), mode), []).append(x)
    rows = []
    for (h, n, sc, mode), xs in sorted(groups.items(), key=lambda kv: (kv[0][1], rank(kv[0][0]))):
        rows.append([h, mode, n, sc, len(xs), med([x["at_hold_end"]["cg_anon_mib"] / n for x in xs]), med([x["at_hold_end"]["pss_mib"] / n for x in xs]),
                     med([x["at_hold_end"]["cg_anon_mib"] for x in xs], 0), med([x["work_phase"]["max_anon_mib"] for x in xs if x.get("work_phase")], 0),
                     med([x["idle_cpu_s"] / n / (x["hold_s"] / 3600) for x in xs], 2), med([x.get("requests_during_hold") for x in xs], 0),
                     med([x.get("net_during_hold", {}).get("sent", 0) / 1024 for x in xs], 0), f"{min(x.get('reprompt_answered', 0) for x in xs)}/{n}",
                     med([x.get("reprompt_first_request_s") for x in xs], 2), med([x.get("disk_state_mib") for x in xs], 0)])
    if rows:
        out.append("\nS2 fleet: N interactive agents after one turn, held (1 h); tmux terminal counted; per agent = total/N (server-style plugins: one server + one attached client)")
        out.append(table(["harness", "mode", "N", "scenario", "runs", "anon MiB/agent", "PSS/agent", "anon total", "anon peak (work)", "CPU s/agent/h",
                          "model req in hold", "net out KiB in hold", "re-prompt ok", "1st re-req s", "disk MiB"], rows))


def main():
    out = []
    task(out)
    crash(out)
    fault(out)
    idle(out)
    print("\n".join(out))


if __name__ == "__main__":
    main()
