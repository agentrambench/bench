# AgentRAMBench runner

Measures what coding-agent harnesses cost on the machine that runs them: private RAM and CPU per agent at work and at idle, fleets of 1 to 50 agents, crash and fault recovery, shared-quota behaviour. Results: https://agentrambench.com

No model is called. A local deterministic mock (`mock.mjs`) speaks the Anthropic Messages, OpenAI Responses and Gemini generateContent APIs, scripts every turn and counts every request. Each harness runs in its default configuration; the only changes are the endpoint, a dummy key and what its first-run dialogs need (listed per harness in `harnesses.py` `FLAGS`, and in every result file).

## Requirements

Linux with cgroup v2 and a systemd user session (`systemd-run --user --scope` must work; run as a normal user over SSH or a desktop login), Node.js 20+ (Ubuntu 22.04 ships 12: install from NodeSource), Python 3.10+, tmux, git, curl. Tested from a fresh clone on Ubuntu 22.04.5 (Vast VM, 19 vCPU, 49 GiB): setup 76 s, quick profile 29 min. Fleets of 50 agents need up to 20 GiB free RAM. Runs wait until load5 <= 12 (`HB_MAX_LOAD`) so timings stay comparable.

## Run

    ./setup.sh                                  # pinned harness versions into ./harnesses
    python3 hb.py quick && python3 export.py    # every scenario, small, ~30 min for the 7 harnesses
    python3 report.py                           # plain-text tables from results/raw

Full protocol for one harness (~4.5 h): `./run-full.sh claude`. Single scenarios: `python3 hb.py {task,crash,fault,herd,idle,resume} --help`.

Raw results land in `results/raw/<scenario>-<date>/` (JSON + the mock's request log); `export.py` builds `results/public/agentrambench.json` (schema `agentrambench/1`).

## Submit a harness or a result

Add an adapter class to `harnesses.py` (config, headless command, resume command, TUI command), install it pinned in `harnesses/package.json` or `setup.sh`, run `./run-full.sh <name>`, and open an issue with `results/public/agentrambench.json` plus a tarball of the `results/raw/` directories it was built from. The harness must accept a custom model endpoint and a dummy key.

## What is counted

Every process in the harness's cgroup: the harness, its terminal (one private tmux server) and the commands it runs, reported separately (`harness` / `terminal` / `tool`). Memory leads with private memory (cgroup anon, Pss_Anon); RSS and cgroup peak are kept but include shared pages and page cache. Swap is disabled in the measurement slices.
