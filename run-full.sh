#!/bin/sh
# Full protocol for one harness: ./run-full.sh HARNESS [PORT] [STEPS]
# STEPS (default all): task crash fault herd idle1 idle10 idle50 marathon resume; burst = fleets of 1, 10, 25, 50 with no idle hold (burstbig = 25, 50 only), all agents working at once. Each step waits for load5 <= 12 (HB_MAX_LOAD).
H=$1
P=${2:-19500}
STEPS=${3:-"task crash fault herd idle1 idle10 idle50 marathon resume"}
set -x
for s in $STEPS; do
  case $s in
    task) python3 hb.py task --runs 5 --harness $H --port $P ;;
    crash) python3 hb.py crash --runs 3 --harness $H --port $P ;;
    fault) python3 hb.py fault --runs 2 --harness $H --port $P ;;
    herd) python3 hb.py herd --runs 2 --harness $H --port $P ;;
    idle1) python3 hb.py idle --harness $H --n 1 --hold 300 --port $P ;;
    idle10) python3 hb.py idle --harness $H --n 10 --hold 900 --port $P ;;
    idle50)
      python3 hb.py idle --harness $H --n 50 --port $P --ready-timeout 1500
      d=$(ls -td results/raw/idle-$H-50-* | head -1)
      if python3 -c "import json,sys; x=json.load(open('$d/idle.json')); sys.exit(0 if (x.get('memory_events') or {}).get('oom_kill') or x.get('ready_agents') != 50 else 1)"; then
        python3 hb.py idle --harness $H --n 25 --port $P --ready-timeout 1500
      fi ;;
    burst)
      # common N=25 for every harness; a fleet that cannot start (OOM at 50) stops early and records the failure
      for n in 1 10 25 50; do python3 hb.py idle --harness $H --n $n --hold 0 --settle 60 --port $P --ready-timeout 1500; done ;;
    burstbig) for n in 25 50; do python3 hb.py idle --harness $H --n $n --hold 0 --settle 60 --port $P --ready-timeout 1500; done ;;
    marathon) python3 hb.py idle --harness $H --n 1 --scenario HBMARATHON --hold 300 --ready-timeout 3000 --port $P ;;
    resume) for n in 5 60 300; do python3 hb.py resume --harness $H --steps $n --hold 300 --port $P; done ;;
  esac
done
