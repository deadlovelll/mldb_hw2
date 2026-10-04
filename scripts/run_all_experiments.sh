#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."
log() { echo "[$(date +%T)] $*"; }
log "local benchmarks x1"; scripts/bench_local.sh data/corpus    x1 3
log "local benchmarks x8"; scripts/bench_local.sh data/corpus_x8 x8 1
log "recreate cluster with tuned memory"
docker compose up -d --force-recreate >/dev/null 2>&1
until docker compose exec -T namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF \
   && [ "$(docker compose exec -T resourcemanager yarn node -list 2>/dev/null | grep -c RUNNING)" = 2 ]; do sleep 5; done
log "hadoop x1 combine";   scripts/run_hadoop.sh data/corpus    corpus_combine 4 10 64
log "hadoop x1 naive";     scripts/run_hadoop.sh data/corpus    corpus         4 10 0
log "hadoop x8 combine";   scripts/run_hadoop.sh data/corpus_x8 corpus_x8      4 10 64
log "compare x1";          python3 scripts/compare.py results/hadoop_corpus_combine results/local_x1
log "compare x1 naive";    python3 scripts/compare.py results/hadoop_corpus results/local_x1
log "compare x8";          python3 scripts/compare.py results/hadoop_corpus_x8 results/local_x8
log "done"
