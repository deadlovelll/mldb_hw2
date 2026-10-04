#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
SRC=$1; TAG=$2; RUNS=${3:-3}
OUT=results/bench_$TAG.tsv
echo -e "impl\trun\tseconds" > "$OUT"
timed() { python3 -c 'import subprocess, sys, time
t = time.perf_counter()
subprocess.run(sys.argv[1:], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
print(f"{time.perf_counter() - t:.2f}")' "$@"; }
for run in $(seq 1 "$RUNS"); do
  for w in 1 8; do
    s=$(timed python3 local/tfidf_local.py "$SRC" results/local_$TAG --workers $w)
    echo -e "python_w$w\t$run\t$s" | tee -a "$OUT"
  done
  s=$(timed docker compose exec -T namenode hadoop jar /workspace/tfidf.jar tfidf.TfIdfDriver \
        -D mapreduce.framework.name=local -D fs.defaultFS=file:/// -D tfidf.reducers=4 \
        /workspace/$SRC /tmp/local_out_$TAG)
  echo -e "hadoop_localjobrunner\t$run\t$s" | tee -a "$OUT"
done
