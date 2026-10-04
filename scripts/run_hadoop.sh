#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
SRC=$1; NAME=$2; R=${3:-4}; N=${4:-10}; COMBINE_MB=${5:-0}
mkdir -p results/hadoop_$NAME
docker compose exec -T namenode bash -c "
  set -e
  hdfs dfs -rm -r -f -skipTrash /user/hadoop/$NAME /user/hadoop/out_$NAME >/dev/null
  hdfs dfs -mkdir -p /user/hadoop/$NAME
  hdfs dfs -put /workspace/$SRC/* /user/hadoop/$NAME/
  hadoop jar /workspace/tfidf.jar tfidf.TfIdfDriver -D tfidf.reducers=$R -D tfidf.top.n=$N -D tfidf.combine.mb=$COMBINE_MB \
      /user/hadoop/$NAME /user/hadoop/out_$NAME
  rm -rf /workspace/results/hadoop_$NAME/3_tfidf /workspace/results/hadoop_$NAME/4_top_n
  hdfs dfs -get /user/hadoop/out_$NAME/3_tfidf /user/hadoop/out_$NAME/4_top_n /workspace/results/hadoop_$NAME/
" 2>&1 | tee results/hadoop_$NAME/job.log | grep -E "^(Documents|STAGE|TOTAL)|ERROR|Exception"
