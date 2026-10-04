#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -d lib ]; then
  mkdir -p lib
  cid=$(docker create --platform linux/amd64 apache/hadoop:3.4.1)
  for j in common/hadoop-common-3.4.1.jar mapreduce/hadoop-mapreduce-client-core-3.4.1.jar; do
    docker cp "$cid:/opt/hadoop/share/hadoop/$j" lib/
  done
  docker rm "$cid" >/dev/null
fi
rm -rf build && mkdir -p build
javac --release 8 -Xlint:-options -cp "lib/*" -d build src/tfidf/*.java
jar cf tfidf.jar -C build .
echo "built tfidf.jar"
