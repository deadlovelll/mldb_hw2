#!/usr/bin/env bash
set -u
cd "$(dirname "$0")/.."
nn() { docker compose exec -T namenode "$@" 2>/dev/null; }
F=/user/hadoop/demo/war_and_peace.txt

echo "## 0. DataNode'ы кластера"
nn hdfs dfsadmin -report | grep -E "^Name:" | sed -E 's/^Name: ([0-9.]+):9866 \(mldb-hw2-(datanode[0-9])-1.*/   \2 = \1/'
echo
echo "## 1. Файл 3.2 МБ, блок 1 МБ, фактор репликации 2"
nn hdfs dfs -rm -r -f -skipTrash /user/hadoop/demo >/dev/null
nn hdfs dfs -mkdir -p /user/hadoop/demo
nn hdfs dfs -D dfs.blocksize=1048576 -D dfs.replication=2 -put /workspace/data/corpus/2600_war_and_peace.txt $F
nn hdfs fsck $F -files -blocks -locations | grep -E "blk_|Status|replicated|Total blocks"
echo "md5 до отказа:   $(nn hdfs dfs -cat $F | md5)"

echo; echo "## 2. Останавливаем datanode2 (имитация падения сервера)"
docker compose stop datanode2 >/dev/null 2>&1
t0=$(date +%s)
until nn hdfs dfsadmin -report | grep -q "Dead datanodes (1)"; do sleep 3; done
echo "NameNode пометил datanode2 мёртвым через $(( $(date +%s) - t0 )) с (нет heartbeat)"
echo "md5 после отказа: $(nn hdfs dfs -cat $F | md5)   <- файл читается с оставшихся реплик"

echo; echo "## 3. Ждём ре-репликацию недостающих копий на живые узлы"
until nn hdfs fsck $F | grep -qE "Under-replicated blocks:[[:space:]]+0 "; do sleep 3; done
echo "Ре-репликация завершена через $(( $(date +%s) - t0 )) с от отказа"
nn hdfs fsck $F -files -blocks -locations | grep -E "blk_|Status|Under-replicated"

echo; echo "## 4. Возвращаем datanode2"
docker compose start datanode2 >/dev/null 2>&1
until nn hdfs dfsadmin -report | grep -q "Live datanodes (3)"; do sleep 3; done
nn hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
