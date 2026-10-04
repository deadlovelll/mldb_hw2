import re
import sys
from collections import Counter, defaultdict
from itertools import groupby
from pathlib import Path


def java_text_hash(s):
    h = 1
    for b in s.encode("utf-8"):
        b = b - 256 if b > 127 else b
        h = (31 * h + b) & 0xFFFFFFFF
    return h - (1 << 32) if h >= (1 << 31) else h


def partition(key, r):
    return (java_text_hash(key) & 0x7FFFFFFF) % r


def tokens(line):
    return [w for w in re.findall(r"[a-z]+", line.lower()) if len(w) >= 2]


src, R = Path(sys.argv[1]), int(sys.argv[2]) if len(sys.argv) > 2 else 2
reducer_inputs = defaultdict(list)

for i, f in enumerate(sorted(src.iterdir())):
    doc = f.name
    print(f"### Mapper {i} - split `{doc}`\n")
    raw = []
    offset = 0
    for line in f.read_text().splitlines(keepends=True):
        print(f"- вход: `({offset}, \"{line.rstrip()}\")`")
        raw += [(f"{t}@{doc}", 1) for t in tokens(line)]
        offset += len(line.encode())
    print(f"\nВыход map() ({len(raw)} пар): " + ", ".join(f"`({k}, {v})`" for k, v in raw))
    combined = Counter()
    for k, v in raw:
        combined[k] += v
    spill = sorted(((partition(k, R), k, v) for k, v in combined.items()))
    print(f"\nПосле sort + combiner в кольцевом буфере ({len(spill)} пар), с номером партиции:\n")
    for p, part in groupby(spill, key=lambda x: x[0]):
        part = list(part)
        print(f"- партиция {p}: " + ", ".join(f"`({k}, {v})`" for _, k, v in part))
        reducer_inputs[p] += [(k, v, i) for _, k, v in part]
    print()

print("### Reduce-сторона: fetch → merge-sort → группировка\n")
for p in sorted(reducer_inputs):
    rows = sorted(reducer_inputs[p])
    print(f"**Reducer {p}** скачал {len(rows)} пар у mapper'ов "
          f"{sorted({m for *_, m in rows})}; после слияния reduce() вызывается для групп:\n")
    print("| ключ | значения (после shuffle) | выход reduce() |\n|---|---|---|")
    for k, grp in groupby(rows, key=lambda x: x[0]):
        vals = [v for _, v, _ in grp]
        print(f"| `{k}` | `{vals}` | `({k}, {sum(vals)})` |")
    print()
