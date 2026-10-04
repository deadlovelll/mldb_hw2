import sys
from pathlib import Path


def read_parts(d):
    for p in sorted(Path(d).glob("part-*")):
        yield from p.read_text().splitlines()


hadoop, local = Path(sys.argv[1]), Path(sys.argv[2])

h = {k: float(v) for k, v in (l.split("\t") for l in read_parts(hadoop / "3_tfidf"))}
l = {k: float(v) for k, v in (x.split("\t") for x in (local / "tfidf.tsv").read_text().splitlines())}
only_h, only_l = h.keys() - l.keys(), l.keys() - h.keys()
max_diff = max(abs(h[k] - l[k]) for k in h.keys() & l.keys())
print(f"TF-IDF pairs: hadoop={len(h)}, local={len(l)}, only_hadoop={len(only_h)}, only_local={len(only_l)}")
print(f"max |tfidf_hadoop - tfidf_local| = {max_diff:.3e}")

ht = [x.split("\t")[:3] for x in read_parts(hadoop / "4_top_n")]
lt = [x.split("\t")[:3] for x in (local / "top_n.tsv").read_text().splitlines()]
ht.sort(key=lambda r: (r[0], int(r[1])))
same = ht == lt
print(f"Top-N rows: hadoop={len(ht)}, local={len(lt)}, identical (doc, rank, term): {same}")
ok = not only_h and not only_l and max_diff < 1e-12 and same
print("RESULT:", "MATCH" if ok else "MISMATCH")
sys.exit(0 if ok else 1)
