import argparse
import math
import os
import re
import time
import unicodedata
from collections import Counter
from multiprocessing import Pool
from pathlib import Path

WORD = re.compile(r"[a-z]+")
MARKS = re.compile("[\u0300-\u036f]")
MIN_LEN = 2


def count_terms(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        text = f.read().lower()
    if not text.isascii():
        text = MARKS.sub("", unicodedata.normalize("NFD", text))
    return path.name, Counter(w for w in WORD.findall(text) if len(w) >= MIN_LEN)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input")
    ap.add_argument("output")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--workers", type=int, default=1, help="процессов для подсчёта (1 = чисто последовательно)")
    args = ap.parse_args()

    files = sorted(p for p in Path(args.input).iterdir() if p.is_file())
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    timings = {}

    t = time.perf_counter()
    if args.workers > 1:
        with Pool(args.workers) as pool:
            counts = dict(pool.map(count_terms, files))
    else:
        counts = dict(map(count_terms, files))
    timings["1_term_counts"] = time.perf_counter() - t

    t = time.perf_counter()
    doc_len = {d: sum(c.values()) for d, c in counts.items()}
    df = Counter()
    for c in counts.values():
        df.update(c.keys())
    timings["2_tf_and_df"] = time.perf_counter() - t

    t = time.perf_counter()
    num_docs = len(files)
    idf = {term: math.log(num_docs / n) for term, n in df.items()}
    tfidf = {d: {term: (n / doc_len[d]) * idf[term] for term, n in c.items()} for d, c in counts.items()}
    timings["3_tfidf"] = time.perf_counter() - t

    t = time.perf_counter()
    top = {d: sorted(((s, term) for term, s in scores.items() if s > 0), key=lambda x: (-x[0], x[1]))[:args.top]
           for d, scores in tfidf.items()}
    timings["4_top_n"] = time.perf_counter() - t

    t = time.perf_counter()
    with open(out / "tfidf.tsv", "w") as f:
        for d, scores in tfidf.items():
            for term, s in scores.items():
                f.write(f"{term}@{d}\t{s!r}\n")
    with open(out / "top_n.tsv", "w") as f:
        for d in sorted(top):
            for rank, (s, term) in enumerate(top[d], 1):
                f.write(f"{d}\t{rank}\t{term}\t{s!r}\n")
    timings["write_output"] = time.perf_counter() - t

    total = sum(timings.values())
    print(f"Documents: {num_docs}, tokens: {sum(doc_len.values())}, unique terms: {len(df)}, "
          f"(term, doc) pairs: {sum(len(c) for c in counts.values())}, workers: {args.workers}")
    for k, v in timings.items():
        print(f"STAGE {k:<16} {v:7.2f}s")
    print(f"TOTAL_SECONDS {total:.2f}")


if __name__ == "__main__":
    main()
