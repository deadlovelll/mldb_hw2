import json, re, sys, time, urllib.request
from pathlib import Path

N = int(sys.argv[1]) if len(sys.argv) > 1 else 100
OUT = Path(__file__).resolve().parent.parent / "data" / "corpus"
OUT.mkdir(parents=True, exist_ok=True)
START = re.compile(r"\*\*\* ?START OF (THE|THIS) PROJECT GUTENBERG EBOOK.*?\*\*\*", re.I | re.S)
END = re.compile(r"\*\*\* ?END OF (THE|THIS) PROJECT GUTENBERG EBOOK", re.I)


def get(url, tries=5):
    req = urllib.request.Request(url, headers={"User-Agent": "mldb-hw2 student project"})
    for i in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                return r.read()
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(2 * (i + 1))


def strip_boilerplate(text):
    m = START.search(text)
    if m:
        text = text[m.end():]
    m = END.search(text)
    if m:
        text = text[:m.start()]
    return text.strip()


meta, page = [], "https://gutendex.com/books/?languages=en&sort=popular"
while len(meta) < N and page:
    d = json.loads(get(page))
    seen = {b["id"] for b in meta}
    meta += [b for b in d["results"] if b["id"] not in seen
             and any(k.startswith("text/plain") for k in b["formats"])]
    page = d["next"]

saved = []
for b in meta[:N]:
    slug = re.sub(r"[^a-z0-9]+", "_", b["title"].lower()).strip("_")[:40]
    path = OUT / f"{b['id']}_{slug}.txt"
    if not path.exists():
        url = f"https://www.gutenberg.org/cache/epub/{b['id']}/pg{b['id']}.txt"
        try:
            path.write_text(strip_boilerplate(get(url).decode("utf-8", "replace")), encoding="utf-8")
        except Exception as e:
            print("skip", b["id"], e)
            continue
        time.sleep(0.5)
    saved.append({"id": b["id"], "title": b["title"],
                  "author": (b["authors"] or [{"name": "?"}])[0]["name"], "file": path.name,
                  "bytes": path.stat().st_size})
    print(f"{len(saved):3d} {path.name} {path.stat().st_size // 1024} KB")

(OUT.parent / "corpus_meta.json").write_text(json.dumps(saved, ensure_ascii=False, indent=1))
print("total", len(saved), "books,", sum(s["bytes"] for s in saved) // 2**20, "MB")
