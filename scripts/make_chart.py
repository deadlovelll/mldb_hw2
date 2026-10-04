import math
from decimal import ROUND_HALF_UP, Decimal
import re
import statistics
from pathlib import Path

RESULTS = Path(__file__).resolve().parent.parent / "results"

COLORS = {"local": "#2a78d6", "cluster": "#eb6834"}
INK, INK_SECONDARY, INK_MUTED = "#0b0b0b", "#52514e", "#8a8984"
BACKGROUND, GRID = "#fcfcfb", "#e4e3df"

WIDTH = 920
ROW_HEIGHT = 30
BAR_HEIGHT = 18
LABEL_COLUMN = 300
RIGHT_MARGIN = 80
HEADER_HEIGHT = 70
SCALE_MIN, SCALE_MAX = 1, 2000
TICKS = (1, 10, 100, 1000)


def hadoop_total(name):
    log = RESULTS / f"hadoop_{name}" / "job.log"
    if not log.exists():
        return None
    match = re.search(r"TOTAL_SECONDS ([\d.]+)", log.read_text())
    return float(match.group(1)) if match else None


def bench_medians(tag):
    runs = {}
    for line in (RESULTS / f"bench_{tag}.tsv").read_text().splitlines()[1:]:
        impl, _, seconds = line.split("\t")
        runs.setdefault(impl, []).append(float(seconds))
    return {impl: statistics.median(values) for impl, values in runs.items()}


def load_rows():
    x1, x8 = bench_medians("x1"), bench_medians("x8")
    return [
        {"name": "Python, 1 процесс", "x1": x1["python_w1"], "x8": x8["python_w1"], "kind": "local", "digits": 2},
        {"name": "Python, 8 процессов", "x1": x1["python_w8"], "x8": x8["python_w8"], "kind": "local", "digits": 2},
        {"name": "Hadoop LocalJobRunner", "x1": x1["hadoop_localjobrunner"], "x8": x8["hadoop_localjobrunner"], "kind": "local", "digits": 2},
        {"name": "Hadoop YARN, map-задача на файл", "x1": hadoop_total("corpus"), "x8": None, "kind": "cluster", "digits": 1},
        {"name": "Hadoop YARN с CombineTextInputFormat", "x1": hadoop_total("corpus_combine"), "x8": hadoop_total("corpus_x8"), "kind": "cluster", "digits": 1},
    ]


def number(value, digits):
    rounded = Decimal(str(value)).quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP)
    return str(rounded).replace(".", ",")


def seconds(value, digits):
    return f"{number(value, digits)} с" if value else "-"


def growth(row):
    return f"в {number(row['x8'] / row['x1'], 1)} раза" if row["x1"] and row["x8"] else "-"


def markdown_table(rows):
    lines = [
        "| Реализация | ×1 (71,9 МБ) | ×8 (575 МБ) | Рост |",
        "|---|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(f"| {row['name']} | {seconds(row['x1'], row['digits'])} | {seconds(row['x8'], row['digits'])} | {growth(row)} |")
    return "\n".join(lines) + "\n"


def x_position(value):
    share = math.log10(value / SCALE_MIN) / math.log10(SCALE_MAX / SCALE_MIN)
    return LABEL_COLUMN + share * (WIDTH - LABEL_COLUMN - RIGHT_MARGIN)


def text(x, y, content, size, color, weight=None, anchor=None):
    extra = ""
    if weight:
        extra += f' font-weight="{weight}"'
    if anchor:
        extra += f' text-anchor="{anchor}"'
    return f'<text x="{x:.1f}" y="{y}" font-size="{size}" fill="{color}"{extra}>{content}</text>'


def legend_item(x, color, label):
    swatch = f'<rect x="{x}" y="46" width="12" height="12" rx="2" fill="{color}"/>'
    return swatch + text(x + 18, 57, label, 13, INK_SECONDARY)


def draw_panel(rows, column, title, top):
    rows = [row for row in rows if row[column]]
    plot_height = len(rows) * ROW_HEIGHT
    plot_top = top + 12
    parts = [text(20, top, title, 15, INK, weight=600)]

    for tick in TICKS:
        x = x_position(tick)
        parts.append(f'<line x1="{x:.1f}" y1="{plot_top}" x2="{x:.1f}" y2="{plot_top + plot_height}" stroke="{GRID}"/>')
        parts.append(text(x, plot_top + plot_height + 14, f"{tick} с", 11, INK_MUTED, anchor="middle"))

    for i, row in enumerate(rows):
        y = plot_top + i * ROW_HEIGHT
        baseline = y + 19
        parts.append(text(LABEL_COLUMN - 10, baseline, row["name"], 13, INK_SECONDARY, anchor="end"))
        value = row[column]
        bar_width = x_position(value) - LABEL_COLUMN
        parts.append(f'<rect x="{LABEL_COLUMN}" y="{y + 6}" width="{bar_width:.1f}" height="{BAR_HEIGHT}" rx="4" fill="{COLORS[row["kind"]]}"/>')
        parts.append(text(x_position(value) + 6, baseline, number(value, row["digits"]), 13, INK))

    next_top = plot_top + plot_height + 48
    return parts, next_top


def render_svg(rows):
    panels = [("x1", "Корпус ×1, 71,9 МБ"), ("x8", "Корпус ×8, 575 МБ")]
    panel_rows = sum(1 for column, _ in panels for row in rows if row[column])
    height = HEADER_HEIGHT + panel_rows * ROW_HEIGHT + len(panels) * 60 + 30

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {height}" font-family="Helvetica, Arial, sans-serif">',
        f'<rect width="{WIDTH}" height="{height}" fill="{BACKGROUND}"/>',
        text(20, 32, "Время полного расчёта TF-IDF и топ-10, секунды, логарифмическая шкала", 20, INK, weight=600),
        legend_item(20, COLORS["local"], "локально, одна машина"),
        legend_item(200, COLORS["cluster"], "Hadoop-кластер в Docker (YARN и HDFS)"),
    ]

    top = HEADER_HEIGHT + 20
    for column, title in panels:
        panel, top = draw_panel(rows, column, title, top)
        parts.extend(panel)

    parts.append("</svg>")
    return "\n".join(parts)


def main():
    rows = load_rows()
    table = markdown_table(rows)
    (RESULTS / "timings.md").write_text(table)
    (RESULTS / "timings.svg").write_text(render_svg(rows))
    print(table, end="")


if __name__ == "__main__":
    main()
