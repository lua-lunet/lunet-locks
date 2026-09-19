#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""message-traces: space-time diagram renderer (Turner's notation).

Reads a small trace language (mermaid-sequence-like) and emits a
deterministic SVG in the space-time grammar of David C. Turner's message
diagrams: each actor is a vertical lifeline with time flowing DOWN, and
every message is a downward diagonal vector from one lifeline to another
(down-right when the receiver sits to the right, down-left when to the
left); there are no horizontal message arrows. Era boundaries are dashed
horizontal lines across all lifelines with the era labels set in the left
margin. Crash markers (x, dashed dead segment below) and reincarnate
markers (open circle, lifeline starts there) are minimal monochrome
extensions. The drawing is black on white in a serif face throughout.

Language (one directive per line, `#` comments, blank lines ignored):

    title <text>                 caption drawn at the top (optional)
    actors NAME[(label)] ...     lifelines, declared once, left to right
    numbers on|off               prefix message numbers (default: on)
    era <text>                   begins an era region over subsequent rows
    crash NAME@STEP              crash marker on NAME's lifeline at STEP
    reincarnate NAME@STEP        birth marker on NAME's lifeline at STEP
    STEP SRC->DST: label         solid diagonal vector; label may be empty
    STEP SRC-->DST: label        dashed diagonal vector; label may be empty

Rows are the sorted step numbers (time positions); every vector and marker
lands on its step's row and each vector drops exactly one row, so
consecutive messages chain tail-to-head as in the original figures. A
message's label (and, when `numbers on`, its step number) is set beside
the destination lifeline at the arrowhead, on the side away from the
incoming diagonal. Output is byte-identical for identical input.
"""

import argparse
import html
import re
import sys
from pathlib import Path

ARROW = re.compile(r"^(\d+)\s+([A-Za-z0-9_]+)(-{1,2}>)([A-Za-z0-9_]+)\s*:\s*(.*)$")
MARKER = re.compile(r"^(crash|reincarnate)\s+([A-Za-z0-9_]+)@(\d+)$")

FONT = "Times New Roman, Times, serif"
INK = "#111111"
DEAD = "#555555"
COL_W = 170
ROW_H = 85
LEFT = 265
RIGHT = 200
CAP_HALF = 22
CAP_Y = 72
ACTOR_BASE = 58
TITLE_Y = 30
ERA_WRAP = 28
LABEL_WRAP = 24
LINE_H = 15


def esc(s: str) -> str:
    return html.escape(s, quote=True)


def fmt(v: float) -> str:
    r = round(v, 2)
    if abs(r - int(r)) < 1e-9:
        return str(int(r))
    return f"{r:.2f}"


def wrap(text: str, width: int) -> list[str]:
    lines, cur = [], ""
    for word in text.split():
        if cur and len(cur) + 1 + len(word) > width:
            lines.append(cur)
            cur = word
        else:
            cur = f"{cur} {word}".strip()
    if cur:
        lines.append(cur)
    return lines


def parse(path: Path):
    events = []
    for lineno, raw in enumerate(path.read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("title "):
            events.append(("title", line[6:].strip()))
        elif line.startswith("actors "):
            names = []
            for tok in line[7:].split():
                if "=" in tok:
                    name, label = tok.split("=", 1)
                    names.append((name, label))
                elif "(" in tok:
                    name, label = tok.split("(", 1)
                    names.append((name, f"{name} ({label.rstrip(')')})"))
                else:
                    names.append((tok, tok))
            events.append(("actors", names))
        elif line == "numbers on":
            events.append(("numbers", True))
        elif line == "numbers off":
            events.append(("numbers", False))
        elif line.startswith("era "):
            events.append(("era", line[4:].strip()))
        elif m := MARKER.match(line):
            events.append((m.group(1), (m.group(2), int(m.group(3)))))
        elif m := ARROW.match(line):
            events.append(
                ("arrow", (int(m.group(1)), m.group(2), m.group(3) == "-->", m.group(4), m.group(5)))
            )
        else:
            sys.exit(f"{path}:{lineno}: cannot parse line: {raw}")
    return events


def build(events, srcname: str):
    if not events or events[0][0] != "actors":
        sys.exit(f"{srcname}: must begin with an `actors` directive")
    actors = {name: (i, label) for i, (name, label) in enumerate(events[0][1])}
    title = next((payload for kind, payload in events if kind == "title"), "")
    numbers = all(payload for kind, payload in events if kind == "numbers")

    steps = {p[0] for k, p in events if k == "arrow"} | {p[1] for k, p in events if k in ("crash", "reincarnate")}
    if not steps:
        sys.exit(f"{srcname}: no rows")
    row_of = {step: i for i, step in enumerate(sorted(steps))}

    regions: list[dict] = []  # {label, rows: [row indexes], labelled}
    unlabelled: dict = {"label": "", "rows": [], "labelled": False}
    regions.append(unlabelled)
    current = unlabelled
    arrows, crashes, births = [], [], []
    for kind, payload in events[1:]:
        if kind == "era":
            current = {"label": payload, "rows": [], "labelled": True}
            regions.append(current)
        elif kind in ("crash", "reincarnate"):
            name, step = payload
            if name not in actors:
                sys.exit(f"{srcname}: unknown actor {name!r}")
            if current is not None:
                current["rows"].append(row_of[step])
            (crashes if kind == "crash" else births).append((name, step))
        elif kind == "arrow":
            step, src, dashed, dst, label = payload
            for a in (src, dst):
                if a not in actors:
                    sys.exit(f"{srcname}: unknown actor {a!r}")
            if src == dst:
                sys.exit(f"{srcname}: self arrow at step {step} not supported")
            if current is not None:
                current["rows"].append(row_of[step])
            arrows.append((step, src, dashed, dst, label))
    return actors, title, numbers, row_of, regions, arrows, crashes, births


def marker_side(arrows, row_of, name: str, step: int, x: dict) -> bool:
    """True when the marker's label goes on the right of its lifeline."""
    r = row_of[step]
    for astep, src, _dashed, dst, _label in arrows:
        if dst == name and row_of[astep] == r - 1:
            return x[src] > x[dst]
    return True


def render(actors, title, numbers, row_of, regions, arrows, crashes, births, srcname: str) -> str:
    n = len(actors)
    rows = sorted(row_of.values())
    x = {name: LEFT + i * COL_W for name, (i, _) in actors.items()}
    first_x = min(x.values())
    last_x = max(x.values())

    # Era furniture. Each `era` directive opens a labelled region. A region
    # whose predecessor has rows gets a dashed separator at its top with its
    # label just below; the diagram's first labelled region has no line above
    # it and its label is set just above the next separator — reproducing the
    # `era e` / `era e + 1` pair bracketing one dashed line.
    era_first_row: dict[int, list[str]] = {}
    gap_before: dict[int, int] = {}
    prev_has_rows = False
    first_region_lines: list[str] | None = None
    for region in regions:
        label = region["label"]
        if region["labelled"] and label and not label.lower().startswith("era"):
            label = f"era {label}"
        lines = wrap(label, ERA_WRAP) if region["labelled"] and label else []
        if region["rows"] and region["labelled"]:
            if prev_has_rows:
                top = min(region["rows"])
                era_first_row[top] = lines
                gap_before[top] = 54 + LINE_H * len(lines)
            elif first_region_lines is None:
                first_region_lines = lines
        prev_has_rows = prev_has_rows or bool(region["rows"])

    y: dict[int, float] = {}
    cursor = float(CAP_Y + 46)
    for r in rows:
        if r in gap_before:
            cursor += gap_before[r]
        y[r] = cursor
        cursor += ROW_H
    ys = lambda step: y[row_of[step]]
    last_drop = y[rows[-1]] + ROW_H
    bottom = last_drop + 30
    width = LEFT + (n - 1) * COL_W + RIGHT

    land = {}  # row -> y of the point one row below
    for r in rows:
        i = rows.index(r)
        land[r] = y[rows[i + 1]] if i + 1 < len(rows) else last_drop

    crash_steps = {name: step for name, step in crashes}
    birth_steps = {name: step for name, step in births}

    out: list[str] = []
    out.append('<?xml version="1.0" encoding="UTF-8"?>')
    out.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{fmt(width)}" height="{fmt(bottom + 16)}" '
        f'viewBox="0 0 {fmt(width)} {fmt(bottom + 16)}" font-family="{FONT}">'
    )
    out.append(f'<rect width="{fmt(width)}" height="{fmt(bottom + 16)}" fill="#ffffff"/>')

    def text(xp, yp, s, size: float = 12.5, fill=INK, anchor="middle", weight="normal", style=None):
        attrs = f' font-style="{style}"' if style else ""
        out.append(
            f'<text x="{fmt(xp)}" y="{fmt(yp)}" font-size="{size}" fill="{fill}" '
            f'text-anchor="{anchor}" font-weight="{weight}"{attrs}>{esc(s)}</text>'
        )

    if title:
        text(24, TITLE_Y, title, size=17, anchor="start", weight="bold")

    # Era separators: dashed horizontal lines across all lifelines. Each
    # separator carries its region's label just below; the first labelled
    # region's label is set just above the first separator.
    for r, lines in sorted(era_first_row.items()):
        ysep = y[r] - 40 - LINE_H * len(lines)
        out.append(
            f'<line x1="{fmt(first_x - 8)}" y1="{fmt(ysep)}" x2="{fmt(last_x + 8)}" y2="{fmt(ysep)}" '
            f'stroke="{INK}" stroke-width="1.2" stroke-dasharray="9 6"/>'
        )
        ly = ysep + 16
        for line in lines:
            text(first_x - 14, ly, line, size=14, anchor="end")
            ly += LINE_H
        if first_region_lines:
            fy = ysep - 10 - LINE_H * (len(first_region_lines) - 1)
            for line in first_region_lines:
                text(first_x - 14, fy, line, size=14, anchor="end")
                fy += LINE_H
            first_region_lines = None

    # Lifelines: solid above a crash, dashed (dead) below it; born at a
    # reincarnate marker.
    for name, (i, _) in actors.items():
        xn = x[name]
        y0, y1 = float(CAP_Y), bottom
        if name in crash_steps:
            yc = ys(crash_steps[name])
            out.append(
                f'<line x1="{fmt(xn)}" y1="{fmt(y0)}" x2="{fmt(xn)}" y2="{fmt(yc)}" stroke="{INK}" stroke-width="1.2"/>'
            )
            out.append(
                f'<line x1="{fmt(xn)}" y1="{fmt(yc)}" x2="{fmt(xn)}" y2="{fmt(y1)}" stroke="{DEAD}" '
                f'stroke-width="1.2" stroke-dasharray="3 5"/>'
            )
        elif name in birth_steps:
            yb = ys(birth_steps[name])
            out.append(
                f'<line x1="{fmt(xn)}" y1="{fmt(yb)}" x2="{fmt(xn)}" y2="{fmt(y1)}" stroke="{INK}" stroke-width="1.2"/>'
            )
        else:
            out.append(
                f'<line x1="{fmt(xn)}" y1="{fmt(y0)}" x2="{fmt(xn)}" y2="{fmt(y1)}" stroke="{INK}" stroke-width="1.2"/>'
            )

    # Actor furniture: cap tick with the label set above it.
    for name, (i, display) in actors.items():
        xn = x[name]
        xn = x[name]
        out.append(
            f'<line x1="{fmt(xn - CAP_HALF)}" y1="{fmt(CAP_Y)}" x2="{fmt(xn + CAP_HALF)}" y2="{fmt(CAP_Y)}" '
            f'stroke="{INK}" stroke-width="1.4"/>'
        )
        text(xn, ACTOR_BASE, display, size=18, style="italic")

    # Diagonal message vectors: tail on the source lifeline at the step's
    # row, filled head on the destination lifeline one row below.
    for step, src, dashed, dst, label in arrows:
        x1, x2 = x[src], x[dst]
        ya, yb = ys(step), land[row_of[step]]
        dx, dy = x2 - x1, yb - ya
        dist = (dx * dx + dy * dy) ** 0.5
        ux, uy = dx / dist, dy / dist
        dash = ' stroke-dasharray="6 4"' if dashed else ""
        out.append(
            f'<line x1="{fmt(x1)}" y1="{fmt(ya)}" x2="{fmt(x2 - 8 * ux)}" y2="{fmt(yb - 8 * uy)}" '
            f'stroke="{INK}" stroke-width="1.3"{dash}/>'
        )
        bx, by = x2 - 9 * ux, yb - 9 * uy
        px, py = -uy * 4, ux * 4
        out.append(
            f'<polygon points="{fmt(x2)},{fmt(yb)} {fmt(bx + px)},{fmt(by + py)} {fmt(bx - px)},{fmt(by - py)}" '
            f'fill="{INK}"/>'
        )
        if label or numbers:
            body = f"{step} {label}" if numbers and label else (str(step) if numbers else label)
            lines = wrap(body, LABEL_WRAP) if " " in body else [body]
            anchor = "start" if x1 < x2 else "end"
            lx = x2 + 10 if anchor == "start" else x2 - 10
            ly = yb + 4 - LINE_H * (len(lines) - 1) / 2
            for line in lines:
                text(lx, ly, line, size=15, anchor=anchor, weight="bold")
                ly += LINE_H

    # Markers. A marker sharing its point with a landing arrowhead sets its
    # label on the side opposite that arrowhead's label.
    for name, step in crashes:
        xn, yn = x[name], ys(step)
        for s in (-1, 1):
            out.append(
                f'<line x1="{fmt(xn - 8 * s)}" y1="{fmt(yn - 8)}" x2="{fmt(xn + 8 * s)}" y2="{fmt(yn + 8)}" '
                f'stroke="{INK}" stroke-width="2.6" stroke-linecap="round"/>'
            )
        side = "start" if marker_side(arrows, row_of, name, step, x) else "end"
        mx = xn + 13 if side == "start" else xn - 13
        text(mx, yn + 4, "crash (fenced)", size=14, anchor=side, style="italic")
    for name, step in births:
        xn, yn = x[name], ys(step)
        out.append(
            f'<circle cx="{fmt(xn)}" cy="{fmt(yn)}" r="6" fill="#ffffff" stroke="{INK}" stroke-width="2"/>'
        )
        side = "start" if marker_side(arrows, row_of, name, step, x) else "end"
        mx = xn + 13 if side == "start" else xn - 13
        text(mx, yn + 4, "reincarnate", size=14, anchor=side, style="italic")

    out.append("</svg>")
    return "\n".join(out) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description="render a message trace to a deterministic space-time SVG")
    ap.add_argument("trace", type=Path)
    ap.add_argument("-o", "--out", type=Path, default=None)
    args = ap.parse_args()
    out = args.out or args.trace.with_suffix(".svg")
    events = parse(args.trace)
    actors, title, numbers, row_of, regions, arrows, crashes, births = build(events, str(args.trace))
    svg = render(actors, title, numbers, row_of, regions, arrows, crashes, births, str(args.trace))
    out.write_text(svg, encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
