#!/usr/bin/env python3
"""linearlatex - render a PDF as a single-column, line-numbered text file.

The output is a fixed-column reading of the paper: for every page the left
column is emitted in full before the right column, so a passage is one
contiguous run of lines and can be quoted verbatim. `pdftotext -layout`
cannot be used for this: it merges the two columns onto shared line numbers,
so no passage spanning more than one physical line is quotable.

Column detection is geometric, not textual. `pdftohtml -xml` reports a box for
every text run; the tool projects those boxes onto the x axis, finds the
interior vertical gutters (runs of x that almost no run covers), and reads the
page column by column, left to right. Runs whose box bridges a gutter are
full-width material (a title, a figure caption) and are placed by vertical
position among the columns' lines.

Invariants the output guarantees:

  * every body line carries a right-aligned line number; blank separator lines
    and page markers carry no number,
  * section headings are emitted as their own numbered line,
  * a word hyphenated across a line break is rejoined,
  * page boundaries are marked and the page number is on the marker line,
  * same PDF in, same bytes out; the output's sha256 is printed on stderr.

Usage:

    linearlatex.py PAPER.pdf -o OUT.txt --title T --authors "A; B" --year 2012 \
        --publication P --url U --extract-date 2026-10-05 --abstract-file A.md \
        --tags "a,b,c"

Every metadata flag except --extract-date and --pdf-label is optional; the
YAML header is written from whatever is supplied plus values derived from the
PDF itself (page count, source sha256, the producing tool and its version).
"""

import argparse
import collections
import hashlib
import html
import os
import re
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

VERSION = "linearlatex 1.0.0"
DEFAULT_PDFTOHTML = "pdftohtml"

# A text run is followed by a space when the horizontal gap to the next run is
# larger than this fraction of the run's font size. Inter-word gaps in a
# justified line are ~0.25em; a run boundary inside a word (a font change, or
# a small-caps continuation such as "P" + "REPARE") is under ~0.1em.
SPACE_GAP_EM = 0.15

# A gutter must be at least this wide (in pdftohtml units) to separate columns,
# and must sit in the central part of the text block so that a figure's internal
# whitespace is not mistaken for a column boundary.
GUTTER_MIN_UNITS = 7
GUTTER_CENTRE_MIN = 0.30
GUTTER_CENTRE_MAX = 0.70

# Fraction of the 95th-percentile run coverage above which an x is "covered".
GUTTER_COVERAGE_FRACTION = 0.15

# Runs on the same visual line if their tops differ by less than this fraction
# of the median run height.
LINE_TOP_TOLERANCE = 0.5

# A vertical gap larger than this multiple of the median line pitch starts a
# new paragraph (a blank, unnumbered separator line is emitted).
PARAGRAPH_GAP = 1.7

DISCLAIMER = (
    "This is an AI extraction with line numbers, provided as-is with no "
    "warranty of correctness. OCR and extraction issues are possible; always "
    "read the original paper for the authoritative source."
)

BOLD_FAMILY = re.compile(r"medi|bold|semibold|demi|black", re.I)
MATH_FAMILY = re.compile(r"^cm(sy|mi|ex|bsy|msy|mm|sy|t10)", re.I)
MATH_GLYPHS = {"h": "⟨", "i": "⟩"}
SMALLCAP_SIZE_RATIO = 0.85


def die(message):
    sys.stderr.write("linearlatex: %s\n" % message)
    raise SystemExit(1)


def run_pdftohtml(pdftohtml, pdf, workdir):
    """Extract the PDF to pdftohtml XML and return the XML text."""
    out = os.path.join(workdir, "paper.xml")
    cmd = [pdftohtml, "-xml", "-q", "-hidden", "-nodrm", "-enc", "UTF-8", pdf, out]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
    except OSError as exc:
        die("cannot run %s: %s" % (pdftohtml, exc))
        raise
    if proc.returncode != 0:
        die("%s failed (%d): %s" % (pdftohtml, proc.returncode, proc.stderr.strip()))
    with open(out, encoding="utf-8") as handle:
        return handle.read()


def parse_fontspecs(root):
    specs = {}
    for spec in root.iter("fontspec"):
        specs[spec.get("id")] = {
            "size": float(spec.get("size") or 0),
            "family": (spec.get("family") or "").split("+")[-1],
        }
    return specs


def parse_runs(root, specs):
    """Return [{page, top, left, width, height, size, bold, math, text}, ...]."""
    runs = []
    for page in root.iter("page"):
        number = int(page.get("number"))
        for node in page.findall("text"):
            text = html.unescape("".join(node.itertext()))
            text = re.sub(r"\s+", " ", text).strip()
            if not text:
                continue
            font = specs.get(node.get("font") or "", {"size": 0.0, "family": ""})
            runs.append(
                {
                    "page": number,
                    "height_page": float(page.get("height") or 0),
                    "top": float(node.get("top")),
                    "left": float(node.get("left")),
                    "width": float(node.get("width")),
                    "height": float(node.get("height")),
                    "size": font["size"],
                    "bold": bool(BOLD_FAMILY.search(font["family"])),
                    "math": bool(MATH_FAMILY.match(font["family"])),
                    "text": text,
                }
            )
    return runs


def normalise(text: str, math: bool) -> str:
    """Fold typographic variants; map CM math angle brackets."""
    text = text.replace("­", "").replace(" ", " ")
    text = (
        text.replace("’", "'")
        .replace("‘", "'")
        .replace("“", '"')
        .replace("”", '"')
    )
    for ligature, plain in (
        ("ﬀ", "ff"),
        ("ﬁ", "fi"),
        ("ﬂ", "fl"),
        ("ﬃ", "ffi"),
        ("ﬄ", "ffl"),
    ):
        text = text.replace(ligature, plain)
    if math and len(text) == 1:
        text = MATH_GLYPHS.get(text, text)
    return text


def page_runs(runs, number):
    return [r for r in runs if r["page"] == number]


def detect_gutter(items):
    """Return (lo, hi, [gutters]) for one page's runs, or None if empty."""
    if not items:
        return None
    lo = min(r["left"] for r in items)
    hi = max(r["left"] + r["width"] for r in items)
    coverage = [0] * (int(hi - lo) + 2)
    for run in items:
        start = max(int(run["left"]), int(lo))
        end = min(int(run["left"] + run["width"]), int(hi) + 1)
        for x in range(start, end):
            coverage[x - int(lo)] += 1
    ordered = sorted(coverage)
    p95 = ordered[int(0.95 * (len(ordered) - 1))]
    threshold = max(1, int(GUTTER_COVERAGE_FRACTION * p95))
    span = hi - lo
    gutters = []
    run_start = None
    for index, value in enumerate(coverage):
        if value <= threshold:
            if run_start is None:
                run_start = index
        elif run_start is not None:
            gutters.append((run_start + lo, index - 1 + lo, index - run_start))
            run_start = None
    if run_start is not None:
        gutters.append((run_start + lo, hi, len(coverage) - run_start))
    central = [
        g
        for g in gutters
        if g[2] >= GUTTER_MIN_UNITS
        and GUTTER_CENTRE_MIN * span <= (g[0] + g[1]) / 2 <= GUTTER_CENTRE_MAX * span
    ]
    return lo, hi, central


def assign_columns(items, gutters):
    """Bucket runs into columns: one per gutter-interval, plus a full-width set.

    Returns a list of column lists ordered left to right. Full-width runs (those
    whose box bridges a gutter) are returned separately so the caller can place
    them by vertical position.
    """
    if not gutters:
        return [sorted(items, key=lambda r: (r["top"], r["left"]))], []
    lo = min(r["left"] for r in items)
    hi = max(r["left"] + r["width"] for r in items)
    bounds = [lo]
    for start, end, _ in gutters:
        bounds.append((start + end) / 2.0)
    bounds.append(hi + 1)
    columns = [[] for _ in range(len(bounds) - 1)]
    full = []
    for run in items:
        left, right = run["left"], run["left"] + run["width"]
        if any(left < g[0] and right > g[1] for g in gutters):
            full.append(run)
            continue
        centre = (left + right) / 2.0
        index = len(columns) - 1
        for i in range(len(bounds) - 1):
            if centre < bounds[i + 1]:
                index = i
                break
        columns[index].append(run)
    ordered = [sorted(c, key=lambda r: (r["top"], r["left"])) for c in columns if c]
    return ordered, sorted(full, key=lambda r: (r["top"], r["left"]))


def group_lines(column_runs, tolerance):
    """Group runs into visual lines: by top, then left.

    Returns [[run, ...], ...] with each line's runs ordered left to right.
    """
    lines = []
    tops = []
    for run in column_runs:
        if lines and run["top"] - tops[-1] <= tolerance:
            lines[-1].append(run)
        else:
            lines.append([run])
            tops.append(run["top"])
    for line in lines:
        line.sort(key=lambda r: r["left"])
    return lines


def ends_with_capital(parts):
    """True when the text accumulated so far ends in an upper-case letter."""
    if not parts:
        return False
    for part in reversed(parts):
        for character in reversed(part):
            return character.isalpha() and character.isupper()
    return False


def join_runs(line, body_size):
    """Join the runs of one visual line into text, restoring small caps.

    A TeX small-caps word is emitted by the extractor as several runs: the
    initial capital at full size, the remaining letters at a reduced size, with
    letter-spacing gaps that are indistinguishable from word spaces. Such runs
    are rejoined into the single upper-case token ("PREPARE" + "OK" ->
    "PREPAREOK"); a space is inserted everywhere else the gap exceeds 0.15em of
    the preceding run's size.
    """
    parts = []
    previous = None
    for run in line:
        text = normalise(run["text"], run["math"])
        if previous is not None:
            gap = run["left"] - (previous["left"] + previous["width"])
            space_gap = SPACE_GAP_EM * max(previous["size"], 1.0)
            smallcaps_pair = min(run["size"], previous["size"]) < body_size * SMALLCAP_SIZE_RATIO
            continuation = (
                smallcaps_pair and text.isupper() and ends_with_capital(parts)
            )
            if gap > space_gap and not continuation:
                parts.append(" ")
        parts.append(text)
        previous = run
    return "".join(parts).strip()


def median(values):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def starts_mid_smallcaps(line, body_size):
    """True when a line opens inside a TeX small-caps word.

    A small-caps word is emitted as a full-size initial capital followed by
    reduced-size capitals, so a line break inside one leaves either a reduced
    run at the start ("PARE" of PRE-PARE) or a lone capital followed by a
    reduced run ("E" then "POCH" of EPOCH).
    """
    if not line:
        return False
    first = line[0]
    if first["size"] < body_size * SMALLCAP_SIZE_RATIO and first["text"].isupper():
        return True
    if len(line) < 2:
        return False
    second = line[1]
    return (
        first["text"].isupper()
        and second["size"] < body_size * SMALLCAP_SIZE_RATIO
        and second["text"].isupper()
    )


def dehyphenate(records):
    """Join words split across a line break inside one column.

    A line ending in a hyphen is joined with the line below it when the
    continuation starts lower-case (ordinary prose) or begins inside a
    small-caps run (a message name such as PRE- / PARE). These are the only
    shapes a TeX line-break hyphen takes. The join follows the reading order, so
    it also crosses a column foot or a page foot: a hyphen at the end of a
    column is continued by the first line of the next column, which is where
    the rest of the word is. The merged text keeps the first line's position, so
    line numbers stay monotonic.
    """
    out = []
    for record in records:
        if "page" in record:
            out.append(record)
            continue
        text = record["text"]
        previous = out[-1] if out else None
        if (
            previous is not None
            and "page" not in previous
            and not record["heading"]
            and previous["text"].endswith("-")
            and (text[:1].islower() or record["smallcap_start"])
        ):
            previous["text"] = previous["text"][:-1] + text
            continue
        out.append(record)
    return out


def is_heading(line_runs, body_size):
    if not line_runs:
        return False
    if all(r["bold"] for r in line_runs):
        return True
    return min(r["size"] for r in line_runs) > body_size


def build_stream(runs):
    """Turn runs into the reading order: a list of line records, in order.

    Each record is a dict with the line's vertical position, whether it is a
    heading, its text, whether it starts inside a small-caps run, and whether a
    blank separator precedes it (a paragraph gap, a column change or a page
    change). De-hyphenation runs afterwards over the whole sequence, so a word
    split at a column or page boundary is rejoined too.
    """
    sizes = collections.Counter()
    heights = []
    for run in runs:
        sizes[round(run["size"], 1)] += 1
        if run["height"] > 0:
            heights.append(run["height"])
    body_size = sizes.most_common(1)[0][0] if sizes else 0.0
    tolerance = max(1.0, LINE_TOP_TOLERANCE * median(heights))
    pages = sorted({r["page"] for r in runs})

    def render_column(column_runs):
        rendered = []
        for line in group_lines(column_runs, tolerance):
            # A line can begin inside a small-caps word, either with a reduced-size run
            # ("PARE" of PRE-PARE) or with the full-size initial capital followed
            # by reduced letters ("E" + "POCH" of EPOCH).
            smallcap_start = starts_mid_smallcaps(line, body_size)
            rendered.append(
                {
                    "top": line[0]["top"],
                    "heading": is_heading(line, body_size),
                    "text": join_runs(line, body_size),
                    "smallcap_start": smallcap_start,
                    "break_before": False,
                    "column": None,
                }
            )
        return rendered

    stream = []
    for number in pages:
        items = page_runs(runs, number)
        detected = detect_gutter(items)
        if detected is None:
            continue
        _, _, gutters = detected
        columns, full = assign_columns(items, gutters)

        # Reading order is fixed-column: the left column in full, then the right
        # column. Full-width material (a title block, a caption spanning the
        # gutter) belongs to no column, so it is emitted ahead of them, which is
        # where a title block sits on an opening page.
        rendered_columns = [render_column(c) for c in columns]
        if full:
            rendered_columns.insert(0, render_column(full))
        rendered_columns = [c for c in rendered_columns if c]
        if not rendered_columns:
            continue

        samples = []
        for rendered in rendered_columns:
            samples += [b["top"] - a["top"] for a, b in zip(rendered, rendered[1:])]
        pitch = median(samples)

        stream.append({"page": number})
        for column_index, rendered in enumerate(rendered_columns):
            previous_top = None
            for record in rendered:
                record["column"] = (number, column_index)
                if previous_top is None:
                    record["break_before"] = True
                elif pitch and (
                    record["top"] - previous_top > pitch * PARAGRAPH_GAP
                    or record["heading"]
                ):
                    record["break_before"] = True
                previous_top = record["top"]
                stream.append(record)
    return dehyphenate(stream)


def strip_running_heads(runs, page_count):
    """Drop page furniture: centred folios and repeated running heads."""
    if not runs:
        return runs
    by_page = collections.defaultdict(list)
    for run in runs:
        by_page[run["page"]].append(run)

    head_texts = collections.Counter()
    for page_runs in by_page.values():
        page_height = max((r["height_page"] for r in page_runs), default=0.0)
        if page_height <= 0:
            continue
        for run in page_runs:
            if run["top"] <= 0.06 * page_height:
                head_texts[run["text"].strip()] += 1
    threshold = max(2, int(0.6 * page_count))
    running_heads = {t for t, c in head_texts.items() if c >= threshold}

    kept = []
    for page_runs_list in by_page.values():
        page_height = max((r["height_page"] for r in page_runs_list), default=0.0)
        footer = [r for r in page_runs_list if r["top"] >= 0.93 * page_height]
        folios = (
            len(footer) == 1 and footer[0]["text"].strip().isdigit() and page_height > 0
        )
        for run in page_runs_list:
            text = run["text"].strip()
            if folios and run["top"] >= 0.93 * page_height:
                continue
            if page_height > 0 and run["top"] <= 0.06 * page_height and text in running_heads:
                continue
            kept.append(run)
    return sorted(kept, key=lambda r: (r["page"], r["top"], r["left"]))


def render(stream, gutter_width):
    """Number every line and mark the page boundaries."""
    lines = []
    number = 0
    for record in stream:
        if "page" in record:
            lines.append("=== PAGE %d ===" % record["page"])
            continue
        if record["break_before"] and lines and lines[-1] != "":
            lines.append("")
        number += 1
        lines.append("%*d  %s" % (gutter_width, number, record["text"]))
    return lines


def yaml_scalar(value):
    return '"%s"' % value.replace("\\", "\\\\").replace('"', '\\"')


def yaml_block(key, value):
    body = " ".join(value.split())
    return "%s: >-\n  %s" % (key, body)


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 16), b""):
            digest.update(chunk)
    return digest.hexdigest()


def yaml_list(values):
    if not values:
        return " []"
    return "\n" + "\n".join('  - "%s"' % v.replace('"', '\\"') for v in values)


def build_header(args, pdf_sha, page_count, run_count):
    lines = ["---"]
    lines.append("title: %s" % yaml_scalar(args.title))
    lines.append("authors:%s" % yaml_list(args.authors))
    lines.append("year: %s" % yaml_scalar(str(args.year)))
    lines.append("publication: %s" % yaml_scalar(args.publication))
    lines.append("handle_url: %s" % yaml_scalar(args.url))
    lines.append("source_pdf: %s" % yaml_scalar(args.pdf_label))
    lines.append("source_pdf_sha256: %s" % yaml_scalar(pdf_sha))
    lines.append("page_count: %d" % page_count)
    lines.append("extracted_text_runs: %d" % run_count)
    lines.append("tool: %s" % yaml_scalar("research/papers/tools/linearlatex.py"))
    lines.append("tool_version: %s" % yaml_scalar(VERSION))
    lines.append("extractor: %s" % yaml_scalar("%s -xml -q -hidden -nodrm" % args.pdftohtml))
    lines.append("extraction_date: %s" % yaml_scalar(args.extract_date))
    lines.append("reading_order: %s" % yaml_scalar("fixed column: left column of each page in full, then right column"))
    lines.append("line_numbering: %s" % yaml_scalar("right-aligned, body lines only; blank separators and page markers unnumbered"))
    lines.append("hyphenation: %s" % yaml_scalar("words hyphenated across line breaks within a column are rejoined"))
    lines.append(yaml_block("disclaimer", DISCLAIMER))
    if args.abstract:
        lines.append(yaml_block("abstract", args.abstract))
    lines.append("tags:%s" % yaml_list(args.tags))
    lines.append("---")
    return lines


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="linearlatex.py",
        description="Render a PDF as a single-column, line-numbered text file.",
    )
    parser.add_argument("pdf")
    parser.add_argument("-o", "--output", help="output path (default: stdout)")
    parser.add_argument("--title", default="")
    parser.add_argument("--authors", action="append", default=[], metavar="NAME")
    parser.add_argument("--year", default="")
    parser.add_argument("--publication", default="")
    parser.add_argument("--url", default="")
    parser.add_argument("--pdf-label", default=None, help="repo-relative path recorded in the header")
    parser.add_argument("--abstract", default="", help="short abstract")
    parser.add_argument("--tags", action="append", default=[], metavar="TAG")
    parser.add_argument("--extract-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--gutter-width", type=int, default=5)
    parser.add_argument("--pdftohtml", default=DEFAULT_PDFTOHTML)
    args = parser.parse_args(argv)

    if not os.path.isfile(args.pdf):
        die("no such file: %s" % args.pdf)
    if args.pdf_label is None:
        args.pdf_label = os.path.basename(args.pdf)
    if not shutil.which(args.pdftohtml):
        die("%s not found on PATH (poppler-utils)" % args.pdftohtml)

    pdf_sha = sha256_file(args.pdf)
    out_dir = os.path.dirname(os.path.abspath(args.output)) if args.output else None
    workdir = tempfile.mkdtemp(prefix=".linearlatex-", dir=out_dir or ".")
    try:
        xml_text = run_pdftohtml(args.pdftohtml, args.pdf, workdir)
    finally:
        pass
    try:
        root = ET.fromstring(xml_text)
        specs = parse_fontspecs(root)
        runs = parse_runs(root, specs)
        pages = sorted({r["page"] for r in runs})
        if not pages:
            die("no text runs extracted; the PDF may be scanned images")
        runs = strip_running_heads(runs, len(pages))
        stream = build_stream(runs)
        body = render(stream, args.gutter_width)
        header = build_header(args, pdf_sha, len(pages), len(runs))
        text = "\n".join(header + [""] + body) + "\n"
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    data = text.encode("utf-8")
    if args.output:
        with open(args.output, "wb") as handle:
            handle.write(data)
        sys.stderr.write("wrote %s (%d lines, %d bytes)\n" % (args.output, len(body), len(data)))
    else:
        sys.stdout.write(text)
    sys.stderr.write("sha256 %s\n" % hashlib.sha256(data).hexdigest())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())