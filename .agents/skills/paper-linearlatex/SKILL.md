---
name: paper-linearlatex
description: >-
  Extract a PDF paper into a single-column, line-numbered TXT so any passage can
  be quoted verbatim with a line number and section context. Use when a citation
  needs an exact quote with a locator, when `pdftotext -layout` interleaved the
  columns of a two-column paper, or when grepping a paper for a phrase must
  return usable line numbers.
status: alpha
---

# paper-linearlatex

`linearlatex.py` renders a PDF as a fixed-column, line-numbered text file: for
every page the left column is emitted in full before the right column, so a
passage is one contiguous run of lines. That is what `pdftotext -layout` cannot
give you on a two-column paper — it merges both columns onto shared line
numbers, so no passage spanning more than one physical line is quotable as one
run.

The tool is at `research/papers/tools/linearlatex.py`. It needs `pdftohtml` from
poppler on `PATH`, and nothing else: no pip packages, no network.

## Method

1. **Locate the PDF and hash it.** Record the path and the sha256 in the header.
   Never modify the PDF.
2. **Extract geometry, not text.** `pdftohtml -xml` reports a box for every
   text run with its page, top, left, width, height and font. The tool
   projects those boxes onto the x axis and finds the interior vertical gutters
   — runs of x that almost no run covers. That yields the column boundaries
   without assuming a fixed character offset.
3. **Read fixed-column.** Assign each run to a column by its box centre, sort
   each column top to bottom, and emit the left column before the right. A run
   whose box bridges a gutter is full-width material (a title block, a caption
   that spans the gutter) and is emitted ahead of the columns.
4. **Group runs into lines, then de-hyphenate.** Runs whose tops agree form one
   line. A space is inserted between runs when the gap exceeds 0.15em of the
   preceding run's size; a reduced-size upper-case run continues the previous
   capital instead, which restores TeX small-caps words (`PREPARE` + `OK` ->
   `PREPAREOK`). A line ending in a hyphen is joined with the next line of the
   reading order when the continuation is lower-case or starts inside a
   small-caps run.
5. **Number and mark.** Every body line gets a right-aligned number; blank
   paragraph separators and `=== PAGE n ===` markers get none. Section headings
   are their own numbered lines.
6. **Write the `.txt`** with a YAML header carrying title, authors, year,
   publication and handle URL, the source path and sha256, the page count, the
   tool and its version, the extraction date, a short abstract, tags, and the
   disclaimer below.
7. **Verify before quoting.** Run the tool twice and compare sha256. Grep a
   distinctive phrase and a section header. Quote a passage that lives in the
   right column and confirm it is contiguous with ascending line numbers.

## Disclaimer to place near the top of every `.txt`

> This is an AI extraction with line numbers, provided as-is with no warranty of
> correctness. OCR and extraction issues are possible; always read the original
> paper for the authoritative source.

## Invocation

```sh
python3 research/papers/tools/linearlatex.py \
  ext/uvrr-core/research/primary-sources/papers/PAPER.pdf \
  -o research/papers/PAPER.txt \
  --title "Paper Title" \
  --authors "First Author" --authors "Second Author" \
  --year 2012 \
  --publication "MIT-CSAIL-TR-2012-021" \
  --url "https://dspace.mit.edu/entities/publication/UUID" \
  --pdf-label "ext/uvrr-core/research/primary-sources/papers/PAPER.pdf" \
  --extract-date YYYY-MM-DD \
  --abstract "One short paragraph." \
  --tags "tag-one" --tags "tag-two"
```

The output's sha256 is printed on stderr, as is the line and byte count. The
run is deterministic: same PDF and same flags, same bytes.

## Column strategy per paper

- **Two columns (the common case).** Detected automatically from the gutter.
  Nothing to configure.
- **One column.** No interior gutter is found and the page is emitted as a
  single stream. Correct, and the same command works.
- **Three columns.** Each gutter becomes a boundary and the columns are emitted
  left to right. Check the output by eye; three-column proceedings with figures
  sometimes need a look.
- **Full-width floats.** A figure or table that spans the gutter is detected as
  full-width and emitted before the columns. If a paper puts full-width floats
  *mid-page*, check where they landed and say so in your report.

## What the `.txt` is for, and what it is not

The `.txt` is a locator and a quoting surface. It is **not** a replacement for
the markdown note or the analysis. A read-aloud tool would read every line
number aloud, so nothing that a human reads should be sourced from the `.txt`:
the number gutter is for grep and for citation, not for prose. Cite the `.txt`
for the exact wording and the line number, and cite the paper itself as the
authority.

## Known limits

- Small caps are restored to upper case, not to the paper's mixed-case small-caps
  convention (`PrepareOk` renders as `PREPAREOK`).
- A hyphen falling at the foot of a column or a page is left in place when
  figure or caption material sits between the two halves; merging across it
  would reorder the figure. Two such splits remain in the Liskov-Cowling
  extraction.
- Only text is extracted. Figures, plots and mathematical display are reduced to
  their glyphs; a formula is not reliably reconstructable from this output.
- A scanned paper with no text layer produces no runs and the tool exits with an
  error rather than writing an empty file.

## Worked example

`research/papers/liskov-cowling-vr-revisited-2012.txt` was produced from the
submodule's copy of Liskov & Cowling, *Viewstamped Replication Revisited*
(MIT-CSAIL-TR-2012-021). The PDF lives in the `ext/uvrr-core` submodule and is
not modified; the `.txt` and the tool live in the main repo under
`research/papers/` because the submodule is not this repository's to write to.