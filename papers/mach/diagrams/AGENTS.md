# paper/diagrams — working rules

Message-trace diagrams for the paper: sources in a small trace language,
rendered to deterministic SVG in the space-time grammar of David C.
Turner's message diagrams. `papers/` stays a pure version record; the
paper's `.tex` is not touched here (figure incorporation happens in a paper
update).

## Contents

- `msgtrace.py` — PEP-723 `uv` script, stdlib only. Renders a `.trace`
  file to a byte-deterministic SVG: vertical lifelines with time flowing
  down, numbered diagonal message vectors, crash / reincarnate markers,
  dashed era separators. Black on white, serif throughout.
- `reincarnation.trace` — the uVRR reincarnation two-era forced sequence
  (unit scale): C crashes and is fenced, reincarnates as D, era 1 crosses
  `DECREMENT(C)+JOIN(D)` with D joining at weight 0 (standby), era 2 runs
  `INCREMENT(D)+LEAVE(C)` with D promoted 0→1 and C leaving at weight 0.
  The quorum facts are stated per rung 23's three-node degenerate case: the
  forming quorum `{A,B}` coincides across both eras (mass 2 of total 2 in
  era 1, mass 2 of total 3 in era 2), leader B is in it throughout, and no
  casting-vote pivot is exercised on this path.
- `turner-fig3.trace` — reproduction of Turner Fig. 3 ("Message flow during
  a reconfiguration") from his rasters (`../turner/figures/fig-3.png`) as a
  standing fidelity check of the renderer against his grammar.
- `Makefile` — verbs: `make diagram NAME=<trace name>`, `make clean`.

## Trace language

One directive per line; `#` comments; blank lines ignored.

    title <text>                 caption drawn at the top (optional)
    actors NAME=DISPLAY ...      lifelines, declared once, left to right;
    actors NAME[(note)] ...      `=` sets the cap label outright, `(note)`
                                 renders as `NAME (note)`; bare NAME renders
                                 as itself
    numbers on|off               prefix message numbers (default: on)
    era <text>                   begins an era region; the label is rendered
                                 verbatim unless it does not already start
                                 with `era`, in which case `era ` is prefixed
    crash NAME@STEP              crash marker on NAME's lifeline at STEP
    reincarnate NAME@STEP        birth marker on NAME's lifeline at STEP
    STEP SRC->DST: label         solid numbered vector; label may be empty
    STEP SRC-->DST: label        dashed numbered vector; label may be empty

## Layout grammar (Turner's space-time charts)

Extracted by vision from `../turner/figures/fig-3..5.png` and their
`.description.md` files; confirmed against a Mistral vision pass.

- Each actor is a thin vertical lifeline with a short horizontal cap tick
  at the top and the actor label set above it (italic serif). Time flows
  down; there are no horizontal message arrows.
- A message is a straight diagonal vector from a point on the source
  lifeline down to a point on the destination lifeline one row below
  (down-right when the receiver sits to the right, down-left when to the
  left), with a small filled triangular head at the receiver. Consecutive
  messages chain tail-to-head because rows are the sorted step numbers and
  each vector drops exactly one row.
- A message's label (bold serif) is set beside the destination lifeline at
  the arrowhead, on the side away from the incoming diagonal; when
  `numbers on`, the bold step number prefixes the label. Unlabelled
  vectors carry no text.
- Era boundaries are dashed horizontal lines across all lifelines. Each
  era's label is set in the left margin just below its opening separator;
  the diagram's first era has no line above it, so its label is set just
  above the next separator — the `era e` / `era e + 1` pair bracketing one
  dashed line. No shading.
- Crash marker: a bold × on the lifeline with the dead segment dashed
  below it; reincarnate marker: a small open circle where the new lifeline
  begins. Both are minimal monochrome extensions — his figures show no
  crash/rejoin furniture. A marker sharing its point with a landing
  arrowhead sets its label opposite that arrowhead's label.
- Row slope, margins, and the serif face (Times standing in for his
  Computer Modern) are fixed constants; output is byte-identical for
  identical input.

## Output rules

- SVG is always written and is deterministic (same input → same bytes);
  SVGs are tracked.
- PNG is a regeneration cache: it is written only when absent and is never
  regenerated over an existing file. PNGs are gitignored, except
  `reincarnation.png`: the paper includes it via `\includegraphics`, so the
  raster is tracked for the paper to publish from a committed tree.
  Rasterization uses headless Chrome, falling back to ImageMagick.
- `make clean` removes both; `make diagram NAME=x` reproduces from clean.

## Vision check

Rendered output is verified by looking at it before it is tracked: actors,
numbered vectors in order, crash marker, era separators, and the
reincarnated node's weight rise must all be visibly correct. Record the
check here.

- **2026-09-08 (Turner fidelity)** — `turner-fig3.svg` compared by eye
  against `../turner/figures/fig-3.png` and `fig-3.description.md`.
  Semantics match: ℓ→a₁ prepare(b) with a₁'s reply, proposals
  `proposed_i(b)` … `proposed_{i+5}(b)` in era e each followed by an
  unlabelled a₂→ℓ return, the reconfiguration crossing into era e + 1 with
  ℓ→a₁ prepare(b') and proposals `proposed_{i+6}(b)` …
  `proposed_{i+13}(b')` under the new ballot. Furniture matches: italic
  serif cap labels a₁ / ℓ / a₂ over cap ticks; thin vertical lifelines;
  downward diagonals with filled heads and no horizontal arrows; bold
  event labels beside the outer lifelines; one dashed era separator with
  `era e` above and `era e + 1` below in the left margin; monochrome; no
  in-figure title (`numbers off` and empty labels reproduce his
  numberless, partly unlabelled vectors). Known deviations, accepted: our
  subscripts are literal text (`proposed_{i+1}(b)`) and the row slope is a
  fixed constant where his hand-tuned layout compresses dense regions.
- **2026-09-08 (CSR)** — `reincarnation.svg` viewed at full size.
  Confirmed: caps A, B (leader), C, D; vectors 1–11 in step order with
  bold numbers at the arrowheads; monochrome crash × on C with
  `crash (fenced)` set opposite arrow 2's head label and C's dead segment
  dashed below; open-circle reincarnate marker on D with D's lifeline
  starting there; era 1 dashed separator carrying the
  `DECREMENT(C)+JOIN(D)` weights/quorum label with arrows 6–8 beneath it,
  D joining at weight 0 via the dashed standby stream; era 2 separator
  carrying the `INCREMENT(D)+LEAVE(C)` label with arrows 9–11 beneath it,
  D promoted 0→1 (arrow 11 `D now weight 1`), C leaving at weight 0, and
  the same forming quorum `{A,B}` promised in both eras with no pivot.
  Two collisions in the first render (crash label over arrow 2's head
  label; era 2's label over arrow 9's head label) and a left-edge clip of
  the era 2 label were found and fixed before this check passed.
