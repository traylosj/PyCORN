# Worked examples for the run structure proposal

One page per example result in [`samples/`](../../samples/), so the proposal in
[`RUN_STRUCTURE_RECORD.md`](../RUN_STRUCTURE_RECORD.md) and the scope decisions in the
[`ROADMAP`](../ROADMAP.md) ("Scope decisions for CADET") can be judged on data. Nothing is
decided or built by these pages. They show what today's readers return, laid out as the
proposal describes, so a wrong reading shows up as a wrong picture.

**Open [`index.html`](index.html) in a browser.** GitHub shows the HTML files as source, so
clone the repository or download the folder. The pages are plain HTML and SVG with no network
use. One small inline script makes every figure zoomable: Ctrl+wheel (or pinch, or the + and
minus buttons) to zoom, drag to pan, double-click to reset. Without the script the figures are
still complete, and hovering a bar or box shows its details.

## What each page holds

| Section | Shows | Proposal |
|---|---|---|
| Check these first | places in this run that test a decision, each linked to its doc | all |
| 0. Above and beside the operations | linked runs, procedure, unit procedure and cycles, chromatograms, equipment, material, who knows each | 3.1 to 3.4, 3.6 |
| 1. Levels | procedure, unit procedure, chromatogram, state, operation, phase, activities, section, display step on one clock | 3.5, 3.7 |
| 2. Set point next to actual | run log, flow fitted to the volume axis, measured curve (broken at pauses) | 3.8 |
| 3. Curves by role | UV, conductivity, pH, pressure, temperature | |
| 4. Tables | operations, display steps, states, sections, loads, fractions | 5 |
| 5. Flow path | every volume on the path, stored or not stored | 3.6.1.1 |
| 6. Every fact | 71 facts display or CADET could need: stored, derived, hint only, caller gives, not stored | 3.9 |
| 7. Scope rows | the ten roadmap rows as they come out on this run | ROADMAP |
| 8. The JSON | map of paths to page sections, key glossary, tree, full text | 5 |
| 9. Raw UNICORN output | run log, settings, mixer, column, calibration, method, curves, fit, `inspect --json`, in scrollable blocks | |

Each `<name>_record_prototype.json` is the prototype record of that run (section 8 of its page).
`sample.zip` and `u73_nonbinding_pulse.zip` are the same run. `u73_blank`, `u711_proa_day12`
and `u711_cadet_reference` share a method.

## Regenerating

```
python -I tools/build_run_structure_examples.py
```

The output is deterministic: no dates, no machine paths, run twice gives identical files.
Regenerate after any change to `run_log_sections`, `delivered_flow`, `event_tables`,
`run_details`, `method_hints`, the CADET `model_view` or the example results, and commit the
changed files with that change. The script resolves every link to the markdown against the
headings of the files in `docs/` and prints a warning for any that no longer resolves, so
renaming a heading in the proposal, the review or the roadmap shows up here.
`tests/test_run_structure_examples.py` builds the pages into a temporary folder and fails on
an error block, a missing section or a broken link.

Names and layout follow the proposal's drafts and change with it.
