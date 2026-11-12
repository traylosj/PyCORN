# CADET Export Guide

PyCORN turns what an ÄKTA run actually executed into input for
[CADET-Process](https://github.com/fau-advanced-separations/CADET-Process) 0.12 and
[CADET-Core](https://cadet.github.io/) 5 or 6. This guide is for users; the design, owner
decisions and open questions are in [`docs/ROADMAP.md`](ROADMAP.md).

> **Experimental.** The CADET export's assumptions are incomplete and not yet tested, and will
> change significantly as development continues. PyCORN writes this line to `warnings.csv` and
> to the command's output whenever CADET files are written
> (`pycorn.exporters.cadet.api.CADET_EXPERIMENTAL_NOTE`).

**Scope.** PyCORN builds what the run records: the process (sections, flows, inlet and valve
switches, pauses), the measured signals, the column geometry and the system volumes. It does
**not** cover CADET's models and parameters (column model options, binding models, transport,
discretization, solver). You set those in CADET's own names (see "Your model values"). Only one
working example is given here, and it is marked as an example. For everything else, see the
CADET documentation for your version.

## Exporting

From Python:

```python
import pycorn

run = pycorn.open_run("my_run.zip")       # or PyCORNDatabase().load_run(result_id)
out = pycorn.export_cadet(run, "cadet_out/", core_version="6",
                          bed_height=(2.5, "cm"), diameter=(1.6, "cm"))
out["written"]     # paths written
out["warnings"]    # read these: placeholders, guesses, what is missing
```

From the command line, the same files sit next to the other export files of each run:

```
pycorn export my_run.zip --out exports/ --cadet --cadet-core 6 --bed-height "2.5 cm" --diameter "1.6 cm"
```

Both go through the same code and write the same files. For the other `pycorn export` flags,
see [`docs/EXPORT_GUIDE.md`](EXPORT_GUIDE.md).

## Options

Quantities are `(value, unit)` in Python and `"value unit"` on the command line.

| Python (`export_cadet`) | CLI (`pycorn export`) | What it does |
|---|---|---|
| `cadet_process=True` | `--cadet` | Write `<chromatogram>_cadet/` (CADET-Process files) |
| `core_version="5"` / `"6"` | `--cadet-core 5` / `6` | Write a CADET-Core input in the layout of 5.1.2 or 6.0.0a5. No default: each Core rejects the other's layout |
| `core_format="json"` / `"hdf5"` | `--core-format` | `.json` (no dependency) or `.h5` (`pip install pycorn[cadet-core]`) |
| `core_overrides={...}` | `--core-overrides FILE.json` | Your model values in CADET-Core's own names, merged into the Core file (below) |
| `process_overrides={...}` | `--process-overrides FILE.json` | Your model values in CADET-Process's own names, stored in `experiment.json` (below) |
| `chromatogram="Chrom.1"` | (the original chromatogram) | Which chromatograms: a name, `"all"`, `"original"`, `"imported"`, `"derived"`, or a list of them (W17; KeyError for a name the run lacks). Without it, the run's original chromatogram; the others are named in the warnings. Several selected are all exported, with a warning (until users settle on a rule). An imported chromatogram exports from the run log it carries (`ImportedLogbook`, its source result's); `experiment.json` `source.run_log_source` names that result, this result's column type, mixer, delay volumes, method and system configuration are used for it, each with a warning (they may differ from the source result's run), and curves of another method run it carries are left out of the signals |
| `overwrite=True` | `--overwrite` | Replace files that exist |
| `bed_height`, `diameter`, `particle_diameter` | `--bed-height`, `--diameter`, `--particle-diameter` | Column geometry, used only where the run stores none |
| `override_geometry=True` | `--override-geometry` | Your geometry wins over what the run stores |
| `epsilon={280: (1.4, "ml/(mg*cm)")}` | `--epsilon 280=1.4` | Extinction coefficient for the primary UV channel: UV becomes a concentration |
| `molecular_weight=(66000, "g/mol")` | `--mw "66000 g/mol"` | Needed for mol/m^3, which CADET works in. Without it, a mass coefficient gives no concentration file and a warning. Given with nothing that uses it, it is noted in `warnings.csv` |
| `epsilon_molar=...` | `--epsilon-molar` | The same with a molar coefficient (needs the molecular weight) |
| `uv_response=0.92` | `--uv-response 0.92` | Detector response factor on the coefficient (here: reads 8 % low). No default. The concentration is an estimate either way: each one adds a note to the warnings (issue #1); `pycorn bypass-uv` measures the coefficient from the run's by-pass sample |
| `absorbance_limit=(2500, "mAU")` | `--absorbance-limit 2500` | Your detector's linear limit: concentration above it is empty. No default |
| `path_length_cm=0.2` | `--path-length` | UV path length where the run gives none |
| `mixer_volume=(0.6, "ml")` | `--mixer-volume` | Mixer CSTR volume; wins over the size the system stores |
| `tubing_volume=(0.2, "ml")` | `--tubing-volume` | Column-to-UV tubing CSTR; the instrument stores none |
| `sample_inlets=["A5"]` | `--sample-inlets A5` | A/B inlet positions that hold sample |
| `system_volumes=False` | `--no-system-volumes` | No mixer or tubing CSTR |

## What is written

### `<chromatogram>_cadet/` (CADET-Process)

- **One CSV per measured signal**, two columns (time in s, value) and no header row, as in the
  CADET-Process examples. `experiment.json` names each file's columns and unit. UV channels are
  in AU/cm, conductivity in S/m, %B in %, and with an extinction coefficient and molecular
  weight, the UV concentration in mol/m^3. Time zero is the method start.
- **`flow.csv`**: the measured flow, in m^3/s.
- **`experiment.json`**: the unit of every file, the column (geometry and where each value came
  from), the system volumes, the sections built from the run log, by-pass windows, events,
  fractions, buffer readings, method hints, the inputs for a CADET-Process `Process`
  (`process_inputs`: units, sections, inlet concentrations) and all warnings. Everything is in SI.

For live CADET-Process objects, use `pycorn.exporters.cadet.build_experiment()` and
`to_cadet_process()`. Set your model values on the returned `Process` with CADET-Process's own
API. To simulate, install both: `pip install pycorn[cadet]` gives CADET-Process 0.12, which
does not install a simulator, and `pip install CADET-Core==5.1.2` in the same environment gives
the CADET-Core it runs on (see "Comparing with your data and fitting").

### `<chromatogram>_core_v5.json` / `_core_v6.json` (CADET-Core)

This is a complete input file that `cadet-cli` runs (the output file must be `.h5`):

```
cadet-cli Chrom.1_core_v6.json out.h5
```

- **Units:** the flow path is buffer inlet -> mixer CSTR -> column -> tubing CSTR -> outlet,
  with a feed inlet straight to the column for valve injections. While the column is
  by-passed and the pumps run, the system flow goes through the mixer and around the column
  to the tubing (else the outlet), so the simulated outlet there is what the UV and
  conductivity saw; the column gets no flow (owner, 2026-11-08). The unit numbers follow
  another order: `unit_000` buffer inlet, `unit_001` column, `unit_002` outlet, then the
  feed inlet, the mixer and the tubing, each only when present, numbered from `unit_003` in
  that order. With all three, `unit_003` is the feed, `unit_004` the mixer and `unit_005` the
  tubing; without a feed inlet the mixer is `unit_003`. `/meta/UNIT_NAMES` lists the names in
  unit order. The column is always `unit_001` and the outlet `unit_002`.
- **Sections:** one per change in the run log, pauses included (written with Q = 0).
- **`/meta`:** where the file came from (run name, chromatogram, versions; no connection
  details), the section names, which sections feed, the placeholders, your overrides and the
  warnings.

No CADET-Core file is written without the column's bed height and diameter, from the run's
linked column or from you. Geometry is never a placeholder.

## Your model values

The run gives the process, not the column model. **Without your values, the CADET-Core file
uses placeholders:** the porosities and transport values of the CADET-Core benchmark, and no
binding (a tracer). They are listed in `/meta/placeholders` and in the warnings. The file
runs, and its flow path and timing are the run's, but it says nothing about your molecule.

Set model values with `core_overrides`: a nested dict in the layout of a CADET-Core input
file, starting at `input`. It is merged in last and unchanged: groups merge, any other value
replaces. PyCORN doesn't translate or check these names, so any field your CADET-Core version
reads can be set. A placeholder you replace leaves `/meta/placeholders`, and
`/meta/OVERRIDES` lists every path you set.

**Example only** (checked on CADET-Core 6.0.0a5, and in the CADET-Core 5 layout described below
on 5.1.2): a linear binding model. The components are `salt` and `sample` unless you give your
own. The values are made up.

```python
# EXAMPLE ONLY: shows the mechanism, not recommended values. Look up the fields for your
# model in the CADET documentation for your CADET-Core version.
linear_v6 = {"input": {"model": {"unit_001": {
    "COL_POROSITY": 0.37,
    "particle_type_000": {
        "ADSORPTION_MODEL": "LINEAR",
        "NBOUND": [0, 1],                 # salt doesn't bind, sample has one bound state
        "INIT_CS": [0.0],                 # one initial value per bound state
        "adsorption": {"IS_KINETIC": 1, "LIN_KA": [0.0, 2.0], "LIN_KD": [1.0, 1.0]}}}}}}

pycorn.export_cadet(run, "cadet_out/", core_version="6", chromatogram="Chrom.1",
                    bed_height=(2.5, "cm"), diameter=(1.6, "cm"), core_overrides=linear_v6)
```

In the CADET-Core 5 layout, the same fields sit on `unit_001` itself (no `particle_type_000`),
with `INIT_Q` instead of `INIT_CS`. Neither layout needs a surface diffusion value here:
5.1.2 and 6.0.0a5 give the same outlet with and without it (6 is written with
`HAS_SURFACE_DIFFUSION` 0).

On the command line, put the same dict in a JSON file: `pycorn export ... --cadet-core 6
--core-overrides linear_v6.json`.

**For CADET-Process**, `process_overrides` is the same kind of block in CADET-Process's own
parameter layout: the nested dict `process.parameters` shows, e.g.
`{"flow_sheet": {"column": {"bed_porosity": 0.37, "axial_dispersion": 1e-7}}}`.
`to_cadet_process(exp, process_overrides=...)` sets it last on the `Process` it returns
(`process.parameters = ...`), and CADET-Process itself rejects a name it doesn't know.
`export_cadet(..., process_overrides=...)` and `pycorn export --cadet --process-overrides
FILE.json` build no `Process`: they store the block unchanged in `experiment.json` under
`process_overrides`, so your own script sets it after building the `Process`. Things that are
objects in CADET-Process rather than values (a binding model) are made with its API on the
returned `Process`.

PyCORN reads nothing in either block (owner, 2026-11-08): it is passed on as given, so it can
hold whatever your CADET version takes. PyCORN gives no further examples and doesn't follow
CADET's models or parameters. If CADET rejects a file, the message comes from CADET and refers
to CADET's names.

## Your offline assays (load and fractions)

The run records how much UV, conductivity and flow there was; it doesn't record what was in the
load or what each fraction held. Those come from your offline assays (protein concentration,
impurity %, salt), and each species you assay becomes a CADET component. `export_cadet()` and
`pycorn export` don't take them: build the experiment yourself, then write it with the same
functions they use.

**1. Components and molecular weights.** One name per species, salt first if it matters for
your model, and a molecular weight for each (CADET works in mol/m^3):

```python
COMPONENTS = ["salt", "protein", "impurity"]
MW = {"salt": 58.44, "protein": 150000.0, "impurity": 300000.0}   # g/mol
```

**2. The load, in mol/m^3 per component.** From g/L: divide by the molecular weight in kg/mol
(`g/L / (g/mol / 1000)`; g/L is kg/m^3). An impurity % splits the total protein first:

```python
total_g_l, impurity_pct = 1.2, 4.0                   # your load assay
impurity_g_l = total_g_l * impurity_pct / 100.0      # % by mass of the total (e.g. SEC area %)
protein_g_l = total_g_l - impurity_g_l
feed = {"salt": 150.0,                               # mol/m^3 salt in the load
        "protein": protein_g_l / (MW["protein"] / 1000.0),
        "impurity": impurity_g_l / (MW["impurity"] / 1000.0)}
```

Area % from a chromatographic assay is taken as mass % here. If your assay reports something
else, convert it first.

**3. The buffers**, in mol/m^3, per inlet position. Many ÄKTA systems have a buffer valve on
each pump line, so a run draws from `A1`, `A2`, `B1`, ... rather than just "A" and "B". The key
is the position exactly as the run log names it (`Inlet A A1` -> `A1`). Process systems name
their inlets their own way (for example `WFI` or `Elution_buffer`): use the names their run log
shows. To see them, look at the `inlets` of the run log sections
(`pycorn.run_log_sections.run_log_sections(run)["sections"][k].inlets`) or the `inlets` column
of the exported `<chromatogram>_run_log_sections.csv`. Component names left out are 0. pycorn mixes the
buffers by the %B (or %Q) the run executed. For example, a run that equilibrates on A1 (20 mM
salt), washes on A2 (1000 mM) and elutes with B1 (500 mM):

```python
buffers = {"A1": {"salt": 20.0}, "A2": {"salt": 1000.0}, "B1": {"salt": 500.0}}
```

A pump line key (`A`, `B`, `Q1`-`Q4`) stands for every position on that line:

```python
buffers = {"A": {"salt": 0.0}, "B": {"salt": 1000.0}}
```

Each section uses the position it ran on, else its line. A line key for a line the run
switches between several positions on is used for all of them, with a warning. A position the
run used with neither key, or a key that is neither a line nor a position of the run, raises
`ValueError` (it names the inlet and time, or lists the positions found). Some runs log only
some positions: [`samples/sample.zip`](../samples/sample.zip) names `A1` only after its first section and no position
on line B, so it needs the line keys `A` and `B` (the walkthrough below uses them).

Where the run log set no position, the key follows the inlet's state, read from the system
configuration the result stores (`run.system_configuration()`; the same labels as the
`inlets` column):

| Line in the run | Shown as | Key it | Warning |
|---|---|---|---|
| position logged | `B2` | `B2` or `B` | only if `B` stands for several positions |
| no inlet valve on the line (one fixed line) | `B` | `B` | none |
| a valve, no position logged yet | `B1 (assumed)` | `B` (never `B1`: the position is a guess) | `buffers['B'] used for line B ...` |
| the configuration doesn't say | `B (not known)` | `B` | `buffers['B'] used for line B ...` |

A key of a position the run never logged (for example `B1` above) still raises `ValueError`,
as before; the message now says when it is a valve's assumed first position and to key the
line. Keys that worked before work the same. The experiment's `hints` give each section's
`inlet_labels` and `inlet_sources` (`known`, `assumed`, `not known`), and
`exp.system['inlet_configuration']` the installed valves and pumps.

**4. Fraction assays**, one row per tube and component, `conc_g_per_l` or `mass_mg`. `tube` is
the fraction name the run gives the tube (see `chrom.fraction_table()`). It's a CSV with these
columns, or the same rows as a list of dicts:

```
tube,component,conc_g_per_l,mass_mg
1.A.3,protein,0.52,
1.A.3,impurity,0.031,
1.A.4,protein,,0.80
```

With a concentration, pycorn multiplies by the fraction's volume from the run. An assay whose
tube matches no fraction is warned about.

**5. Build and write:**

```python
from pycorn.exporters.cadet import build_experiment, to_csv
from pycorn.exporters.cadet.core_input import to_core_input, write_json

exp = build_experiment(run, components={"names": COMPONENTS, "molecular_weights": MW,
                                        "feed": feed, "buffers": buffers},
                       fraction_masses="fraction_assays.csv",
                       epsilon=1.4, molecular_weight=MW["protein"],   # UV as protein, mol/m^3
                       bed_height=(2.5, "cm"), diameter=(1.6, "cm"))
to_csv(exp, "cadet_out/Chrom.1_cadet")              # experiment.json lists the fractions with their mass
core = to_core_input(exp, "6", placeholders=True)    # core_overrides= as above
write_json(core, "cadet_out/Chrom.1_core_v6.json")
```

The feed concentrations go into the feed sections (valve injections) and the buffer
concentrations into every section by its %B. The UV reference stays one signal: the UV sees
every species, so pycorn converts it with the coefficient and molecular weight you give (here the
protein's). `exp.warnings` says what was left out.

## Walkthrough: export, simulate, compare

[`examplescripts/cadet_walkthrough.py`](../examplescripts/cadet_walkthrough.py) does all of the above on [`samples/sample.zip`](../samples/sample.zip) and runs
the result:

```
python examplescripts/cadet_walkthrough.py --cadet-cli PATH/TO/cadet-cli --core 6 --out walk --plot
```

1. It fills in made-up offline values: a 0.043 g/L load with 4 % impurity in running buffer,
   0.17 M salt in line B, and assays for the run's fraction `Frac`. They're chosen to agree with
   the run: the load's mass is what the measured UV shows. Replace them with your own. They are
   at the top of the script, under "EXAMPLE VALUES".
2. It builds the experiment and writes the CADET-Core input with placeholders (no binding).
3. It runs `cadet-cli` (5 or 6).
4. It writes `overlay.csv` (measured UV as protein, simulated protein, impurity and salt,
   measured conductivity, and a flag where the column was by-passed), `fractions.csv` (simulated
   vs assayed mass per fraction) and, with `--plot`, `overlay.png`. It prints the mass balance.

What you should see: protein and impurity mass balance 1.000, and salt about 0.92 (the column and
mixer are still full of salt at the end). The simulated fraction mass is within 2 % of the
assays. The simulated peak comes earlier and narrower than the measured one: the placeholder
porosities, dispersion and the missing tubing volume are not this column's, and nothing is
fitted. Closing that gap is CADET's job (next section).

`tests/test_cadet_core_run.py` runs this script on both Cores when `cadet-cli` is given (see
"Checking with CADET"), so the walkthrough stays working.

## Comparing with your data and fitting

pycorn hands CADET-Process the measured signals as references. The comparison and the fitting
are CADET-Process's own `Comparator` and optimization; pycorn adds no comparison or fitting
code of its own.

- **A quick look without CADET-Process:** `overlay.csv` from the walkthrough, or the
  `<chromatogram>_cadet/` CSVs (one per signal: time in s and value, no header row) next to your
  simulation's outlet.
- **In CADET-Process** (`pip install pycorn[cadet]` and `pip install CADET-Core==5.1.2` in the
  same environment: CADET-Process 0.12 brings no simulator and runs on CADET-Core 5): `to_cadet_process()`
  returns `references` (one `ReferenceIO` per measured signal, plus `UV ... mol/m^3` when you gave a
  coefficient and molecular weight), `fractionation` (your fraction assays, in mol) and `process`.
  Set your model values on the `Process` with CADET-Process's API, simulate, and compare. Checked
  with CADET-Process 0.12.0 on CADET-Core 5.1.2:

```python
# Values below are placeholders for the mechanism, not recommendations
from CADETProcess.comparison import Comparator
from CADETProcess.processModel import ComponentSystem
from CADETProcess.reference import ReferenceIO
from CADETProcess.simulator import Cadet
from pycorn.exporters.cadet import to_cadet_process

cp = to_cadet_process(exp, molecular_weights=MW)       # exp from "Your offline assays"
process = cp["process"]
column = process.flow_sheet["column"]
column.bed_porosity, column.particle_porosity, column.particle_radius = 0.37, 0.75, 4.5e-5
column.axial_dispersion = 5.75e-8
column.film_diffusion = [6.9e-6] * 3
column.pore_diffusion = [6.07e-11] * 3

# install_path: the root folder of the environment CADET-Core was pip-installed into (the
# folder that holds Scripts/ on Windows or bin/ elsewhere), e.g. "C:/envs/cadet" or "/home/me/envs/cadet"
results = Cadet(install_path="PATH/TO/ENVIRONMENT").simulate(process)

measured = next(r for r in cp["references"] if r.name.endswith("mol/m^3"))
# CADET-Process matches components by name: name the measured UV's component like the
# simulated species it stands for
uv = ReferenceIO("UV as protein", measured.time, measured.solution, flow_rate=measured.flow_rate,
                 component_system=ComponentSystem(["protein"]))
comparator = Comparator()
comparator.add_reference(uv)
comparator.add_difference_metric("NRMSE", uv, "outlet.inlet", components=["protein"], start=400, end=2250)

# The fraction assays: one value per assayed component and fraction
fractions = cp["fractionation"]                      # None without fraction assays
comparator.add_reference(fractions)
comparator.add_difference_metric("FractionationSSE", fractions, "outlet.inlet",
                                 components=fractions.component_system.names)
comparator.evaluate(results)            # [NRMSE, SSE protein, SSE impurity]
comparator.plot_comparison(results)
```

The solution path is `outlet.inlet`: the outlet unit of pycorn's flow sheet. Use `start` / `end`
(in s from the method start) to leave out windows where the column was by-passed
(`experiment.json`, `bypass`) and the end of the run. `start=400, end=2250` fit
[`samples/sample.zip`](../samples/sample.zip) only (by-pass until 79.5 s, run end at about 2260 s); take yours from
your run's `bypass` windows and the part of the UV you want to fit.

`cp["fractionation"]` is a CADET-Process `FractionationReference` in mol, one fraction per
assayed tube with its start and end time and volume, for the components you assayed.
`FractionationSSE` collects the simulated outlet over the same windows; name the components,
or CADET-Process compares every simulated component and stops with "Simulation shape does not
match experiment". For other fraction metrics, see the CADET-Process documentation.

- **Fitting:** CADET-Process fits a `Process` to a `Comparator` with its `OptimizationProblem`
  (variables such as `flow_sheet.column.bed_porosity`, the comparator's metrics as objectives,
  and an optimizer). See the CADET-Process documentation for your version. pycorn's part ends
  with the references and the `Process` above.

## Warnings

`export_cadet()` returns the warnings as `source: message`. `pycorn export` writes them to
`warnings.csv`. The sources:

- **`cadet`**: building the experiment from the run. Examples: guesses (a wavelength taken from a
  curve name), missing values (no tubing volume, no extinction coefficient for the primary UV),
  and sample inlets or caller values that weren't used. Column geometry notes (the run's values
  versus yours, a geometry that disagrees with the run's Vc) come as `cadet: column: ...`.
- **`cadet_core`**: the CADET-Core file. Examples: the placeholders used, sections with no column
  flow, your overrides (`caller overrides (...)`), and the reason a file wasn't written.
- **`geometry`** (`pycorn export` only): `warnings.csv` also lists the run's column geometry
  notes under this source, with or without `--cadet`. `export_cadet()` has no `geometry`
  source; it gives the same notes as `cadet: column: ...`.

## Known limitations

- Sample pumped through an A/B inlet line with no injection mark is treated as buffer. Name
  such inlets with `sample_inlets`. On a system whose stored configuration has no sample
  pump, the injection-mark warnings add that sample reaches the column through the loop or an
  A/B line (inferred); the configuration never makes or removes a feed window (D27 (4)).
- The quaternary valve's lines are taken, for now, as feeding the A and B pumps; the Q shares
  are used as logged (provisional until the instrument manual says where the valve sits).
- The instrument stores no column-to-UV tubing volume; give `tubing_volume` for a tubing CSTR.
  The mixer and tubing are CSTRs (stirred tanks), so they carry no axial dispersion and PyCORN
  writes none. If you replace one with a plug-flow tube in your own model, do not set its axial
  dispersion to 0: CADET can stall or need a very fine grid. The CADET forum's advice
  (threads 1172 and 1253) is a small nonzero value, about 1e-4 (the thread gives no unit; check
  it against yours) for 1 mm tubing by the Taylor expression for open tubes, and the right inner diameter for each piece of tubing.
- The column, mixer and tubing start empty: `INIT_C` is 0 for every component, salt included
  (the column's particles too: `INIT_CP` 0). What they should hold at the start is an open
  question for the modeler ([`docs/ROADMAP.md`](ROADMAP.md), "Modeler input"). Set it through
  `core_overrides` / `to_core_input(overrides=)` in CADET-Core's own names, or on the
  CADET-Process units. For example, a column full of 100 mol/m^3 salt in the CADET-Core 6
  layout, with components `salt` and `sample` (checked on 5.1.2 and 6.0.0a5; in the 5 layout
  `INIT_CP` sits on `unit_001` itself):

  ```python
  core_overrides = {"input": {"model": {"unit_001": {
      "INIT_C": [100.0, 0.0],                                  # interstitial liquid
      "particle_type_000": {"INIT_CP": [100.0, 0.0]}}}}}       # particle pores
  ```

  The mixer and tubing take `INIT_C` the same way, on their unit numbers (`/meta/UNIT_NAMES`).
- pycorn doesn't compare the measured UV with the simulated outlet or fit anything; CADET-Process
  does (see "Comparing with your data and fitting").
- Offline assays (load composition, fraction results) go in through `build_experiment()`, not
  through `export_cadet()` or `pycorn export` (see "Your offline assays").
- CADET-Process 0.12 runs on CADET-Core 5 only. CADET-Core 6 is an alpha (6.0.0a5), so its
  field names may change.

## Checking with CADET

`tests/test_cadet_core_run.py` runs the CADET-Core input with `cadet-cli`. It checks that the
injected sample comes out (mass balance within 1 %) on each version and that each Core rejects
the other's layout. It is skipped unless `PYCORN_CADET_CLI_5` and/or
`PYCORN_CADET_CLI_6` name a `cadet-cli` executable (from `pip install CADET-Core==5.1.2` or
`==6.0.0a5` in a separate virtual environment), and it needs `h5py`.
