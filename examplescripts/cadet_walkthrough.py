# -*- coding: utf-8 -*-
"""
CADET walkthrough: from a UNICORN run to a CADET-Core simulation, next to what was measured.

    python examplescripts/cadet_walkthrough.py --cadet-cli PATH/TO/cadet-cli [--core 6] [--run RUN.zip]
                                               [--out DIR] [--plot]

Steps (docs/CADET_GUIDE.md, "Walkthrough"):
1. Load the run and fill in what was measured offline: the load (protein g/L and impurity %), the
   buffers' salt, and the fraction assays. They become components with mol/m^3 concentrations.
2. Build the experiment from what the run executed and write the CADET-Core input.
3. Run cadet-cli.
4. Write the simulated outlet next to the measured UV and conductivity (`overlay.csv`), the
   simulated vs assayed mass per fraction (`fractions.csv`), the mass balance, and with --plot
   `overlay.png` (needs matplotlib).

EVERY VALUE IN "EXAMPLE VALUES" IS MADE UP for samples/sample.zip. Replace them with your own.
Nothing is fitted: the column model uses placeholders (CADET-Core benchmark transport, no binding),
so all species come out together. Set your model with `core_overrides` (docs/CADET_GUIDE.md).
Needs h5py (pip install pycorn[cadet-core]) and a cadet-cli (pip install CADET-Core in its own venv).
"""
import argparse
import csv
import os
import subprocess
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))  # run from a checkout without installing

from pycorn import open_run  # noqa: E402
from pycorn.exporters.cadet import build_experiment, process_inputs  # noqa: E402
from pycorn.exporters.cadet.core_input import to_core_input, write_json  # noqa: E402

# ---- EXAMPLE VALUES (made up; replace with your own) --------------------------------------------
RUN = os.path.join(os.path.dirname(HERE), "samples", "sample.zip")
# The run stores no column geometry; these give its stored column volume (0.7 ml).
COLUMN = dict(bed_height=(3.57, "cm"), diameter=(0.5, "cm"))
EPSILON_280 = (1.0, "ml/(mg*cm)")  # the protein's mass extinction coefficient at 280 nm
COMPONENTS = ["salt", "protein", "impurity"]  # CADET component order; salt first
MOLECULAR_WEIGHT_G_MOL = {"salt": 58.44, "protein": 150000.0, "impurity": 300000.0}
# Offline load assay: total protein (g/L, protein + impurity) and the impurity's share (% by mass,
# e.g. a SEC area %), in a load with this much salt. Chosen so the load's mass matches what the
# measured UV shows with EPSILON_280 (0.0039 mg from 0.092 ml), and the load in running buffer.
LOAD = {"total_protein_g_per_l": 0.043, "impurity_percent": 4.0, "salt_mol_m3": 169.0}
# Salt in line A and line B, mol/m^3 (170 mol/m^3 = 0.17 M; the run's ~17 mS/cm at 99 %B)
BUFFER_SALT_MOL_M3 = {"A": 0.0, "B": 170.0}
# Offline fraction assays: one row per tube and component, `conc_g_per_l` or `mass_mg`. `tube` is the
# fraction name in the run (here 'Frac', 2.2 ml). The same rows can come from a CSV file with these
# columns (read_fraction_masses).
FRACTION_ASSAYS = [
    {"tube": "Frac", "component": "protein", "conc_g_per_l": 0.00170},
    {"tube": "Frac", "component": "impurity", "conc_g_per_l": 0.000071},
]
# -------------------------------------------------------------------------------------------------


def g_per_l_to_mol_m3(g_per_l, molecular_weight_g_mol):
    """g/L is kg/m^3; divided by kg/mol gives mol/m^3."""
    return g_per_l / (molecular_weight_g_mol / 1000.0)


def load_concentrations(load, mw):
    """The load assay as mol/m^3 per component: protein and impurity split by the impurity %."""
    impurity_g_l = load["total_protein_g_per_l"] * load["impurity_percent"] / 100.0
    protein_g_l = load["total_protein_g_per_l"] - impurity_g_l
    return {"salt": load["salt_mol_m3"],
            "protein": g_per_l_to_mol_m3(protein_g_l, mw["protein"]),
            "impurity": g_per_l_to_mol_m3(impurity_g_l, mw["impurity"])}


def build(run_path=RUN):
    """The experiment with the offline values filled in (step 1-2)."""
    feed = load_concentrations(LOAD, MOLECULAR_WEIGHT_G_MOL)
    components = {"names": COMPONENTS, "molecular_weights": MOLECULAR_WEIGHT_G_MOL,
                  "feed": feed, "buffers": {line: {"salt": c} for line, c in BUFFER_SALT_MOL_M3.items()}}
    return build_experiment(open_run(run_path), components=components, fraction_masses=FRACTION_ASSAYS,
                            epsilon=EPSILON_280[0], epsilon_unit=EPSILON_280[1],
                            molecular_weight=MOLECULAR_WEIGHT_G_MOL["protein"], molecular_weight_unit="g/mol",
                            **COLUMN)


def simulate(exp, cadet_cli, core_version, out_dir, core_overrides=None):
    """Write the Core input, run cadet-cli, return (times s, outlet mol/m^3 [time, component], input path)."""
    import h5py
    core = to_core_input(exp, core_version, placeholders=True, overrides=core_overrides)
    src = os.path.join(out_dir, "core_v%s.json" % core_version)
    out = os.path.join(out_dir, "core_v%s_out.h5" % core_version)  # cadet-cli writes .h5 only
    write_json(core, src)
    done = subprocess.run([cadet_cli, src, out], capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError("cadet-cli failed (exit %d): %s" % (done.returncode, (done.stderr or done.stdout)[-800:]))
    with h5py.File(out, "r") as f:
        t = f["output/solution/SOLUTION_TIMES"][()]
        c = f["output/solution/unit_002/SOLUTION_OUTLET"][()].reshape(len(t), -1)
    return t, c, src


def flows_on(exp, grid):
    """
    On `grid`, from the sections: the outlet flow (m^3/s: the column's, plus the system flow past a
    by-passed column), the feed flow (m^3/s), and what enters through both inlets (mol/s per
    component).
    """
    rows = process_inputs(exp)["sections"]
    n = len(rows[0]["c_feed"])
    q, q_feed, inflow = np.zeros(len(grid)), np.zeros(len(grid)), np.zeros((len(grid), n))
    for r in rows:
        on = (grid >= r["start_s"]) & (grid < r["end_s"])
        dt = grid[on] - r["start_s"]
        q[on] = r["flow_m3_s"][0] + r["flow_m3_s"][1] * dt + r["bypass_flow_m3_s"][0] + r["bypass_flow_m3_s"][1] * dt
        q_feed[on] = r["feed_flow_m3_s"][0] + r["feed_flow_m3_s"][1] * dt
        q_buffer = r["buffer_flow_m3_s"][0] + r["buffer_flow_m3_s"][1] * dt
        for k in range(n):
            inflow[on, k] = (q_feed[on] * np.polyval(np.asarray(r["c_feed"][k])[::-1], dt)
                             + q_buffer * np.polyval(np.asarray(r["c_buffer"][k])[::-1], dt))
    return q, q_feed, inflow


def report(exp, t, c, out_dir):
    """overlay.csv, fractions.csv and the mass balance (step 4). Returns the numbers."""
    # A fine grid with the section flows: the solver's output grid has almost no points in pauses
    rows = process_inputs(exp)["sections"]
    grid = np.linspace(rows[0]["start_s"], rows[-1]["end_s"], 200001)
    step = grid[1] - grid[0]
    q, _, inflow = flows_on(exp, grid)
    c_grid = np.column_stack([np.interp(grid, t, c[:, k]) for k in range(c.shape[1])])
    # Out over in for the whole run; what is still in the system at the end (salt filling the
    # column and mixer) counts as missing
    balance = {}
    for k, name in enumerate(COMPONENTS):
        fed = inflow[:, k].sum() * step
        if fed > 0:
            balance[name] = (c_grid[:, k] * q).sum() * step / fed

    # Measured signals on their own time grid; simulated values interpolated onto it
    uv = exp.primary_uv
    cond = next((s for s in exp.signals if s.role == "conductivity"), None)
    ts = np.asarray(uv.time_s, dtype=float)
    keep = (ts >= grid[0]) & (ts <= grid[-1])
    ts = ts[keep]
    measured_uv = np.asarray(uv.concentration["mol/m^3"], dtype=float)[keep]
    bypass = np.zeros(len(ts), dtype=int)
    for a, b in uv.not_column_outlet:
        bypass[(ts >= a) & (ts <= b)] = 1
    measured_cond = (np.interp(ts, np.asarray(cond.time_s, float), np.asarray(cond.export_values, float))
                     if cond is not None else np.full(len(ts), np.nan))
    sim = {name: np.interp(ts, t, c[:, k]) for k, name in enumerate(COMPONENTS)}
    overlay = os.path.join(out_dir, "overlay.csv")
    with open(overlay, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time_s", "measured_uv_protein_equivalent_mol_m3", "simulated_protein_mol_m3",
                    "simulated_impurity_mol_m3", "simulated_salt_mol_m3", "measured_conductivity_S_m",
                    "column_bypassed"])
        for i in range(len(ts)):
            w.writerow(["%.3f" % ts[i], "%.6g" % measured_uv[i], "%.6g" % sim["protein"][i],
                        "%.6g" % sim["impurity"][i], "%.6g" % sim["salt"][i], "%.6g" % measured_cond[i], bypass[i]])

    # Simulated mass per fraction window vs the assay
    fractions = []
    for fr in exp.fractions:
        if not fr.mass_kg or fr.start_s is None or fr.end_s is None:
            continue
        on = (grid >= fr.start_s) & (grid < fr.end_s)
        for name, assay_kg in fr.mass_kg.items():
            k = COMPONENTS.index(name)
            mol = (c_grid[on, k] * q[on]).sum() * step
            fractions.append({"tube": fr.tube, "component": name, "start_s": fr.start_s, "end_s": fr.end_s,
                              "assay_mg": assay_kg * 1e6,
                              "simulated_mg": mol * MOLECULAR_WEIGHT_G_MOL[name] / 1000.0 * 1e6})
    frac_path = os.path.join(out_dir, "fractions.csv")
    with open(frac_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["tube", "component", "start_s", "end_s", "assay_mg", "simulated_mg"])
        w.writeheader()
        w.writerows(fractions)
    return {"balance": balance, "fractions": fractions, "overlay": overlay, "fractions_csv": frac_path,
            "series": (ts, measured_uv, sim, measured_cond, bypass)}


def plot(result, exp, path):
    """overlay.png: outlet concentration, simulated salt, measured conductivity (one unit per panel)."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    ts, measured_uv, sim, measured_cond, bypass = result["series"]
    minutes = ts / 60.0
    # Reference palette (dataviz skill), categorical slots 1-3 in fixed order; ink and grid recessive
    blue, orange, aqua, ink, muted, grid = "#2a78d6", "#eb6834", "#1baf7a", "#0b0b0b", "#52514e", "#e4e3df"
    fig, axes = plt.subplots(3, 1, figsize=(9, 7.5), sharex=True, facecolor="#fcfcfb",
                             gridspec_kw={"height_ratios": [2.2, 1, 1]})
    feed = [s for s in exp.sections if s.feed]
    for ax in axes:
        ax.set_facecolor("#fcfcfb")
        ax.grid(True, color=grid, linewidth=0.8)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color(muted)
        ax.tick_params(colors=muted, labelsize=9)
        for s in feed:
            ax.axvspan(s.start_s / 60.0, s.end_s / 60.0, color=muted, alpha=0.12, linewidth=0)
        for a0, b0 in exp.primary_uv.not_column_outlet:  # the measured signals there are not column outlet
            ax.axvspan(a0 / 60.0, b0 / 60.0, facecolor="none", edgecolor=grid, hatch="///", linewidth=0)
    a = axes[0]
    a.plot(minutes, measured_uv, color=blue, linewidth=2, label="Measured UV 280 (as protein)")
    a.plot(minutes, sim["protein"] + sim["impurity"], color=orange, linewidth=2,
           label="Simulated protein + impurity")
    a.plot(minutes, sim["impurity"], color=aqua, linewidth=2, label="Simulated impurity")
    a.set_ylabel("Outlet, mol/m$^3$", color=ink, fontsize=10)
    a.legend(frameon=False, fontsize=9, labelcolor=ink, loc="upper right")
    a.set_title("Measured vs simulated outlet (placeholder model, nothing fitted)\n"
                "shaded: load; hatched: column by-passed (measured values not column outlet)",
                color=ink, fontsize=11, loc="left")
    axes[1].plot(minutes, sim["salt"], color=blue, linewidth=2)
    axes[1].set_ylabel("Simulated salt,\nmol/m$^3$", color=ink, fontsize=10)
    axes[2].plot(minutes, measured_cond * 10.0, color=blue, linewidth=2)  # S/m -> mS/cm
    axes[2].set_ylabel("Measured cond.,\nmS/cm", color=ink, fontsize=10)
    axes[2].set_xlabel("Time from method start, min", color=ink, fontsize=10)
    fig.tight_layout()
    fig.savefig(path, dpi=130, facecolor=fig.get_facecolor())
    plt.close(fig)
    return path


def walkthrough(cadet_cli, core_version="6", run_path=RUN, out_dir="cadet_walkthrough", make_plot=False,
                core_overrides=None):
    os.makedirs(out_dir, exist_ok=True)
    exp = build(run_path)
    t, c, src = simulate(exp, cadet_cli, core_version, out_dir, core_overrides)
    result = report(exp, t, c, out_dir)
    result.update(core_input=src, warnings=list(exp.warnings), times=t, outlet=c)
    if make_plot:
        result["plot"] = plot(result, exp, os.path.join(out_dir, "overlay.png"))
    return result


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--core", choices=("5", "6"), default="6", help="CADET-Core layout (default 6)")
    p.add_argument("--cadet-cli", help="cadet-cli of that CADET-Core version (default: $PYCORN_CADET_CLI_<core>)")
    p.add_argument("--run", default=RUN, help="UNICORN .zip (default: samples/sample.zip; the example values fit it)")
    p.add_argument("--out", default="cadet_walkthrough", help="output folder")
    p.add_argument("--plot", action="store_true", help="also overlay.png (needs matplotlib)")
    args = p.parse_args(argv)
    cli = args.cadet_cli or os.environ.get("PYCORN_CADET_CLI_%s" % args.core)
    if not cli:
        p.error("give --cadet-cli or set PYCORN_CADET_CLI_%s" % args.core)
    r = walkthrough(cli, args.core, args.run, args.out, args.plot)
    print("[OK] simulated %s on CADET-Core %s" % (os.path.basename(r["core_input"]), args.core))
    for name, ratio in r["balance"].items():
        print("  mass balance %-9s out/in %.4f" % (name, ratio))
    for row in r["fractions"]:
        print("  fraction %-6s %-9s assay %.4g mg, simulated %.4g mg"
              % (row["tube"], row["component"], row["assay_mg"], row["simulated_mg"]))
    print("  wrote %s, %s%s" % (r["overlay"], r["fractions_csv"], (", " + r["plot"]) if "plot" in r else ""))
    print("  %d warning(s) from the experiment; the first ones:" % len(r["warnings"]))
    for w in r["warnings"][:5]:
        print("   - %s" % w[:150].encode("ascii", "replace").decode("ascii"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
