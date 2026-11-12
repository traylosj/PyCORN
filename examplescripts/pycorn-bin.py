#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
PyCORN - script to extract data from .res (results) files generated
by UNICORN Chromatography software supplied with ÄKTA Systems
(c)2014-2016 - Yasar L. Ahmed
v0.18

Command-line tool: check, extract (csv/xlsx) and plot UNICORN results.
Accepts UNICORN 3-5 .res files (PcRes3) and UNICORN 6+ .zip bundles (PcUni6).

Example:
    python pycorn-bin.py -p -e csv run.res
'''

import argparse
import os
import re
import sys
from collections import OrderedDict
from types import SimpleNamespace

try:
    import pycorn  # noqa: F401  (installed)
except ImportError:  # run from a source checkout: the package is next to this folder
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pycorn import PcUni6, PcRes3, open_run
from pycorn.curve_roles import curve_has_role

try:
    from mpl_toolkits.axes_grid1 import host_subplot
    from matplotlib.ticker import AutoMinorLocator
    import mpl_toolkits.axisartist as AA
    import matplotlib.pyplot as plt

    plotting = True
except ImportError:
    plotting = False

try:
    import xlsxwriter

    xlsx = True
except ImportError:
    xlsx = False

pcscript_version = 0.14

# command line options; parsed at import, so this file is a script, not a module

parser = argparse.ArgumentParser(
    description="Extract data from UNICORN .res files to .csv/.txt and plot them (matplotlib required)",
    epilog="Make it so!")
parser.add_argument("-c", "--check", help="Perform simple check if file is supported (.res only: reads the "
                                          "binary .res header)", action="store_true")
parser.add_argument("-n", "--info", help="Display entries in header (.res only)", action="store_true")
parser.add_argument("-i", "--inject", type=int, default=None,
                    help="Set injection number # as zero retention (0 = stored volumes, default -1 = last "
                         "injection); for .res use -t to find injection points", metavar="#")
parser.add_argument("-r", "--reduce", type=int, default=1, help="Write/Plot only every n sample", metavar="#")
parser.add_argument("-t", "--points", help="Display injection points (.res only)", action="store_true")

group0 = parser.add_argument_group('Extracting', 'Options for writing csv/txt files')
group0.add_argument("-e", "--extract", type=str, choices=['csv', 'xlsx'],
                    help="Write data to csv or xlsx file for supported data blocks")

group1 = parser.add_argument_group('Plotting', 'Options for plotting')
group1.add_argument("-p", "--plot", help='Plot curves', action="store_true")
group1.add_argument("--no_fractions", help="Disable plotting of fractions", action="store_true")
group1.add_argument("--no_inject", help="Disable plotting of inject marker(s)", action="store_true")
group1.add_argument("--no_legend", help="Disable legend for plot", action="store_true")
group1.add_argument("--no_title", help="Disable title for plot", action="store_true")
group1.add_argument("--xmin", type=float, default=None, help="Lower bound on the x-axis", metavar="#")
group1.add_argument("--xmax", type=float, default=None, help="Upper bound on the x-axis", metavar="#")
group1.add_argument("--par1", type=str, default='Cond',
                    help="Data for 2nd y-axis (Default=Cond), to disable 2nd y-axis, use --par1 None")
group1.add_argument("--par2", type=str, default=None, help="Data for 3rd y-axis (Default=None)")
group1.add_argument('-f', '--format', type=str,
                    choices=['svg', 'svgz', 'tif', 'tiff', 'jpg', 'jpeg', 'png', 'ps', 'eps', 'raw', 'rgba', 'pdf',
                             'pgf'], default='pdf', help="File format of plot files (default: pdf)")
group1.add_argument('-d', '--dpi', default=300, type=int,
                    help="DPI (dots per inch) for raster images (png, jpg, etc.). Default is 300.")
parser.add_argument("-u", "--user", help="Show stored user name (.res only)", action="store_true")
parser.add_argument('--version', action='version', version=str(pcscript_version))
parser.add_argument("inp_res", help="Input .res file(s)", nargs='+', metavar="<file>.res")
# args.no_inject
args = parser.parse_args()
# what was asked for and can't be done: stop before reading anything
if args.plot and not plotting:
    parser.error("-p/--plot needs matplotlib: pip install pycorn[plotting]")
if args.extract == 'xlsx' and not xlsx:
    parser.error("-e xlsx needs xlsxwriter: pip install pycorn[xlsx-output]")


def mapper(min_val, max_val, perc):
    '''
    calculate relative position in delta min/max

    Returns the value a fraction `perc` of the way along the range; used to place
    labels at a fixed height on an axis.
    '''
    x = abs(max_val - min_val) * perc
    if min_val < 0:
        return (x - abs(min_val))
    else:
        return (x + min_val)


def expander(min_val, max_val, perc):
    '''
    expand -/+ direction of two values by a percentage of their delta

    Returns (new_min, new_max); used to pad axis limits.
    '''
    delta = abs(max_val - min_val)
    x = delta * perc
    return (min_val - x, max_val + x)


def safe_name(name):
    '''
    `name` with characters not allowed in file names replaced by '_'
    (curve names such as "Sample flow (CV/h)" contain them).
    '''
    return ''.join('_' if c in '\\/:*?"<>|' else c for c in name)


def xy_data(inp):
    '''
    Takes a data block and returns two lists with x- and y-data
    '''
    x_data = [x[0] for x in inp]
    y_data = [x[1] for x in inp]
    return x_data, y_data


# wavelength at the end of a UV curve name: 'UV1_280nm', 'UV2_0nm' (.res), 'UV 1_280', 'UV 3_0',
# 'UV_280' (UNICORN 6+). Neither format stores the wavelength per curve, only in the name.
UV_WAVELENGTH = re.compile(r"[\s_](\d+(?:\.\d+)?)\s*(?:nm)?\s*$", re.IGNORECASE)


def is_uv(entry):
    '''
    True for a UV curve: by the stored CurveDataType for UNICORN 6+ (the 'uv' role of
    pycorn.curve_roles), by name for .res files, which store no curve type.
    '''
    if entry.get('data_type') == 'curve':  # .res sensor data
        return entry['data_name'].startswith('UV')
    curve = SimpleNamespace(name=entry['data_name'], data_type=entry.get('data_type'), unit=entry.get('unit'))
    return curve_has_role(curve, 'uv')


def uv_blocks(inp):
    '''
    Names of the UV curves to plot and scale on: every UV curve except channels that
    were off (wavelength 0 in the name). A name without a wavelength is kept.
    '''
    names = []
    for i in inp.keys():
        if not is_uv(inp[i]):
            continue
        m = UV_WAVELENGTH.search(inp[i]['data_name'])
        if m and float(m.group(1)) == 0:
            continue
        names.append(i)
    return names


def smartscale(inp):
    '''
    input is the entire fdata block
    checks user input/fractions to determine scaling of x/y-axis
    returns min/max for x/y

    x range: --xmin/--xmax if given, else the span of the fractions (plus two
    fraction widths), else the span of the first UV curve. y range: the min/max of
    all UV curves within the x range, padded by 8.5 %.
    '''
    UV_blocks = uv_blocks(inp)
    uv1_data = inp[UV_blocks[0]]['data']
    uv1_x, uv1_y = xy_data(uv1_data)
    try:
        frac_data = inp['Fractions']['data']
        frac_x, frac_y = xy_data(frac_data)
        frac_delta = [abs(a - b) for a, b in zip(frac_x, frac_x[1:])]
        frac_delta.append(frac_delta[-1])
    except (KeyError, IndexError):
        # no fractions, or fewer than two (no width to compute)
        frac_data = None
    if args.xmin != None:
        plot_x_min = args.xmin
    else:
        if frac_data:
            plot_x_min = frac_data[0][0]
        else:
            plot_x_min = uv1_x[0]
    if args.xmax:
        plot_x_max = args.xmax
    else:
        if frac_data:
            plot_x_max = frac_data[-1][0] + frac_delta[-1] * 2  # recheck
        else:
            plot_x_max = uv1_x[-1]
    if plot_x_min > plot_x_max:
        print("Warning: xmin bigger than xmax - adjusting...")
        plot_x_min = uv1_x[0]
    if plot_x_max < plot_x_min:
        print("Warning: xmax smaller than xmin - adjusting...")
        plot_x_max = uv1_x[-1]
    # optimize y_scaling
    min_y_values = []
    max_y_values = []
    for i in UV_blocks:
        tmp_x, tmp_y = xy_data(inp[i]['data'])
        range_min_lst = [abs(a - plot_x_min) for a in tmp_x]
        range_min_idx = range_min_lst.index(min(range_min_lst))
        range_max_lst = [abs(a - plot_x_max) for a in tmp_x]
        range_max_idx = range_max_lst.index(min(range_max_lst))
        values_in_range = tmp_y[range_min_idx:range_max_idx]
        min_y_values.append(min(values_in_range))
        max_y_values.append(max(values_in_range))
    plot_y_min_tmp = min(min_y_values)
    plot_y_max_tmp = max(max_y_values)
    plot_y_min, plot_y_max = expander(plot_y_min_tmp, plot_y_max_tmp, 0.085)
    return plot_x_min, plot_x_max, plot_y_min, plot_y_max


def plotterX(inp, fname):
    '''
    Plot the UV curves of a loaded file and save the plot next to the input file.

    --par1/--par2 add curves on a 2nd/3rd y-axis; fractions and the injection mark are
    drawn unless disabled. Output: <fname>_<run name>_plot.<format>
    '''
    if not uv_blocks(inp):
        print("Skipping plot of " + inp.run_name + ": no UV curve")
        return
    plot_x_min, plot_x_max, plot_y_min, plot_y_max = smartscale(inp)
    host = host_subplot(111, axes_class=AA.Axes)
    host.set_xlabel("Elution volume (ml)")
    host.set_ylabel("Absorbance (mAu)")
    host.set_xlim(plot_x_min, plot_x_max)
    host.set_ylim(plot_y_min, plot_y_max)
    for i in uv_blocks(inp):
        x_dat, y_dat = xy_data(inp[i]['data'])
        print("Plotting: " + inp[i]['data_name'])
        stl = style(i)
        p0, = host.plot(x_dat, y_dat, label=inp[i]['data_name'], color=stl['color'], ls=stl['ls'], lw=stl['lw'],
                        alpha=stl['alpha'])
    if args.par1 == 'None':
        args.par1 = None
    if args.par1:
        if args.par1 not in inp:
            print("Warning: Data block chosen for par1 does not exist!")
        else:
            par1 = host.twinx()
            # axisartist twin axes start with tick labels and label hidden (matplotlib's
            # demo_parasite_axes2): switch them on
            par1.axis["right"].toggle(all=True)
            par1_data = inp[args.par1]
            stl = style(args.par1)
            par1.set_ylabel(axis_label(par1_data), color=stl['color'])
            x_dat_p1, y_dat_p1 = xy_data(par1_data['data'])
            p1_ymin, p1_ymax = expander(min(y_dat_p1), max(y_dat_p1), 0.085)
            par1.set_ylim(p1_ymin, p1_ymax)
            print("Plotting: " + par1_data['data_name'])
            p1, = par1.plot(x_dat_p1, y_dat_p1, label=par1_data['data_name'], color=stl['color'], ls=stl['ls'],
                            lw=stl['lw'], alpha=stl['alpha'])
    if args.par2:
        if args.par2 not in inp:
            print("Warning: Data block chosen for par2 does not exist!")
        else:
            par2 = host.twinx()
            offset = 60
            # a right axis of its own, 60 points further out, with ticks, numbers and label
            par2.axis["right"] = par2.new_fixed_axis(loc="right", offset=(offset, 0))
            par2.axis["right"].toggle(all=True)
            par2_data = inp[args.par2]
            stl = style(args.par2)
            par2.set_ylabel(axis_label(par2_data), color=stl['color'])
            x_dat_p2, y_dat_p2 = xy_data(par2_data['data'])
            p2_ymin, p2_ymax = expander(min(y_dat_p2), max(y_dat_p2), 0.075)
            par2.set_ylim(p2_ymin, p2_ymax)
            print("Plotting: " + par2_data['data_name'])
            p2, = par2.plot(x_dat_p2, y_dat_p2, label=par2_data['data_name'], color=stl['color'], ls=stl['ls'],
                            lw=stl['lw'], alpha=stl['alpha'])
    if not args.no_fractions and 'Fractions' in inp:
        frac_data = inp['Fractions']['data']
        frac_x, frac_y = xy_data(frac_data)
        frac_delta = [abs(a - b) for a, b in zip(frac_x, frac_x[1:])]
        # the last fraction has no successor: give it the width of the one before
        if frac_delta:
            frac_delta.append(frac_delta[-1])
        else:
            frac_delta = [0.0] * len(frac_data)
        frac_y_pos = mapper(host.get_ylim()[0], host.get_ylim()[1], 0.015)
        for n, (frac_vol, frac_label) in enumerate(frac_data):
            host.axvline(x=frac_vol, ymin=0.065, ymax=0.0, color='r', linewidth=0.85)
            host.annotate(str(frac_label), xy=(frac_vol + frac_delta[n] * 0.55, frac_y_pos),
                          horizontalalignment='center', verticalalignment='bottom', size=8, rotation=90)
    if inp.inject_vol != 0.0 and not args.no_inject:
        host.axvline(x=0, ymin=0.10, ymax=0.0, color='#FF3292', ls='-', marker='v', markevery=2, linewidth=1.5,
                     alpha=0.85, label='Inject')
    host.set_xlim(plot_x_min, plot_x_max)
    if not args.no_legend:
        host.legend(fontsize=8, fancybox=True, labelspacing=0.4, loc='upper right', numpoints=1)
    host.xaxis.set_minor_locator(AutoMinorLocator())
    host.yaxis.set_minor_locator(AutoMinorLocator())
    if not args.no_title:
        plt.title(fname, loc='left', size=9)
    plot_file = fname[:-4] + "_" + safe_name(inp.run_name) + "_plot." + args.format
    plt.savefig(plot_file, bbox_inches='tight', dpi=args.dpi)
    print("Plot saved to: " + plot_file)
    plt.clf()


def data_writer1(fname, inp):
    '''
    writes sensor/run-data to csv-files

    One file per entry, named <fname>_<run name>_<data name>: curves and annotations
    as .csv (x,y per line), meta-data (method text) as .txt.
    '''
    for i in inp.keys():
        print("Writing: " + inp[i]['data_name'])
        outfile_base = fname[:-4] + "_" + safe_name(inp.run_name + "_" + inp[i]['data_name'])
        # UNICORN 6 event curves have no data_type
        if inp[i].get('data_type') == 'meta':
            data = inp[i]['data']
            data_to_write = data.encode('utf-8')
            ext = '.txt'
            sep = '\t'
            with open(outfile_base + ext, 'wb') as fout:
                fout.write(data_to_write)
        else:
            x_dat, y_dat = xy_data(inp[i]['data'])
            ext = '.csv'
            sep = ','
            with open(outfile_base + ext, 'wb') as fout:
                for x, y in zip(x_dat, y_dat):
                    dp = str(x) + sep + str(y) + str('\r\n')
                    data_to_write = dp.encode('utf-8')
                    fout.write(data_to_write)


def generate_xls(inp, fname):
    '''
    Input = pycorn object
    output = xlsx file

    Writes curves and fractions to one worksheet, two columns (ml, value) per entry,
    to <fname>_<run name>.xlsx.
    '''
    xls_filename = fname[:-4] + "_" + safe_name(inp.run_name) + ".xlsx"
    workbook = xlsxwriter.Workbook(xls_filename)
    worksheet = workbook.add_worksheet()
    # curves (the only entries with a unit) and the fractions
    d_list = [i for i in inp.keys() if 'unit' in inp[i] or inp[i]['data_name'] == 'Fractions']
    for col_pair, i in enumerate(d_list):
        unit = inp[i].get('unit', 'Fraction')
        header1 = (inp[i]['data_name'], '')
        header2 = ('ml', unit or '')
        # build the rows in a new list: the loaded data must stay unchanged
        dat = [header1, header2] + list(inp[i]['data'])
        row = 0
        col = col_pair * 2
        print("Writing: " + i)
        for x_val, y_val in (dat):
            worksheet.write(row, col, x_val)
            worksheet.write(row, col + 1, y_val)
            row += 1
    workbook.close()
    print("Data written to: " + xls_filename)


# plot style per curve, keyed by the first 4 characters of the curve name
styles = {'UV': {'color': '#1919FF', 'lw': 1.6, 'ls': "-", 'alpha': 1.0},
          'UV1_': {'color': '#1919FF', 'lw': 1.6, 'ls': "-", 'alpha': 1.0},
          'UV2_': {'color': '#e51616', 'lw': 1.4, 'ls': "-", 'alpha': 1.0},
          'UV3_': {'color': '#c73de6', 'lw': 1.2, 'ls': "-", 'alpha': 1.0},
          'UV 1': {'color': '#1919FF', 'lw': 1.6, 'ls': "-", 'alpha': 1.0},
          'UV 2': {'color': '#e51616', 'lw': 1.4, 'ls': "-", 'alpha': 1.0},
          'UV 3': {'color': '#c73de6', 'lw': 1.2, 'ls': "-", 'alpha': 1.0},
          'Cond': {'color': '#FF7C29', 'lw': 1.4, 'ls': "-", 'alpha': 0.75},
          'Conc': {'color': '#0F990F', 'lw': 1.0, 'ls': "-", 'alpha': 0.75},
          'Pres': {'color': '#C0CBBA', 'lw': 1.0, 'ls': "-", 'alpha': 0.50},
          'Temp': {'color': '#b29375', 'lw': 1.0, 'ls': "-", 'alpha': 0.75},
          'Inje': {'color': '#d56d9d', 'lw': 1.0, 'ls': "-", 'alpha': 0.75},
          'pH': {'color': '#0C7F7F', 'lw': 1.0, 'ls': "-", 'alpha': 0.75}, }
default_style = {'color': '#555555', 'lw': 1.0, 'ls': "-", 'alpha': 0.75}


def style(name):
    '''
    Plot style for a curve name; curves not in `styles` get `default_style`.
    '''
    return styles.get(name[:4], default_style)


def axis_label(entry):
    '''
    Axis label "<name> (<unit>)" for a curve entry; just the name if it has no unit.
    '''
    if entry.get('unit'):
        return entry['data_name'] + " (" + entry['unit'] + ")"
    return entry['data_name']


class ChromatogramView(OrderedDict):
    '''
    One chromatogram of a UNICORN 6 bundle in the form the functions above expect.

    PcUni6 keeps the res3-like entries of each chromatogram in a dict of their own
    (e.g. fdata['Chrom.1']); this adds the `run_name` and `inject_vol` attributes a
    loaded PcRes3 has, and does what PcRes3 does for -r and -i: curves keep every
    `reduce`-th point, and every volume is relative to the injection `inj_sel`
    selects in `injection_points` ([0.0, 1st injection, ...], as PcRes3). The
    PcUni6 entries are not changed.
    '''

    def __init__(self, entries, run_name, injection_points=(0.0,), reduce=1, inj_sel=-1):
        OrderedDict.__init__(self)
        self.run_name = run_name
        self.injection_points = list(injection_points)
        try:
            self.inject_vol = self.injection_points[inj_sel]
        except IndexError:
            print("\n WARNING - Injection point does not exist! Selected default.\n")
            self.inject_vol = self.injection_points[-1]
        for name, entry in entries.items():
            data = entry['data']
            if 'unit' in entry:  # curves (events have no unit)
                data = data[0::reduce]
            if self.inject_vol != 0.0:
                data = [(x - self.inject_vol, y) for x, y in data]
            self[name] = dict(entry, data=data)


def zip_injection_points(fname):
    '''
    Injection volumes of each chromatogram in a UNICORN 6+ bundle, by chromatogram name,
    in PcRes3's form: 0.0 followed by every injection volume that is not 0. Taken from
    the run's injection table (pycorn.event_tables: events of type Injection), on the
    stored volume axis the PcUni6 curves have (adjust_axes=False).
    '''
    points = OrderedDict()
    for chrom in open_run(fname, adjust_axes=False).chromatograms.values():
        rows = [r for r in chrom.table_rows('injection') if not r['pseudo'] and r['start_volume'] is not None]
        # every original Injection event curve gets rows; use the first one only
        rows = [r for r in rows if r['event_curve'] == rows[0]['event_curve']] if rows else []
        points[chrom.name] = [0.0] + [r['start_volume'] for r in rows if r['start_volume'] != 0.0]
    return points


def chromatograms(fdata):
    '''
    The data sets to extract/plot: the PcRes3 itself, or one ChromatogramView per
    chromatogram of a PcUni6 (reduced and zeroed at the selected injection).
    '''
    if isinstance(fdata, PcRes3):
        return [fdata]
    # load_all_xml() turns each Chrom.N.Xml into fdata['Chrom.N'] and keeps a 'Chrom.N.Xml_dict'
    names = [key[:-len('.Xml_dict')] for key in fdata if key.endswith('.Xml_dict')]
    views = []
    for name in names:
        if name not in fdata:
            continue
        points = [0.0] if args.inject == 0 else fdata.injection_points.get(name)
        if points is None:
            print("Note: " + name + ": injections not read; volumes as stored")
            points = [0.0]
        elif len(points) == 1 and args.inject != 0:
            print("Note: " + name + ": no injection found; volumes as stored (from method start)")
        views.append(ChromatogramView(fdata[name], name, points, reduce=args.reduce, inj_sel=args.inject))
    return views


def main2():
    '''
    Load each input file (by extension: .zip or .res) and run the requested actions.
    '''
    for fname in args.inp_res:
        if args.inject == None:
            args.inject = -1
        if (fname[-3:]).lower() == "zip":
            fdata = PcUni6(fname)
            fdata.load()
            fdata.load_all_xml()
            fdata.clean_up()
            # PcUni6 neither reduces nor zeroes at an injection: chromatograms() does, with
            # the injections from the structured reader (not needed for -i 0)
            fdata.injection_points = {}
            if args.inject != 0:
                try:
                    fdata.injection_points = zip_injection_points(fname)
                except Exception as e:
                    print("Warning: could not read the injections of " + fname + " (" + str(e) + ")")
        elif (fname[-3:]).lower() == "res":
            fdata = PcRes3(fname, reduce=args.reduce, inj_sel=args.inject)
            fdata.load()
        else:
            print("Skipping " + fname + ": not a .res or .zip file")
            continue
        chroms = chromatograms(fdata)
        for chrom in chroms:
            if args.extract == 'csv':
                data_writer1(fname, chrom)
            if args.extract == 'xlsx' and xlsx == True:
                generate_xls(chrom, fname)
        # header, injection point and user name options read the binary .res layout
        res_only = [opt for opt, used in (("-c", args.check), ("-n", args.info),
                                          ("-t", args.points), ("-u", args.user)) if used]
        if res_only and not isinstance(fdata, PcRes3):
            print("Options " + ", ".join(res_only) + " are only supported for .res files")
        else:
            if args.check:
                fdata.input_check(show=True)
            if args.info:
                fdata.showheader()
            if args.points:
                fdata.inject_det(show=True)
            if args.user:
                user = fdata.get_user()
                print("User: " + user)
        if args.plot and plotting:
            for chrom in chroms:
                plotterX(chrom, fname)


main2()
