# Using the pycorn module:

These are the legacy parsers, kept with their original dict output. For new code, use
`pycorn.open_run()` and the `Run` model (`docs/USER_GUIDE.md`); `run.to_res3_dict()` gives
the same dict as `PcUni6` below.

## UNICORN 6 and later (`.zip` exports): `PcUni6`

```python
from pycorn import PcUni6

my_file = PcUni6("samples/sample.zip")
my_file.load_all_xml()
print(list(my_file.keys()))

>>>['Chrom.1.Xml_dict', 'Chrom.1']
```

`load_all_xml()` builds one dict per chromatogram (`Chrom.1`, ...) and drops the raw
bundle entries (`load_all_xml(keep_all=True)` keeps them; `load()` alone gives only the raw
entries, one key per file in the bundle). The chromatogram's keys are the curve and event
names the instrument used, so they differ between systems. On `samples/sample.zip`:

```python
print(list(my_file['Chrom.1'].keys()))

>>>['Fractions', 'Injection', 'Run Log', 'UV 1_280', 'UV 2_295', 'UV 3_0', 'Cond', '% Cond', 'Conc B', 'System flow', ...]
```

Each entry is a dict. Its keys:

- ``data``: a list of (volume, value) pairs for curves, (volume, text) pairs for events
- ``unit``: the unit of a curve (mAU, mS/cm etc.); events have none
- ``run_name``, ``data_name``, ``magic_id``, and ``data_type`` for curves

For example, the unit and first points of a UV curve:

```python
uv = my_file['Chrom.1']['UV 1_280']
uv['unit']
>>>'mAU'
uv['data'][0:2]
>>>[(0.0, -0.0016426085494458675), (0.0, -0.0016426085494458675)]
```

Curve names such as `UV 1_280` are this sample's; look curves up by role with the `Run`
model (`chrom.curves_with_role("uv")`) instead of by name.

## UNICORN 3 to 5 (`.res` files): `PcRes3`

```python
from pycorn import PcRes3

my_res_file = PcRes3("run.res")
my_res_file.load()
print(my_res_file.keys())
```

The keys are the data names in the file's header (for example `Logbook` or `UV1_280nm`;
they differ between files). Each entry is a dict with ``data``, ``data_type`` ('curve',
'annotation' or 'meta'), ``run_name``, and ``unit`` for curves. Volumes are relative to the
selected injection (`inj_sel`, default the last). See the `PcRes3` docstring. No `.res`
file is in this repository, so this part is not checked here.
