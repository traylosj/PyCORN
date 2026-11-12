"""Anonymize a UNICORN 6+ result .zip before it is shared or packaged as a sample.

    python tools/anonymize_zip.py IN.zip OUT.zip [--token WORD ...] [--keep-names] [--no-verify]

Values are never printed, only counts, lengths, field names and OK/FAIL. Console output is ASCII.

Where a result names a person, site, instrument or project (sanitize these in any result you
share, with this tool or by hand). Most of them are inside the nested members: every entry other
than Result.xml, Chrom.1.Xml, EvaluationLog.xml and Manifest.xml is a zip holding a .NET
BinaryFormatter string of XML (`Xml`), so a byte search of the outer zip does not see them.

| Field (any XML document)                         | Holds                         | Becomes      |
|--------------------------------------------------|-------------------------------|--------------|
| CreatedBy, LastModifiedBy, Operator              | logins (DOMAIN\\user parts too) | User1, ...   |
| GroupName                                        | access group                  | Group1, ...  |
| SystemName, SystemData System/Name               | instrument name (often serial) | System1, ... |
| ComputerName                                     | control PC host name          | Computer1    |
| *SerialNumber (ICUSerialNumber)                  | instrument serial number      | Serial1      |
| Item name="... serial ..." value= in base64 XML  | module serials (SystemData    | Serial1      |
| (SystemData OperationalStatistics, UVInformation) | OperationalStatistics)        |              |
| *IPAddress (ICUIPAddress; not loopback)          | instrument network address    | 192.0.2.1    |
| BatchId (also "Batch ID:" event text)            | GUID that finds the original  | new GUID     |
| Result.xml Name                                  | result name                   | Run1         |
| FolderPath (Result.xml, MethodDocumentationData) | folders below the root        | Folder1, ... |
| Method/Description (MethodData)                  | method name                   | Method1      |
| ScoutingName                                     | scouting series name          | Scouting1    |
| values of the result-name variable and of        | run and sample names, typed   | Value1, ...  |
| variables named like Name, ID, Sample, Batch,    | per run; shown in Result.xml  |              |
| Lot, Project, User, Operator, Comment, Note      | Keyword2, ScoutingRunData and |              |
|                                                  | Set mark texts                |              |
| "Method Run ..." event text in Chrom.1.Xml       | method, result path at run    | (the above)  |
|                                                  | time, login, computer         |              |

The folder path at run time can differ from Result.xml's (results get moved), so every
FolderPath and the "Method Run" text are read. Element text that is base64 of an XML document
(SystemData's OperationalStatistics, UVInformation, MixerInformation; RunInformation) is
decoded, searched and rewritten like any other document, and checked afterwards. Each value is replaced in every XML document as a
whole token, case-insensitive, UTF-8 and UTF-16-LE; in binary documents only by a same-length
placeholder. Values shorter than 4 characters are replaced only as exact element text
(`>VALUE<`). The built-in accounts Default and System are kept. --token WORD replaces a word
anywhere, also inside longer words (use it for a person's name in method or file names).
--keep-names keeps result, folder, method, scouting and variable-value names.

Kept on purpose: timestamps and time zone (the time axes need them), UNICORN version,
SystemTypeName and firmware (instrument model), column and media names, RunInformation
(base64; column name and volume, read by pycorn.column_volume), curve, chromatogram and event
names, block, phase and variable names.

After a rewrite the BinaryFormatter length prefix is set to the new length and Manifest.xml's
CRCCode is updated for members it stores as CRC32 (Result.xml, EvaluationLog.xml). The other
manifest CRCs use an unknown checksum and are left as they are: pycorn does not check them,
UNICORN may refuse to import the file.

The check afterwards reads every document (nested zips decoded) and confirms that no original
value remains, that each field above holds only a placeholder, lists the XML fields whose text
changed (look for fields you did not expect), lists path and e-mail looking strings (random
binary data gives a few false hits), and, when pycorn imports, that `open_run()` returns the
same curves and events for both files.
"""
import argparse
import base64
import binascii
import io
import os
import re
import sys
import uuid
import zipfile
import zlib
import xml.etree.ElementTree as ET

FIELDS = {  # XML field -> pseudonym prefix
    "CreatedBy": "User",
    "LastModifiedBy": "User",
    "Operator": "User",
    "GroupName": "Group",
    "SystemName": "System",
    "ComputerName": "Computer",
    "ScoutingName": "Scouting",
}
LOGIN_FIELDS = ("CreatedBy", "LastModifiedBy", "Operator")
NAME_PREFIXES = ("Run", "Folder", "Method", "Scouting", "Value")  # kept by --keep-names
BUILTIN = {"default", "system"}
METHOD_WORDS = ("VariableName", "BlockName", "CalledBlockName", "BreakPointVariable", "Keyword1", "PhaseName",
                "InstructionName", "ReadableName", "CurveName", "Name")  # a value equal to one is method text
ID_VARIABLE = re.compile(r"name|(^|[^a-z])id($|[^a-z])|sample|batch|lot|project|user|operator|comment|note",
                         re.IGNORECASE)
METHOD_RUN = re.compile(r"Method Run .*? Method: (.*?) Result: (.*?) User (.*?) at computer (.*?) is in control")
GUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
PSEUDONYM = re.compile(r"(%s)\d+_*" % "|".join(sorted(set(FIELDS.values()) | {"Serial", "Anon"} | set(NAME_PREFIXES))))
SERIAL_ITEM = re.compile(r"serial", re.IGNORECASE)  # Item name="UV cell serial number" value="..."
IP_PREFIX = "IP"
IP_PSEUDONYM = re.compile(r"192\.0\.2\.\d+")  # TEST-NET-1, for documentation
KEEP_IPS = {"127.0.0.1", "0.0.0.0", "::1"}
B64_TEXT = re.compile(rb">([A-Za-z0-9+/\r\n]{40,}={0,2})<")
MIN_LEN = 4
ENCODINGS = ("utf-8", "utf-16-le")
NRBF_STRING = bytes.fromhex("0001000000ffffffff01000000000000000601000000")  # header record + string record, id 1
PATH_PATTERNS = {
    "unc path": rb"\\\\[A-Za-z0-9_.-]{2,}\\",
    "drive path": rb"[A-Za-z]:\\[A-Za-z]",
    "users path": rb"[\\/]users[\\/]",
    "email": rb"[A-Za-z0-9._-]+@[A-Za-z0-9-]+\.[A-Za-z]{2,}",
}


def _local(el):
    return el.tag.split("}")[-1]


def _text(el):
    return (el.text or "").strip()


def _zip(data):
    try:
        return zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        return None


def _varint(data, pos):
    value, shift = 0, 0
    while True:
        b = data[pos]
        pos += 1
        value |= (b & 0x7F) << shift
        shift += 7
        if not b & 0x80:
            return value, pos


def _to_varint(n):
    out = bytearray()
    while True:
        b, n = n & 0x7F, n >> 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _parse(payload):
    """The XML root of payload, or None when it is not XML."""
    if not payload.lstrip(b"\xef\xbb\xbf \r\n\t").startswith(b"<"):
        return None
    try:
        return ET.fromstring(payload)
    except ET.ParseError:
        pass
    try:  # UTF-8 text that declares utf-16 (SystemData's base64 OperationalStatistics)
        return ET.fromstring(re.sub(rb"^\s*<\?xml[^>]*\?>", b"", payload))
    except ET.ParseError:
        return None


def _split(data):
    """(kind, payload, join): kind 'xml' or 'bin'; join(new_payload) rebuilds the document bytes."""
    if data.startswith(NRBF_STRING) and len(data) > len(NRBF_STRING):
        n, pos = _varint(data, len(NRBF_STRING))
        payload, tail = data[pos:pos + n], data[pos + n:]
        if _parse(payload) is not None:
            return "xml", payload, lambda new: NRBF_STRING + _to_varint(len(new)) + new + tail
    if _parse(data) is not None:
        return "xml", data, lambda new: new
    return "bin", data, lambda new: new


def _b64_xml(text):
    """The XML document base64 `text` (bytes, line breaks allowed) holds, as bytes; else None."""
    try:
        raw = base64.b64decode(re.sub(rb"\s+", b"", text), validate=True)
    except (binascii.Error, ValueError):
        return None
    return raw if _parse(raw) is not None else None


def _b64_blocks(payload):
    """[(match, decoded XML bytes)] for every element text of `payload` that is base64 of XML."""
    out = []
    for m in B64_TEXT.finditer(payload):
        decoded = _b64_xml(m.group(1))
        if decoded is not None:
            out.append((m, decoded))
    return out


def _rewrite_b64(payload, fn):
    """`payload` with each base64 XML block decoded, passed through fn(decoded) and re-encoded as stored."""
    parts, last = [], 0
    for m, decoded in _b64_blocks(payload):
        new = fn(decoded)
        if new == decoded:
            continue
        text = m.group(1)
        sep = b"\r\n" if b"\r\n" in text else b"\n" if b"\n" in text else b""
        width = len(text.split(sep)[0]) if sep else 0
        flat = base64.b64encode(new)
        if width:
            flat = sep.join(flat[i:i + width] for i in range(0, len(flat), width))
        parts += [payload[last:m.start(1)], flat]
        last = m.end(1)
    return b"".join(parts) + payload[last:] if parts else payload


def documents(data, path=""):
    """
    Every leaf document of a zip (nested zips opened): list of (path, kind, payload). Base64 XML
    blocks inside an XML document follow it as ('<path>#<n>', 'xml', decoded bytes).
    """
    z = _zip(data) if path == "" or not path.lower().endswith(".xml") else None
    if z is None:
        kind, payload, _ = _split(data)
        out = [(path, kind, payload)]
        if kind == "xml":
            out += [("%s#%d" % (path, i), "xml", decoded) for i, (_, decoded) in enumerate(_b64_blocks(payload), 1)]
        return out
    out = []
    for info in z.infolist():
        out += documents(z.read(info.filename), path + "/" + info.filename)
    return out


def rewrite(data, fn, path=""):
    """data with every leaf document passed through fn(path, kind, payload) -> payload."""
    z = _zip(data) if path == "" or not path.lower().endswith(".xml") else None
    if z is None:
        kind, payload, join = _split(data)
        new = fn(path, kind, payload)
        return data if new == payload else join(new)
    parts = [(info, z.read(info.filename)) for info in z.infolist()]
    new_parts = [(info, rewrite(raw, fn, path + "/" + info.filename)) for info, raw in parts]
    if all(a[1] == b[1] for a, b in zip(parts, new_parts)):
        return data  # unchanged: keep the original bytes
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zout:
        for info, raw in new_parts:
            out = zipfile.ZipInfo(info.filename, info.date_time)
            out.compress_type = info.compress_type
            out.external_attr = info.external_attr
            zout.writestr(out, raw)
    return buf.getvalue() + _padding(data)


def _padding(data):
    """Bytes after the end-of-central-directory record (UNICORN pads nested zips with nulls)."""
    end = data.rfind(b"PK\x05\x06")
    if end < 0 or len(data) < end + 22:
        return b""
    return data[end + 22 + int.from_bytes(data[end + 20:end + 22], "little"):]


def harvest(docs, project_names=True):
    """Return (whole, short, guids): whole/short map value -> prefix, guids is a list of BatchIds."""
    xml_docs = [(p, _parse(payload)) for p, kind, payload in docs if kind == "xml"]
    xml_docs.sort(key=lambda item: item[0] != "/Result.xml")  # Result.xml first: Run1 is its name
    members = {p.rsplit("/", 1)[-1].lower() for p, _, _ in docs}
    vocabulary = {_text(el).lower() for _, root in xml_docs for el in root.iter() if _local(el) in METHOD_WORDS}
    found, guids = {}, []

    def add(value, prefix):
        value = value.strip()
        if not value or value.lower() in BUILTIN or value.lower() in members or PSEUDONYM.fullmatch(value):
            return
        if prefix == IP_PREFIX and IP_PSEUDONYM.fullmatch(value):
            return
        if prefix in NAME_PREFIXES and not project_names:
            return
        if prefix in ("Value", "Folder") and (value.lower() in vocabulary or re.fullmatch(r"[-+0-9.,eE ]+", value)):
            return  # a run called Regen next to a block called Regen: renaming it would rename the block
        found.setdefault(value, prefix)

    def add_path(path):
        segs = [s.strip() for s in path.split("/") if s.strip()][1:]  # keep the root folder
        for i, seg in enumerate(segs):
            add(seg, "Folder")

    id_vars = set()
    for _, root in xml_docs:
        for el in root.iter():
            if _local(el) == "ResultSettingUniqueIdentifier":
                id_vars |= {_text(c) for c in el.iter() if _local(c) == "VariableName" and _text(c)}
            if _local(el) in ("VariableName", "Keyword1") and ID_VARIABLE.search(_text(el)):
                id_vars.add(_text(el))

    for path, root in xml_docs:
        if path == "/Result.xml":
            for child in root:
                if _local(child) == "Name" and _text(child):
                    add(_text(child), "Run")
        if _local(root) == "Method":
            for child in root:
                if _local(child) == "Description" and _text(child):
                    add(_text(child), "Method")
        if _local(root) == "Systems":
            for system in root:
                for child in system:
                    if _local(child) == "Name" and _text(child):
                        add(_text(child), "System")
        for el in root.iter():
            tag, text = _local(el), _text(el)
            if tag in FIELDS and text:
                add(text, FIELDS[tag])
                if tag in LOGIN_FIELDS:
                    for part in re.split(r"[\\/@]+", text):
                        if part != text:
                            add(part, "User")
            elif tag.endswith("SerialNumber") and text:
                add(text, "Serial")
            elif tag.lower().endswith("ipaddress") and text and text not in KEEP_IPS:
                add(text, IP_PREFIX)
            elif tag == "FolderPath" and text:
                add_path(text)
            elif tag == "BatchId" and GUID.fullmatch(text):
                if text.lower() not in (g.lower() for g in guids):
                    guids.append(text)
            elif tag == "EventText":
                m = METHOD_RUN.search(text)
                if m:
                    add(m.group(1), "Method")
                    add_path(m.group(2))
                    add(m.group(3), "User")
                    add(m.group(4), "Computer")
            attrs = {k.lower(): v for k, v in el.attrib.items()}
            if SERIAL_ITEM.search(attrs.get("name", "")) and (attrs.get("value") or "").strip():
                add(attrs["value"], "Serial")
            children = {_local(c): _text(c) for c in el}
            var = children.get("VariableName") or children.get("Keyword1")
            if var in id_vars:
                for key in ("Value", "ReadableValue", "Keyword2", "ParameterValue", "ParameterReadableValue"):
                    if children.get(key) and children[key] != var:
                        add(children[key], "Value")
    whole = {v: p for v, p in found.items() if len(v) >= MIN_LEN}
    short = {v: p for v, p in found.items() if len(v) < MIN_LEN}
    return whole, short, guids


def _pattern(value, enc, whole):
    """Bytes regex for value in this encoding; whole=True: not inside a longer letter/digit run."""
    body = re.escape(value.encode(enc))
    if not whole:
        return body
    unit = b"[A-Za-z0-9]" if enc == "utf-8" else b"[A-Za-z0-9]\x00"
    return b"(?<!" + unit + b")" + body + b"(?!" + unit + b")"


def _same_length(name, length):
    return (name + "_" * length)[:length]


def _crc_text(data):
    """Manifest.xml style: CRC32 as a signed 32-bit number printed as 64-bit hex."""
    crc = zlib.crc32(data)
    return "FFFFFFFF%08X" % crc if crc & 0x80000000 else "%X" % crc


def _fix_manifest(src_bytes, out_bytes):
    """Update Manifest.xml CRCCode entries that were the CRC32 of their member."""
    zs, zo = zipfile.ZipFile(io.BytesIO(src_bytes)), zipfile.ZipFile(io.BytesIO(out_bytes))
    if "Manifest.xml" not in zo.namelist():
        return out_bytes, 0
    manifest, n = zo.read("Manifest.xml"), 0
    for name, crc in re.findall(rb"<FileName>([^<]+)</FileName>\s*<CRCCode>([^<]+)</CRCCode>", manifest):
        member = name.decode("utf-8")
        if member not in zs.namelist() or int(crc, 16) & 0xFFFFFFFF != zlib.crc32(zs.read(member)):
            continue
        new = _crc_text(zo.read(member)).encode()
        if new != crc:
            manifest = manifest.replace(b"<FileName>" + name + b"</FileName>", b"\x00", 1)
            manifest = re.sub(rb"\x00(\s*)<CRCCode>[^<]+</CRCCode>",
                              lambda m: b"<FileName>" + name + b"</FileName>" + m.group(1) + b"<CRCCode>" + new + b"</CRCCode>",
                              manifest, count=1)
            n += 1
    return rewrite(out_bytes, lambda p, k, payload: manifest if p == "/Manifest.xml" else payload), n


def anonymize(src, dst, tokens=(), project_names=True):
    with open(src, "rb") as f:
        src_bytes = f.read()
    docs = documents(src_bytes)
    whole, short, guids = harvest(docs, project_names)
    counters = {}  # go on after the placeholders a file already holds (a second pass)
    for _, kind, payload in docs:
        if kind == "xml":
            for m in re.finditer(PSEUDONYM.pattern.replace(r"\d+", r"(\d+)").encode(), payload.replace(b"\x00", b"")):
                prefix = m.group(1).decode()
                counters[prefix] = max(counters.get(prefix, 0), int(m.group(2)))

    def pseudonyms(found):
        out = {}
        for val, prefix in found.items():  # in order of appearance: Folder1 is the first folder
            counters[prefix] = counters.get(prefix, 0) + 1
            out[val] = ("192.0.2.%d" if prefix == IP_PREFIX else prefix + "%d") % counters[prefix]
        return out

    whole_map, short_map = pseudonyms(whole), pseudonyms(short)
    plan = [(k, v, True) for k, v in whole_map.items()]
    plan += [(g, str(uuid.uuid4()), True) for g in guids]
    plan += [(tok, "Anon%d" % i, False) for i, tok in enumerate(tokens, 1)]
    plan.sort(key=lambda item: len(item[0]), reverse=True)
    kinds = {}
    for prefix in list(whole.values()) + list(short.values()):
        kinds[prefix] = kinds.get(prefix, 0) + 1
    print("replacements: %d values (%s), %d BatchIds, %d tokens; %d of them shorter than %d characters"
          % (len(whole) + len(short), ", ".join("%s %d" % kv for kv in sorted(kinds.items())), len(guids),
             len(tokens), len(short), MIN_LEN))

    hits = {}

    def replace(path, kind, data):
        n_all = 0
        if kind == "xml":  # base64 XML blocks first, as documents of their own
            data = _rewrite_b64(data, lambda decoded: replace(path + "#", "xml", decoded))
        for key, new_text, is_whole in plan:
            if kind == "bin" and len(key) < MIN_LEN:
                continue
            for enc in ENCODINGS:
                new = (new_text if kind == "xml" else _same_length(new_text, len(key))).encode(enc)
                data, n = re.subn(_pattern(key, enc, is_whole), lambda m, new=new: new, data, flags=re.IGNORECASE)
                n_all += n
        if kind == "xml":
            for key, new_text in short_map.items():
                data, n = re.subn(b">" + re.escape(key.encode()) + b"<", b">" + new_text.encode() + b"<", data)
                n_all += n
        if n_all:
            hits[path] = n_all
        return data

    out_bytes = rewrite(src_bytes, replace)
    out_bytes, n_crc = _fix_manifest(src_bytes, out_bytes)
    with open(dst, "wb") as f:
        f.write(out_bytes)
    for member, n in sorted(hits.items()):
        print("  %s: %d replacements" % (member, n))
    print("  Manifest.xml: %d CRC32 entries updated" % n_crc)


def _changed_fields(do, ds):
    """Per XML document: the field names whose text differs between the two zips."""
    out, ds = {}, {p: (k, payload) for p, k, payload in ds}
    for path, kind, payload in do:
        if kind != "xml" or path not in ds or ds[path][0] != "xml":
            continue
        a, b = _parse(payload), _parse(ds[path][1])
        changed = set()
        for x, y in zip(a.iter(), b.iter()):
            if _local(x) != _local(y) or (x.text or "") != (y.text or "") or x.attrib != y.attrib:
                changed.add(_local(x))
        if changed:
            out[path] = sorted(changed)
    return out


def _placeholder_fields(docs):
    """(path, field) pairs where a field from the docstring table holds something not anonymized."""
    bad = []
    for path, kind, payload in docs:
        if kind != "xml":
            continue
        root = _parse(payload)
        for el in root.iter():
            tag, text = _local(el), _text(el)
            attrs = {k.lower(): v for k, v in el.attrib.items()}
            value = (attrs.get("value") or "").strip()
            if SERIAL_ITEM.search(attrs.get("name", "")) and value and not PSEUDONYM.fullmatch(value):
                bad.append((path, "%s %s" % (tag, attrs["name"])))
            if not text:
                continue
            if tag.lower().endswith("ipaddress"):
                if text not in KEEP_IPS and not IP_PSEUDONYM.fullmatch(text):
                    bad.append((path, tag))
            elif tag in FIELDS or tag.endswith("SerialNumber"):
                parts = [text] + (re.split(r"[\\/@]+", text) if tag in LOGIN_FIELDS else [])
                if not all(p.lower() in BUILTIN or PSEUDONYM.fullmatch(p) for p in parts if p):
                    bad.append((path, tag))
            elif tag == "FolderPath":
                segs = [s for s in text.split("/") if s.strip()][1:]
                if not all(PSEUDONYM.fullmatch(s.strip()) for s in segs):
                    bad.append((path, tag))
    return bad


def verify(src, dst, tokens=(), project_names=True):
    """Print the checks; return True when nothing identifying is left and the data is unchanged."""
    with open(src, "rb") as f:
        do = documents(f.read())
    with open(dst, "rb") as f:
        ds = documents(f.read())
    whole, short, guids = harvest(do, project_names)
    checks = [(v, True) for v in list(whole) + guids] + [(v, False) for v in tokens]
    bad = 0
    if [p for p, _, _ in do] != [p for p, _, _ in ds] or [k for _, k, _ in do] != [k for _, k, _ in ds]:
        bad += 1
        print("[FAIL] document names, order or kinds differ")
    for path, kind, data in ds:
        for value, is_whole in checks:
            if kind == "bin" and len(value) < MIN_LEN:
                continue
            for enc in ENCODINGS:
                n = len(re.findall(_pattern(value, enc, is_whole), data, flags=re.IGNORECASE))
                if n:
                    bad += 1
                    print("[FAIL] value of length %d still in %s (%d times)" % (len(value), path, n))
        if kind == "xml":
            for v in short:
                if b">" + v.encode() + b"<" in data:
                    bad += 1
                    print("[FAIL] short value of length %d still in %s" % (len(v), path))
    print("[%s] leftovers: %d values checked in %d documents, %d found"
          % ("OK" if not bad else "FAIL", len(checks) + len(short), len(ds), bad))

    fields = _placeholder_fields(ds) if project_names else []
    for path, tag in fields:
        print("[FAIL] %s in %s holds a value that is not a placeholder" % (tag, path))
    print("[%s] sensitive fields hold placeholders only" % ("OK" if not fields else "FAIL"))
    bad += len(fields)

    for path, changed in sorted(_changed_fields(do, ds).items()):
        print("  changed in %s: %s" % (path, ", ".join(changed)))

    flagged = 0
    for path, kind, data in ds:
        views = (data, data.replace(b"\x00", b""))  # the second view catches UTF-16-LE text
        for label, pat in PATH_PATTERNS.items():
            n = max(len(re.findall(pat, v, flags=re.IGNORECASE)) for v in views)
            if n:
                flagged += 1
                print("  note: %s style string in %s: %d (random binary data gives false hits)" % (label, path, n))
    print("[%s] path/e-mail style strings: %d documents" % ("OK" if not flagged else "CHECK", flagged))

    same = True
    try:
        same = _parity(src, dst)
    except ImportError:
        print("[SKIP] pycorn parity: pycorn or numpy not importable")
    return not bad and same


def _parity(src, dst):
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if repo not in sys.path:
        sys.path.append(repo)  # this checkout, when run with -I
    import numpy as np
    from pycorn import open_run

    def summary(path):
        out = []
        for chrom in open_run(path).chromatograms.values():
            for name, curve in chrom.curves.items():
                frame = curve.to_frame()
                out.append(("curve", name, frame.shape, round(float(np.nansum(frame.select_dtypes("number").to_numpy())), 6)))
            for name, ec in chrom.event_curves.items():
                out.append(("events", name, len(ec.events)))
        return out

    a, b = summary(src), summary(dst)
    print("[%s] pycorn parity: %d items before, %d after" % ("OK" if a == b else "FAIL", len(a), len(b)))
    for x, y in zip(a, b):
        if x != y:
            print("  differs: %s" % (x[:2],))
    return a == b


def main(argv=None):
    ap = argparse.ArgumentParser(description="Anonymize a UNICORN result zip (values are never printed).")
    ap.add_argument("src")
    ap.add_argument("dst")
    ap.add_argument("--token", action="append", default=[], help="extra word to replace everywhere (repeatable)")
    ap.add_argument("--keep-names", action="store_true",
                    help="keep result, folder, method, scouting and variable-value names")
    ap.add_argument("--no-verify", action="store_true", help="skip the checks after writing")
    args = ap.parse_args(argv)
    if os.path.abspath(args.src) == os.path.abspath(args.dst):
        ap.error("dst must differ from src")
    names = not args.keep_names
    anonymize(args.src, args.dst, args.token, names)
    if args.no_verify:
        return 0
    return 0 if verify(args.src, args.dst, args.token, names) else 1


if __name__ == "__main__":
    sys.exit(main())
