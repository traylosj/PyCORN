# -*- coding: utf-8 -*-
"""
Byte-level decoders for UNICORN 6+ result bundles (.zip).

A bundle is a zip whose entries are either plain XML or nested zips. A nested zip
holds pairs of `<name>DataType` (a .NET type name such as `System.String` or
`System.Single[]`) and `<name>` (that value serialized with the .NET
BinaryFormatter, i.e. MS-NRBF).

Only the MS-NRBF records UNICORN writes for these values are supported: a
string (BinaryObjectString), a null, or a single primitive array
(ArraySinglePrimitive). Anything else raises `NrbfError` rather than guessing.
"""
import io
import re
import struct
from zipfile import BadZipFile, ZipFile

# MS-NRBF record types
_SERIALIZED_STREAM_HEADER = 0x00
_BINARY_OBJECT_STRING = 0x06
_OBJECT_NULL = 0x0A
_MESSAGE_END = 0x0B
_ARRAY_SINGLE_PRIMITIVE = 0x0F

# MS-NRBF PrimitiveTypeEnumeration -> struct format (fixed-size types only)
_PRIMITIVE_FORMATS = {
    1: "?",   # Boolean
    2: "B",   # Byte
    6: "d",   # Double
    7: "h",   # Int16
    8: "i",   # Int32
    9: "q",   # Int64
    10: "b",  # SByte
    11: "f",  # Single
    12: "q",  # TimeSpan (ticks)
    13: "q",  # DateTime (ticks + kind bits)
    14: "H",  # UInt16
    15: "I",  # UInt32
    16: "Q",  # UInt64
}

_EOCD = b"PK\x05\x06"
_XML_DECLARATION = re.compile(r"^\s*<\?xml[^>]*\?>")


class NrbfError(ValueError):
    """The bytes are not an MS-NRBF value of a supported shape."""


def _read_length_prefixed_string(data, pos):
    """Read an MS-NRBF LengthPrefixedString (7-bit varint length + UTF-8)."""
    length = 0
    shift = 0
    for _ in range(5):
        if pos >= len(data):
            raise NrbfError("truncated string length")
        byte = data[pos]
        pos += 1
        length |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            break
    else:
        raise NrbfError("string length prefix longer than 5 bytes")
    end = pos + length
    if end > len(data):
        raise NrbfError("string runs past end of data")
    return data[pos:end].decode("utf-8"), end


def decode_nrbf(data):
    """
    Decode a BinaryFormatter blob holding a single string, null or primitive array.

    Returns
    -------
    str, None, or list (for arrays; Single values come back as Python floats)
    """
    if len(data) < 18 or data[0] != _SERIALIZED_STREAM_HEADER:
        raise NrbfError("missing SerializationHeaderRecord")
    pos = 17
    record = data[pos]
    pos += 1
    if record == _BINARY_OBJECT_STRING:
        value, pos = _read_length_prefixed_string(data, pos + 4)
    elif record == _OBJECT_NULL:
        value = None
    elif record == _ARRAY_SINGLE_PRIMITIVE:
        if pos + 9 > len(data):
            raise NrbfError("truncated array header")
        length, primitive_type = struct.unpack_from("<iB", data, pos + 4)
        pos += 9
        fmt = _PRIMITIVE_FORMATS.get(primitive_type)
        if fmt is None:
            raise NrbfError("unsupported primitive array type %d" % primitive_type)
        size = struct.calcsize("<" + fmt) * length
        if length < 0 or pos + size > len(data):
            raise NrbfError("array runs past end of data")
        value = list(struct.unpack_from("<%d%s" % (length, fmt), data, pos))
        pos += size
    else:
        raise NrbfError("unsupported record type 0x%02X" % record)
    if pos >= len(data) or data[pos] != _MESSAGE_END:
        raise NrbfError("missing MessageEnd record")
    if pos + 1 != len(data):
        raise NrbfError("%d unexpected bytes after MessageEnd" % (len(data) - pos - 1))
    return value


def trim_zip_padding(raw):
    """
    Cut trailing bytes after the end-of-central-directory record.

    UNICORN pads nested zips with null bytes, which older Python versions reject
    (https://bugs.python.org/issue24621).
    """
    end = raw.rfind(_EOCD)
    if end < 0 or end + 22 > len(raw):
        return raw
    comment_length = struct.unpack_from("<H", raw, end + 20)[0]
    return raw[:end + 22 + comment_length]


def read_nested_zip(raw):
    """Return {name: bytes} for a (possibly padded) zip, or None if `raw` is not a zip."""
    if not raw.startswith(b"PK\x03\x04"):
        return None
    try:
        with ZipFile(io.BytesIO(trim_zip_padding(raw))) as inner:
            return {info.filename: inner.read(info) for info in inner.infolist()}
    except BadZipFile:
        return None


def strip_xml_declaration(text):
    """
    Remove a leading `<?xml ...?>` declaration.

    Embedded XML strings often declare `encoding="utf-16"` although they are
    already decoded text, which makes expat reject them.
    """
    return _XML_DECLARATION.sub("", text, count=1)


def decode_text(raw):
    """Decode bytes from a bundle: UTF-8, or UTF-16 when a BOM says so."""
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16")
    return raw.decode("utf-8-sig")
