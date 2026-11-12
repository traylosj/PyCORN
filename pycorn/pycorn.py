# -*- coding: utf-8 -*-
"""
PyCORN - script to extract data from .res (results) files generated
by UNICORN Chromatography software supplied with ÄKTA Systems
(c)2014-2016 - Yasar L. Ahmed
v0.18b
"""
import codecs
import io
import os
import struct
import traceback
import warnings
from collections import OrderedDict
from xml.etree import ElementTree
from zipfile import ZipFile
from zipfile import is_zipfile

import xmltodict

from .bundle_codecs import NrbfError, decode_nrbf


def return_on_failure(errors=(Exception,), default_value=None):
    """
    Decorator factory: make the wrapped function return `default_value` instead of raising.

    Exceptions matching `errors` are printed with their traceback and swallowed, so one
    undecodable entry does not stop a whole file from loading.

    Parameters
    ----------
    errors : tuple of Exception types, optional
        Exceptions to catch. Default: every `Exception`.
    default_value : optional
        Returned when one of `errors` is raised. Default: None.
    """
    def decorator(f):
        def applicator(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except errors as e:
                print(f'Error {e} on {args, kwargs}')
                traceback.print_exc()
                return default_value

        return applicator

    return decorator


# decorator: print the error and return None instead of raising
try_except_wrapper = return_on_failure(errors=(Exception,), default_value=None)


class PcRes3(OrderedDict):
    """A class for holding the PyCORN/RESv3 data.
    A subclass of `dict`, with the form `data_name`: `data`.

    Reads binary `.res` result files written by UNICORN 3.10 (UNICORN 3-5). The file
    layout is described in `pycorn/docs/RES_files_layout.txt`.

    Usage::

        data = PcRes3("run.res")
        data.load()
        x, y = zip(*data['UV1_280nm']['data'])

    After `load()`, each entry is a dict with the header fields (`magic_id`, `run_name`,
    `data_name`, `d_size`, `off_next`, `adresse`, `off_data`, `d_start`, `d_end`) plus
    `data` and `data_type`:

    - 'curve': `data` is a list of (volume in ml, value) pairs and `unit` is set
    - 'annotation': `data` is a list of (volume in ml, text) pairs (logbook, fractions, injections)
    - 'meta': `data` is a string (method, creation notes)

    Volumes are relative to the selected injection point (see `inj_sel`).

    Parameters
    ----------
    file_name : str
        Path to the `.res` file. The whole file is read into memory.
    reduce : int, optional
        Keep only every n-th point of sensor curves. Default: 1 (keep all).
    inj_sel : int, optional
        Index into `injection_points` of the injection used as zero volume.
        Default: -1 (the last injection).
    """

    # first, some magic numbers
    # 16-byte signature at the start of every UNICORN 3.10 .res file; the rest are the
    # 8-byte ids that precede each header entry and give its data type. The *_id2
    # variants are written when a file has been opened and resaved in UNICORN
    # (the 2nd last byte is incremented by one).
    RES_magic_id = b'\x11\x47\x11\x47\x18\x00\x00\x00\xB0\x02\x00\x00\x20\x6C\x03\x00'
    CNotes_id = b'\x00\x00\x01\x00\x02\x00\x03\x22'
    Methods_id = b'\x00\x00\x01\x00\x02\x00\x01\x02'
    Logbook_id = b'\x00\x00\x01\x00\x04\x00\x48\x04'
    Logbook_id2 = b'\x00\x00\x01\x00\x04\x00\x49\x04'
    SensData_id = b'\x00\x00\x01\x00\x04\x00\x01\x14'
    SensData_id2 = b'\x00\x00\x01\x00\x04\x00\x02\x14'
    Fractions_id = b'\x00\x00\x01\x00\x04\x00\x44\x04'
    Fractions_id2 = b'\x00\x00\x01\x00\x04\x00\x45\x04'
    Inject_id = b'\x00\x00\x01\x00\x04\x00\x46\x04'
    Inject_id2 = b'\x00\x00\x01\x00\x04\x00\x47\x04'
    LogBook_id = b'\x00\x00\x01\x00\x02\x00\x01\x13'  # capital B!

    def __init__(self, file_name, reduce=1, inj_sel=-1):
        OrderedDict.__init__(self)
        self.file_name = file_name
        self.reduce = reduce
        # volumes (ml) of the injection marks, always starting with 0.0; set by inject_det()
        self.injection_points = None
        self.inj_sel = inj_sel
        # volume of the selected injection, subtracted from every x value; set by load()
        self.inject_vol = None
        self.header_read = False
        self.run_name = ''

        with open(self.file_name, 'rb') as f:
            self.raw_data = f.read()

    def input_check(self, show=False):
        """
        Checks if input file is a supported res file
        x = magic number, y = version string, z = file size/EOF

        Returns True or False

        Parameters
        ----------
        show : bool, optional
            Print the result of each check.
        """
        if show:
            print(f" ---- \n Input file: {self.file_name}")

        # magic number at byte 0, version string at byte 24, recorded file size at bytes 16-19
        x = self.raw_data.find(self.RES_magic_id, 0, 16)
        y = self.raw_data.find(b'UNICORN 3.10', 16, 36)
        z = struct.unpack("i", self.raw_data[16:20])

        if (x, y) == (0, 24):
            if show:
                print(" Input is a UNICORN 3.10 file!")
            x, y = (0, 0)
        else:
            if show:
                print(" Input is not a UNICORN 3.10 file!")
            x, y = (1, 1)

        if z[0] == os.path.getsize(self.file_name):
            if show:
                print(" File size check - OK")
            z = 0
        else:
            if show:
                print(" File size mismatch - file corrupted?")
            z = 1
        if (x, y, z) != (0, 0, 0):
            if show:
                print("\n File not supported - stop!")
            return False
        else:
            if show:
                print("\n Alles safe! - Go go go!")
            return True

    def readheader(self):
        """
        Extracts all the entries/declarations in the header (starts at position 686)

        Each entry becomes a dict stored under its data name; the data itself is read
        later by `load()`. The header ends with the `LogBook` entry.
        """

        # we only need to do this once
        if self.header_read:
            return
        self.header_read = True

        fread = self.raw_data
        header_end = fread.find(self.LogBook_id) + 342
        # entries are 344 bytes apart; the first 320 hold:
        # 8-byte magic id, 296-byte label, then 4 ints: block size, offset to next block,
        # block address, offset from the block address to the data
        for i in range(686, header_end, 344):
            decl = struct.unpack("8s296s4i", fread[i:i + 320])
            full_label = codecs.decode(decl[1], 'iso8859-1').rstrip("\x00")
            # labels look like "<run name>:<n>_<data name>"; entries such as CONFIG have no run name
            if full_label.find(':') == -1:
                r_name = ''
                d_name = full_label
            else:
                r_name = full_label[:full_label.find(':')]
                d_name = full_label[full_label.find('_') + 1:]
            x = dict(magic_id=decl[0],
                     run_name=r_name,
                     data_name=d_name,
                     d_size=decl[2],
                     off_next=decl[3],
                     adresse=decl[4],
                     off_data=decl[5],
                     d_start=decl[4] + decl[5],
                     d_end=decl[4] + decl[2])
            name = x['data_name']
            dat = self.get(name, dict())
            dat.update(x)
            self[name] = dat

    def showheader(self, full=True):
        """
        Prints content of header

        Parameters
        ----------
        full : bool, optional
            Also print the magic id of each entry. Default: True.
        """
        print(f" ---- \n Header of {self.file_name}: \n")
        if full:
            print("  MAGIC_ID, ENTRY_NAME, BLOCK_SIZE, OFFSET_TO_NEXT, ADRESSE, OFFSET_TO_DATA")
        else:
            print(" ENTRY_NAME, BLOCK_SIZE, OFFSET_TO_NEXT, ADRESSE, OFFSET_TO_DATA")
        num_blocks = len(self.items())
        for i in range(num_blocks):
            dtp = (list(self.items()))[i][1]
            if full:
                print(" ", dtp['magic_id'], dtp['data_name'], dtp['d_size'], dtp['off_next'], dtp['adresse'],
                      dtp['off_data'])
            else:
                print(" ", dtp['data_name'], dtp['d_size'], dtp['off_next'], dtp['adresse'], dtp['off_data'])

    def get_user(self):
        """
        Show stored user name

        Returns the user name stored at byte 118 (up to 40 characters); ÄKTA Prime
        systems store "prime".
        """
        fread = self.raw_data[:512]
        u = struct.unpack("40s", fread[118:158])
        dec_u = codecs.decode(u[0], 'iso8859-1').rstrip("\x00")
        return dec_u

    def dataextractor(self, dat, show=False):
        """
        Identify data type by comparing magic id, then run appropriate
        function to extract data, update orig. dict to include new data

        Parameters
        ----------
        dat : dict
            A header entry from `readheader()`.
        show : bool, optional
            Print the name of each entry as it is read.

        Returns
        -------
        dict or None
            `dat` with `data` and `data_type` added, or None for empty entries and
            types that are not supported (the caller then drops the entry).
        """
        meta1 = [
            self.Logbook_id, self.Logbook_id2,
            self.Inject_id, self.Inject_id2,
            self.Fractions_id, self.Fractions_id2]
        meta2 = [self.CNotes_id, self.Methods_id]
        sensor = [self.SensData_id, self.SensData_id2]
        if dat['d_size'] == 0:
            pass
        elif dat['magic_id'] in meta1:
            dat.update(data=self.meta1_read(dat, show=show), data_type='annotation')
            return dat
        elif dat['magic_id'] in meta2:
            dat.update(data=self.meta2_read(dat, show=show), data_type='meta')
            return dat
        elif dat['magic_id'] in sensor:
            values, unit = self.sensor_read(dat, show=show)
            dat.update(data=values, unit=unit, data_type='curve')
            return dat

    def meta1_read(self, dat, show=False, do_it_for_inj_det=False):
        """
        Extracts meta-data/type1, Logbook, fractions and Inject marks
        for a specific datum

        Records are 180 bytes apart: a double (time, not used), a double (volume in ml)
        and a text label.

        Parameters
        ----------
        dat : dict
            A header entry from `readheader()`.
        show : bool, optional
            Print the name of the entry.
        do_it_for_inj_det : bool, optional
            Return raw volumes, without subtracting the injection volume. Used by
            `inject_det()`, which runs before the injection volume is known.

        Returns
        -------
        list of (float, str)
            (volume in ml, label) pairs.
        """
        if show:
            print(f" Reading: {dat['data_name']}")
        final_data = []
        inj_vol_to_subtract = self.inject_vol
        if do_it_for_inj_det:
            inj_vol_to_subtract = 0.0
        for i in range(dat['d_start'], dat['d_end'], 180):
            dp = struct.unpack("dd158s", self.raw_data[i:i + 174])
            # acc_time = dp[0] # not used atm
            acc_volume = round(dp[1] - inj_vol_to_subtract, 4)
            label = (codecs.decode(dp[2], 'iso8859-1')).rstrip('\x00')
            merged_data = acc_volume, label
            final_data.append(merged_data)
        return final_data

    def meta2_read(self, dat, show=False):
        """
        Extracts meta-data/type2, Method/Program used in the run

        Returns the block as a string with Windows (CRLF) line endings.
        """
        if show:
            print(" Reading: {0}".format(dat['data_name']))
        start, size = dat['d_start'], dat['d_size']
        tmp_data = self.raw_data[start:start + size]
        size = tmp_data.rfind(b'\n')  # declared block-size in header is always off
        # by a few bytes, hence it is redetermined here
        if show and size != len(tmp_data):
            print('meta2: reevaluated size {} -> {}'.format(size, len(tmp_data)))

        raw_data = codecs.decode(self.raw_data[start:start + size], 'iso8859-1')
        if '\r' in raw_data:
            data = raw_data
        else:
            data = raw_data.replace('\n', '\r\n')
        return data

    def sensor_read(self, dat, show=False):
        """
        extracts sensor/run-data and applies correct division

        Points are stored as pairs of 4-byte ints: volume in 1/100 ml and the sensor
        value times a factor that depends on the sensor (1000 for UV, Cond and Flow,
        100 for pressure, 10 for everything else).

        Returns
        -------
        tuple
            (list of (volume in ml, value) pairs, unit string). Only every
            `self.reduce`-th point is kept.
        """
        s_unit_dec = None
        final_data = []
        # stored integers are the value times this divisor
        if "UV" in dat['data_name'] or "Cond" == dat['data_name'] or "Flow" == dat['data_name']:
            sensor_div = 1000.0
        elif "Pressure" in dat['data_name']:
            sensor_div = 100.0
        else:
            sensor_div = 10.0
        if show:
            print(" Reading: {0}".format(dat['data_name']))

        fread = self.raw_data
        # the unit is a 15-byte string 207 bytes into the block's metadata (the loop runs once)
        for i in range(dat['adresse'] + 207, dat['adresse'] + 222, 15):
            s_unit = struct.unpack("15s", fread[i:i + 15])
            s_unit_dec = (codecs.decode(s_unit[0], 'iso8859-1')).rstrip('\x00')
            # FIX: in some files the unit for temperature reads 'C' instead of '°C'
            if s_unit_dec == 'C':
                s_unit_dec = u'°C'
        for i in range(dat['d_start'], dat['d_end'], 8):
            sread = struct.unpack("ii", fread[i:i + 8])
            data = round((sread[0] / 100.0) - self.inject_vol, 4), sread[1] / sensor_div
            final_data.append(data)
        return final_data[0::self.reduce], s_unit_dec

    def inject_det(self, show=False):
        """
        Finds injection points - required for adjusting retention volume

        Fills `self.injection_points` with 0.0 followed by the volume of every
        non-zero injection mark. Requires `readheader()` to have run.

        Parameters
        ----------
        show : bool, optional
            Print the list of injection points.
        """
        inject_ids = [self.Inject_id, self.Inject_id2]
        injections = []
        if self.injection_points is None:
            self.injection_points = [0.0]
            for i in self.values():
                if i['magic_id'] in inject_ids:
                    injections = self.meta1_read(i, show=show, do_it_for_inj_det=True)
        for i in injections:
            if i[0] != 0.0:
                self.injection_points.append(i[0])
        if show:
            print(" ---- \n Injection points: \n # \t ml")
            for x, y in enumerate(self.injection_points):
                print(" {0} \t {1}".format(x, y))

    def load(self, print_log=False):
        """
        extract all data and store in list

        Reads the header, picks the injection selected by `inj_sel` as zero volume,
        then decodes every entry. Unsupported and empty entries are removed.

        Parameters
        ----------
        print_log : bool, optional
            Print the name of each entry as it is read.
        """
        self.readheader()
        self.run_name = self['Logbook']['run_name']
        self.inject_det()
        try:
            self.inject_vol = self.injection_points[self.inj_sel]
        except IndexError:
            print("\n WARNING - Injection point does not exist! Selected default.\n")
            self.inject_vol = self.injection_points[-1]
        for name, dat in list(self.items()):
            dat = self.dataextractor(dat, show=print_log)
            if dat is not None:
                self[name] = dat
            else:
                del self[name]


class PcUni6(OrderedDict):
    """
    A class for holding the pycorn/RESv6 data
    A subclass of `dict`, with the form `data_name`: `data`.

    Reads the `.zip` result bundles exported by UNICORN 6 and later. Each file in the
    bundle becomes a key; files that are themselves zips (e.g. `Chrom.1_2_True`) become
    dicts of their decoded contents. `load_all_xml()` additionally builds a res3-like
    dict per chromatogram (e.g. `self['Chrom.1']`) so the same code can handle both
    formats. For a structured view of everything in the bundle, use `pycorn.open_run`.

    Parameters
    ----------
    inp_file : str
        Path to the `.zip` bundle.
    """
    # for manual zip-detection
    _zip_magic_start = b'\x50\x4B\x03\x04\x2D\x00\x00\x00\x08'
    _zip_magic_end = b'\x50\x4B\x05\x06\x00\x00\x00\x00'

    # hack to get pycorn-bin to move on: it selects entries by these res3 magic ids,
    # so every UNICORN 6 entry gets magic_id 0 and matches all of them
    _sens_data_id = 0
    _sens_data_id2 = 0
    _fractions_id = 0
    _fractions_id2 = 0

    def __init__(self, inp_file):
        OrderedDict.__init__(self)
        self.file_name = inp_file
        self._inject_vol = 0.0
        self._run_name = 'blank'
        self._date = None
        self._result_xml = None
        self._loaded = False

    def load_all_xml(self, keep_all=False):
        """
        Load all data stored as xml in the res file.

        Parameters
        ----------
        keep_all : bool, optional
            If False (default), only the res3-like chromatogram dicts are kept (see
            `clean_up`). If True, nothing is deleted and the plain XML entries
            (e.g. `Result.xml`) are also parsed into `<name>_dict` keys. For a
            structured view of everything in the bundle, use `pycorn.open_run`.
        """
        if self._loaded is False:
            self.load(print_log=False)
        xml_keys_to_be_parsed = [key for key in self.keys() if (".Xml" in key and "dict" not in key)]
        for key in xml_keys_to_be_parsed:
            self._xml_parse(key, print_log=False)
        if keep_all:
            for key in list(self.keys()):
                if key.endswith(".xml") and isinstance(self[key], bytes):
                    self[key + "_dict"] = self._unpack_xml(self[key], end_index=len(self[key]))
        else:
            self.clean_up()

    @property
    def date(self):
        """Creation date of the result as 'YYYY-MM-DD', from `Result.xml`. Loads the file if needed."""
        if self._date is None:
            result_xml = self.get("Result.xml", self._result_xml)
            if result_xml is None:
                self.load(print_log=False)
                result_xml = self._result_xml
            root = ElementTree.fromstring(result_xml)
            date = root.find(".//Created").text
            self._date = date[:10]

        return self._date

    def load(self, print_log=False):
        """
        zip-files inside the zip-bundle are replaced by dicts, again with dicts with filename:content
        Chrom.#_#_True (=zip-files) files are unpacked from binary to floats by unpacker()
        To access x/y-value of Chrom.1_2:
        udata = PcUni6("mybundle.zip")
        udata.load()
        x = udata['Chrom.1_2_True']['CoordinateData.Volumes']
        y = udata['Chrom.1_2_True']['CoordinateData.Amplitudes']

        Plain `.Xml` entries are also parsed into `<name>_dict` keys.

        Parameters
        ----------
        print_log : bool, optional
            Print which files in the bundle were decoded.
        """
        self._loaded = True
        with open(self.file_name, 'rb') as f:
            input_zip = ZipFile(f)
            zip_data = self._zip2dict(input_zip)
            self.update(zip_data)
            supported_keys = []
            unsupported_keys = []
            # unpack the zips nested inside the bundle; other files stay raw bytes
            for key in zip_data.keys():
                tmp_raw = io.BytesIO(input_zip.read(key))

                tmp_raw = self._strip_nonstandard_zeros(tmp_raw)

                if not is_zipfile(tmp_raw):
                    unsupported_keys.append(key)
                    continue

                x = {key: self._zip2dict(ZipFile(tmp_raw))}

                self.update(x)

                supported_keys.append(key)

            self._result_xml = zip_data.get("Result.xml")
            if print_log:
                print("Loaded " + self.file_name + " into memory")
                print("\n-Supported-")
                for key in supported_keys:
                    print(" " + key)
                print("\n-Not supported-")
                for key in unsupported_keys:
                    print(" " + key)
        # filter out data we can not handle
        keys_with_array_data = []
        keys_with_xml_data = []
        for key in self.keys():
            if "True" in key and "Xml" not in key:
                keys_with_array_data.append(key)
            else:
                keys_with_xml_data.append(key)
        if print_log:
            print("\nFiles to process:")
            for key in keys_with_array_data:
                print(" " + key)

        # decode the nested files: XML into dicts, binary arrays into lists of floats
        self_keys = list(self.keys())
        for key in self_keys:
            data_entry = self[key]
            if "Xml" in key:
                self[key + "_dict"] = self._unpack_xml(data_entry, end_index=len(data_entry))
            elif type(data_entry) is dict:
                self[key] = self._unpack_dict_data(data_entry, key)
            else:
                pass

        if print_log:
            print("Finished decoding x/y-data!")

    def clean_up(self):
        """
        deletes everything and just keeps relevant run-data
        resulting dict is more like res3

        Removes every file listed in `Manifest.xml` and the manifest itself, leaving the
        dicts built by `_xml_parse()` and the `_dict` entries. Safe to call twice.
        """
        if 'Manifest.xml' not in self:
            return  # already cleaned up
        manifest = ElementTree.fromstring(self['Manifest.xml'])
        for i in range(len(manifest)):
            file_name = manifest[i][0].text
            self.pop(file_name, None)
        self.pop('Manifest.xml')

    @try_except_wrapper
    def _unpack_dict_data(self, data_entry, key):
        """
        Decode the files of one nested zip in place.

        `DataType` files hold a plain type name; in array zips (keys containing "True",
        e.g. `Chrom.1_2_True`) the other files are float arrays; elsewhere they are strings.

        Parameters
        ----------
        data_entry : dict
            file name: raw bytes, as returned by `_zip2dict()`.
        key : str
            Name of the nested zip in the bundle.

        Returns
        -------
        dict or None
            `data_entry` with decoded values, or None if decoding failed.
        """
        for sub_key, sub_value in data_entry.items():
            if "DataType" in sub_key:
                # value contains the DataType, so decode from bytes to string and remove the \r\n
                processed_sub_value = sub_value.decode('utf-8').strip("\r\n")
            elif "True" in key and "Xml" not in key:
                processed_sub_value = self._unpacker(sub_value)
            else:
                processed_sub_value = self._unpack_string(sub_value)
            data_entry[sub_key] = processed_sub_value
        return data_entry

    def _strip_nonstandard_zeros(self, tmp_raw):
        """
        Fix the non-standard zip files by stripping out all the null-bytes at the end
        # see https://bugs.python.org/issue24621

        Parameters
        ----------
        tmp_raw : io.BytesIO
            Contents of a file from the bundle.

        Returns
        -------
        io.BytesIO
            The data cut off after the end-of-central-directory record if it starts like
            one of UNICORN's nested zips, otherwise `tmp_raw` unchanged.
        """

        f_header = tmp_raw.read(9)
        if f_header == self._zip_magic_start:
            proper_zip = tmp_raw.getvalue()
            # the end-of-central-directory record is 22 bytes long (no zip comment)
            f_end = proper_zip.rindex(self._zip_magic_end) + 22
            tmp_raw = io.BytesIO(proper_zip[0:f_end])
        return tmp_raw

    @staticmethod
    def _zip2dict(inp):
        """
        input = zip object
        outout = dict with filename:file-object pairs
        """
        mydict = {}
        for i in inp.NameToInfo:
            tmp_dict = {i: inp.read(i)}
            mydict.update(tmp_dict)
        return mydict

    @staticmethod
    def _unpacker(inp):
        """
        input = data block (a .NET BinaryFormatter float array)
        output = list of values
        """
        try:
            return decode_nrbf(inp)
        except NrbfError as e:
            # unknown layout: fall back to the historical fixed offsets, which
            # drop the first 5 and last 12 values of a standard array
            warnings.warn("could not decode array (%s); using legacy offsets" % e)
            inp_trunc = inp[47:-49]
            return [x[0] for x in struct.iter_unpack("<f", inp_trunc)]

    @classmethod
    def _unpack_string(cls, inp):
        """
        input = data block (a .NET BinaryFormatter string)
        output = dict for XML content, None for an empty string, else the string
        """
        try:
            text = decode_nrbf(inp)
        except NrbfError:
            # unknown layout: fall back to slicing from the first '<'
            return None if len(inp) <= 24 else cls._unpack_xml(inp)
        if not text:
            return None
        if not text.lstrip().startswith("<"):
            return text
        return xmltodict.parse(text)

    @staticmethod
    @try_except_wrapper
    def _unpack_xml(inp, start_index=None, end_index=None):
        """
        Parse XML held in a bytes block into a dict (via `xmltodict`).

        Parameters
        ----------
        inp : bytes
            Block containing the XML.
        start_index : int, optional
            Start of the XML. Default: the first '<', skipping any binary prefix.
        end_index : int, optional
            End of the XML (exclusive). Default: -1, which drops the last byte.

        Returns
        -------
        dict or None
            The parsed XML, or None if it could not be parsed.
        """
        if start_index is None:
            start_index = inp.find(b"<")
        if end_index is None:
            end_index = -1

        input_truncated = inp[start_index:end_index]
        input_decoded = input_truncated.decode()
        xml_dict = xmltodict.parse(input_decoded)
        return xml_dict

    @staticmethod
    def _curve_points_file(curve):
        """File name of a curve's point set; the full-resolution one if there are several."""
        point_sets = curve.find('CurvePoints')
        for point_set in point_sets:
            if point_set.findtext('IsFullResolution') == 'true':
                return point_set.findtext('BinaryCurvePointsFileName')
        return point_sets[0].findtext('BinaryCurvePointsFileName')

    def _xml_parse(self, chrom_name, print_log=False):
        """
        Parse parts of the Chrom.1.Xml and create a res3-like dict

        Stores `self['Chrom.1']` (for `Chrom.1.Xml`) holding one entry per event curve
        (original data only) and per curve whose points were decoded by `load()`, in
        the form `PcRes3` uses: `data` is a list of (volume, value or text) pairs.

        Parameters
        ----------
        chrom_name : str, name of the chromatogram to parse
        print_log : bool, optional

        """
        chrom_key = chrom_name.replace(".Xml", "")
        self[chrom_key] = {}
        tree = ElementTree.fromstring(self[chrom_name])
        mc = tree.find('Curves')
        me = tree.find('EventCurves')
        # print(tree.tag)
        # print(tree.attrib)
        # event curves: logbook, fractions, injections, ...
        event_dict = {}
        for i in range(len(me)):
            magic_id = self._sens_data_id
            # e_type = me[i].attrib['EventCurveType']
            e_name = me[i].find('Name').text
            if e_name == 'Fraction':
                e_name = 'Fractions'  # another hack for pycorn-bin
            e_orig = me[i].find('IsOriginalData').text
            e_list = me[i].find('Events')
            e_data = []
            for e in range(len(e_list)):
                e_vol = float(e_list[e].find('EventVolume').text)
                e_txt = e_list[e].find('EventText').text
                e_data.append((e_vol, e_txt))
            if e_orig == "false":
                print("not added - not orig data")
            if e_orig == "true":
                # print("added - orig data")
                x = {'run_name': chrom_name, 'data': e_data, 'data_name': e_name, 'magic_id': magic_id}
                event_dict.update({e_name: x})
        self[chrom_key].update(event_dict)
        # data curves: the points are in the array zip named in the curve's XML
        chrom_dict = {}
        for i in range(len(mc)):
            d_type = mc[i].attrib['CurveDataType']
            d_name = mc[i].find('Name').text
            d_fname = self._curve_points_file(mc[i])
            d_unit = mc[i].find('AmplitudeUnit').text
            magic_id = self._sens_data_id
            try:
                x_dat = self[d_fname]['CoordinateData.Volumes']
                y_dat = self[d_fname]['CoordinateData.Amplitudes']
                zdata = list(zip(x_dat, y_dat))
                if d_name == "UV cell path length":
                    d_name = "xUV cell path length"  # hack to prevent pycorn-bin from picking this up
                x = {'run_name': chrom_name, 'data': zdata, 'unit': d_unit, 'data_name': d_name, 'data_type': d_type,
                     'magic_id': magic_id}
                chrom_dict.update({d_name: x})
            except KeyError as e:
                print("not parsing", e)
                # don't deal with data that does not make sense atm
                # orig2.zip contains UV-blocks that are (edited) copies of
                # original UV-trace but they dont have the volume data
            if print_log:
                print("---")
                print(d_type)
                print(d_name)
                print(d_fname)
                print(d_unit)
        self[chrom_key].update(chrom_dict)
