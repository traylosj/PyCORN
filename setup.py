#!/usr/bin/env python
import os
import re
import subprocess

from setuptools import setup

HERE = os.path.dirname(os.path.abspath(__file__))
# the version is set once, in pycorn/version.py
with open(os.path.join(HERE, 'pycorn', 'version.py'), encoding='utf-8') as f:
    VERSION = re.search(r'^__version__ = "([^"]+)"', f.read(), re.M).group(1)


def write_build_info():
    """pycorn/_build_info.py with the checkout's commit, so an installed copy can say where it
    came from (`pycorn.version.commit()`). Left as it is outside a checkout (the web-page build
    writes it into its source copy itself)."""
    if not os.path.exists(os.path.join(HERE, '.git')):
        return
    try:
        git = ['git', '-C', HERE]
        head = subprocess.run(git + ['rev-parse', '--short=12', 'HEAD'], capture_output=True, text=True,
                              check=True).stdout.strip()
        dirty = subprocess.run(git + ['status', '--porcelain', '--untracked-files=no'], capture_output=True,
                               text=True, check=True).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return
    with open(os.path.join(HERE, 'pycorn', '_build_info.py'), 'w', encoding='utf-8') as f:
        f.write('# written by setup.py; not tracked\nCOMMIT = %r\n' % (head + ('.dirty' if dirty else '')))


write_build_info()

setup(
    name='pycorn',
    version=VERSION,
    author='R. Jaepel',
    maintainer='traylosj',
    packages=['pycorn', 'pycorn.sources', 'pycorn.exporters', 'pycorn.exporters.cadet'],
    python_requires='>=3.9',
    # pandas: catalogs, tables, variables() and the `pycorn` CLI all need it
    # xlsxwriter: `pycorn export --format xlsx` works out of the box (owner, 2026-11-08)
    install_requires=["xmltodict", "numpy", "pandas>=1.5", "xlsxwriter"],
    # 'processing' is kept so older install commands still work; it adds nothing now
    extras_require={'plotting':  ["matplotlib"], 'xlsx-output': ['xlsxwriter'], "processing": [],
                    "testing": ["pytest"], "database": ["pymssql"],
                    "cadet": ["CADET-Process>=0.12,<0.13"], "cadet-core": ["h5py"]},
    scripts=['examplescripts/pycorn-bin.py'],
    # `pycorn find` / `pycorn export` (also `python -m pycorn`)
    # `pycorn-setup`: write the database .env by answering questions
    entry_points={'console_scripts': ['pycorn=pycorn.cli:main', 'pycorn-setup=pycorn.setup_env:main']},
    platforms=['Linux', 'Windows', 'MacOSX'],
    zip_safe=False,
    classifiers=["Environment :: Console",
                 "Intended Audience :: Science/Research",
                 "Programming Language :: Python",
                 "Programming Language :: Python :: 3",],
    package_data={'pycorn': ['docs/*.*', 'sql/*.sql', 'sql/results/*.sql']},
    license='GPL-2.0-only',
    description='Read UNICORN chromatography runs from result files or the UNICORN database',
    long_description=open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/traylosj/PyCORN',
)
