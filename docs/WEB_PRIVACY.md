# PyCORN in the Browser: How It Works, and Why Your Data Stays Private

[Back to the README](../README.md) | [Open the page](https://traylosj.github.io/PyCORN/)

[GitHub Pages](https://pages.github.com/) hosts static files only: GitHub sends the page to
your browser like any other web page, and runs no code of its own. There is no PyCORN
server, and nothing receives or stores your files.

- The page loads Python into your browser tab ([Pyodide](https://pyodide.org/)), plus
  numpy, pandas and the plotting library from public CDNs (jsDelivr, cdn.plot.ly). These
  downloads are program code only; they carry nothing of yours. The page checks the
  Pyodide and plotting scripts against fixed hashes (subresource integrity) and refuses
  them if a CDN serves anything else.
- The page also tells the browser, with a Content-Security-Policy, which addresses it may
  contact: the two CDNs above and its own address, nothing else. Your files therefore cannot
  be sent anywhere, even by a mistake in the page: the browser refuses the request.
- "Try a sample" downloads the public sample file from the page's own address; it sends
  nothing of yours.
- When you drop a file, the tab reads it into its own memory and PyCORN reads it there (in a
  web worker: a background part of the same tab, under the same rules). The Excel and csv
  downloads are also made inside the tab and saved straight to your computer.
- Closing the tab clears your files and results. The browser keeps only your page choices
  (language, x axis, plot layouts, table columns, the download panel's settings), nothing from
  your files.
- To check this yourself, disconnect from the network once the page says "Ready". Dropping
  files, viewing runs and downloading all keep working, because nothing is sent anywhere.
  The browser's developer tools (F12, Network tab) show no upload either.

As with any website, GitHub and the CDNs see that a page was visited (address, browser),
never the files you open in it.

## Running it from a shared drive

The page is one file. Build it with `python tools/build_web_page.py` (writes
`build/web/index.html`; the script is [`tools/build_web_page.py`](../tools/build_web_page.py))
and open it in a browser. The first load still needs internet for Python itself.

## First visit

The first visit downloads about 15 MB (measured with an empty cache: Python and its packages
from jsdelivr, the chart library from cdn.plot.ly, and the page itself). The browser normally
keeps these files, so later visits, also after restarting the browser, download almost nothing;
a private window or cleared browser data downloads them again.

## More

- What the page does with a file, in Python: [User guide](USER_GUIDE.md)
- The files and tables behind the downloads: [Export guide](EXPORT_GUIDE.md)
- The CADET-Core download: [CADET guide](CADET_GUIDE.md)
- Questions or problems: [GitHub issues](https://github.com/traylosj/PyCORN/issues)
