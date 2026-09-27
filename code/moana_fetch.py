"""PACE MOANA maps for the eastern Mediterranean, to judge whether they are worth a page.

MOANA is the one satellite product that counts cyanobacteria as cyanobacteria.
PACE's hyperspectral sensor is fitted against cell abundances of three groups:

    prococcus   Prochlorococcus, cells/ml   - the smallest cyanobacterium
    syncoccus   Synechococcus, cells/ml     - the phycocyanin/phycoerythrin one
    picoeuk     picoeukaryotes, cells/ml    - the non-cyanobacterial picoplankton

That is a different thing from the Copernicus "prokaryote" map, which splits total
chlorophyll between plankton types with a model and therefore looks like the
chlorophyll map. Chlorophyll from the same satellite, same grid and same day is
downloaded beside it so the two can be laid side by side: the question the maps
have to answer is when a bloom is cyanobacteria and when it is everything else.

    python code/moana_fetch.py                      # 2026-08-05 and 2026-08-20
    python code/moana_fetch.py 2026-08-05 2026-09-01

Downloads need a free Earthdata Login account (register once at
https://urs.earthdata.nasa.gov/users/new), because the archive allows an
anonymous HEAD but not an anonymous GET. Put the credentials in ~/.netrc:

    machine urs.earthdata.nasa.gov login YOURNAME password YOURPASSWORD

    chmod 600 ~/.netrc

or set EARTHDATA_USER and EARTHDATA_PASS in the environment. Each global daily
map is about 25 MB; they are cached outside the repository and only the cut-out
over our own coast is kept, as a picture and a table.
"""
import netrc
import os
import sys
import urllib.request
import json

import requests

import numpy as np
import xarray as xr
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

CMR = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
# MOANA carries the cell counts; BGC carries chlorophyll on the same grid. Each
# has a near-real-time collection, published about a day after the overpass, and
# a standard one that follows about a month later.
COLLECTIONS = {
    "moana": ["PACE_OCI_L4M_MOANA", "PACE_OCI_L4M_MOANA_NRT"],
    "chl": ["PACE_OCI_L3M_BGC", "PACE_OCI_L3M_BGC_NRT"],
}
RES = "4km"
CACHE = os.path.expanduser("~/.cache/pace")
OUT = "moana_maps"                     # pictures land here, outside data/ and docs/
# The coast this project is about, from the Nile delta to the Lebanese border.
BOX = dict(lat=slice(33.6, 30.5), lon=slice(29.5, 35.8))
DATES = ["2026-08-05", "2026-08-20"]

VARS = [
    ("syncoccus", "Synechococcus", "cells/ml", 1e3, 3e5, "viridis"),
    ("prococcus", "Prochlorococcus", "cells/ml", 1e3, 3e5, "viridis"),
    ("picoeuk", "picoeukaryotes", "cells/ml", 1e2, 3e4, "viridis"),
    ("chlor_a", "chlorophyll a", "mg m-3", 0.02, 3.0, "YlGnBu_r"),
]


def granule_url(kind, day):
    """The download link for one day, preferring the standard product."""
    for short_name in COLLECTIONS[kind]:
        url = (f"{CMR}?short_name={short_name}&temporal={day}T00:00:00Z,{day}T23:59:59Z"
               f"&page_size=20")
        try:
            items = json.load(urllib.request.urlopen(url, timeout=120))["items"]
        except Exception as e:
            print(f"  CMR search failed for {short_name}: {e}")
            continue
        for item in items:
            umm = item["umm"]
            if day.replace("-", "") not in umm["GranuleUR"] or RES not in umm["GranuleUR"]:
                continue
            for rel in umm.get("RelatedUrls", []):
                if rel.get("Type") == "GET DATA" and rel["URL"].endswith(".nc"):
                    return umm["GranuleUR"], rel["URL"]
    return None, None


class EarthdataSession(requests.Session):
    """Keeps the credentials across Earthdata's login redirect.

    The archive bounces a download through urs.earthdata.nasa.gov and on to a
    signed CloudFront link. requests drops the auth header when the host
    changes, which is right in general and wrong here, so it is put back for
    NASA's own hosts only."""

    def __init__(self, auth):
        super().__init__()
        self.auth = auth

    def rebuild_auth(self, prepared, response):
        if "Authorization" in prepared.headers:
            host = requests.utils.urlparse(prepared.url).hostname or ""
            if not (host.endswith("earthdata.nasa.gov") or host.endswith("nasa.gov")):
                del prepared.headers["Authorization"]


def credentials():
    """(user, password) from the environment or ~/.netrc, or None."""
    user, password = os.environ.get("EARTHDATA_USER"), os.environ.get("EARTHDATA_PASS")
    if user and password:
        return user, password
    try:
        found = netrc.netrc().authenticators("urs.earthdata.nasa.gov")
        if found:
            return found[0], found[2]
    except Exception:
        pass
    return None


def fetch(kind, day, session):
    """The day's file, from the cache when it is already there."""
    name, url = granule_url(kind, day)
    if not url:
        print(f"  no {kind} granule for {day}")
        return None
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, name)
    if os.path.exists(path) and os.path.getsize(path) > 1e6:
        return path
    print(f"  downloading {name}")
    r = session.get(url, stream=True, timeout=600, allow_redirects=True)
    if r.status_code != 200:
        print(f"  {r.status_code} for {name} - check the Earthdata credentials")
        return None
    with open(path, "wb") as f:
        for chunk in r.iter_content(1 << 20):
            f.write(chunk)
    return path


def panel(ax, da, title, unit, vmin, vmax, cmap):
    """One map of one variable over the box, on a log scale - these span decades."""
    values = da.values
    ok = np.isfinite(values)
    if not ok.any():
        ax.text(0.5, 0.5, "no data", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(title, fontsize=10)
        ax.set_xticks([]); ax.set_yticks([])
        return
    im = ax.pcolormesh(da.lon, da.lat, values, shading="auto", cmap=cmap,
                       norm=LogNorm(vmin=vmin, vmax=vmax))
    ax.set_title(f"{title}\n{100 * ok.mean():.0f}% of the box measured", fontsize=10)
    ax.set_xlabel("lon"); ax.set_ylabel("lat")
    plt.colorbar(im, ax=ax, shrink=0.85, label=unit)


def main():
    days = sys.argv[1:] or DATES
    creds = credentials()
    if not creds:
        sys.exit("No Earthdata credentials. Register once at "
                 "https://urs.earthdata.nasa.gov/users/new and put them in ~/.netrc "
                 "as:  machine urs.earthdata.nasa.gov login NAME password PASSWORD "
                 "(or set EARTHDATA_USER and EARTHDATA_PASS).")
    session = EarthdataSession(creds)
    os.makedirs(OUT, exist_ok=True)
    summary = []
    for day in days:
        print(day)
        paths = {kind: fetch(kind, day, session) for kind in COLLECTIONS}
        if not paths["moana"]:
            continue
        data = {}
        with xr.open_dataset(paths["moana"], engine="h5netcdf") as ds:
            for name in ("syncoccus", "prococcus", "picoeuk"):
                if name in ds:
                    data[name] = ds[name].sel(**BOX).load()
        if paths["chl"]:
            with xr.open_dataset(paths["chl"], engine="h5netcdf") as ds:
                if "chlor_a" in ds:
                    data["chlor_a"] = ds["chlor_a"].sel(**BOX).load()

        fig, axes = plt.subplots(2, 2, figsize=(13, 10))
        for ax, (name, title, unit, vmin, vmax, cmap) in zip(axes.ravel(), VARS):
            if name in data:
                panel(ax, data[name], f"{title}  {day}", unit, vmin, vmax, cmap)
            else:
                ax.set_visible(False)
        fig.suptitle(f"PACE OCI, {day} — cyanobacteria counts beside chlorophyll", y=0.98)
        fig.tight_layout()
        png = os.path.join(OUT, f"pace_{day}.png")
        fig.savefig(png, dpi=110)
        plt.close(fig)
        print(f"  {png}")

        # The same numbers as a table, for looking at rather than eyeballing.
        rows = {}
        for name, da in data.items():
            v = da.values[np.isfinite(da.values)]
            rows[name] = dict(measured_pct=round(100 * np.isfinite(da.values).mean(), 1),
                              median=float(np.median(v)) if v.size else None,
                              p90=float(np.percentile(v, 90)) if v.size else None)
        summary.append({"date": day, "fields": rows})
        for name, r in rows.items():
            print(f"    {name:10s} covered {r['measured_pct']:5.1f}%  "
                  f"median {r['median']}  p90 {r['p90']}")

    with open(os.path.join(OUT, "summary.json"), "w") as f:
        json.dump(summary, f, indent=1)


if __name__ == "__main__":
    main()
