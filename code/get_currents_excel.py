"""
Download hourly ocean-current data for the SE Mediterranean (Egypt -> Israel corridor)
from Copernicus Marine and write it to an Excel file.

SETUP (once):
    pip install copernicusmarine pandas openpyxl xarray netcdf4

    Credentials (free account: https://data.marine.copernicus.eu), in ~/.profile:
        export COPERNICUSMARINE_SERVICE_USERNAME="..."
        export COPERNICUSMARINE_SERVICE_PASSWORD="..."
    Alternatively `copernicusmarine login`, which stores them in
    ~/.copernicusmarine/.copernicusmarine-credentials.

RUN:
    python get_currents_excel.py

OUTPUT:
    israel_currents_hourly.xlsx
        Sheet "hourly"  - one row per hour per point: u, v, speed, direction
        Sheet "daily"   - daily means, plus daily northward transport proxy
"""

import os
import pathlib

import numpy as np
import pandas as pd
import copernicusmarine as cm

# Fail fast instead of hanging on the interactive username prompt.
if not (os.environ.get("COPERNICUSMARINE_SERVICE_USERNAME")
        and os.environ.get("COPERNICUSMARINE_SERVICE_PASSWORD")) \
        and not (pathlib.Path.home() / ".copernicusmarine"
                 / ".copernicusmarine-credentials").exists():
    raise SystemExit(
        "No Copernicus Marine credentials found.\n"
        "Set COPERNICUSMARINE_SERVICE_USERNAME / COPERNICUSMARINE_SERVICE_PASSWORD "
        "(remember to restart the IDE after editing ~/.profile), or run "
        "`copernicusmarine login`."
    )

# ----------------------------------------------------------------------
# 1. SETTINGS -- edit these
# ----------------------------------------------------------------------

START = "2025-06-01"
END   = "2025-09-01"

# Stations and their local shore orientation - see coast_points.py / shore_angle.py.
from coast_points import POINTS, SHORE_AZIMUTH_DEG

DEPTH_M = 1.5          # ~surface. Use e.g. 10 or 30 for subsurface (forecast dataset only).

# Hourly currents, 4.2 km (1/24 deg), MED-MFC.
# Reanalysis: 1987-01-01 .. 2026-07-31, SURFACE ONLY (no depth dimension).
DATASET_MY = "cmems_mod_med_phy-cur_my_4.2km_PT1H-m"
MY_END = "2026-07-31"
# Analysis/forecast: rolling window, currently starts 2025-10-06, has depth levels.
DATASET_ANFC = "cmems_mod_med_phy-cur_anfc_4.2km-3D_PT1H-m"
ANFC_START = "2025-10-06"
# Check current coverage with:
#   copernicusmarine describe --product-id MEDSEA_MULTIYEAR_PHY_006_004
#   copernicusmarine describe --product-id MEDSEA_ANALYSISFORECAST_PHY_006_013

if END <= MY_END:
    DATASET, HAS_DEPTH = DATASET_MY, False
elif START >= ANFC_START:
    DATASET, HAS_DEPTH = DATASET_ANFC, True
else:
    raise SystemExit(
        f"{START}..{END} spans the reanalysis/forecast boundary. "
        f"Run it in two chunks: up to {MY_END}, and from {ANFC_START} on."
    )

OUTFILE = "israel_currents_hourly.xlsx"

# ----------------------------------------------------------------------
# 2. DOWNLOAD
# ----------------------------------------------------------------------

frames = []

for name, (lat, lon) in POINTS.items():
    print(f"fetching {name} ({lat}, {lon}) ...")
    kwargs = dict(
        dataset_id=DATASET,
        variables=["uo", "vo"],
        minimum_longitude=lon, maximum_longitude=lon,
        minimum_latitude=lat,  maximum_latitude=lat,
        start_datetime=f"{START}T00:00:00",
        end_datetime=f"{END}T00:00:00",
    )
    if HAS_DEPTH:
        kwargs.update(minimum_depth=DEPTH_M, maximum_depth=DEPTH_M)
    ds = cm.open_dataset(**kwargs)

    df = ds.to_dataframe().reset_index()
    df = df[["time", "uo", "vo"]].dropna()
    if df.empty:
        print(f"  no data for {name} - point may fall on a land/masked grid cell")
    df["station"] = name
    df["lat"] = lat
    df["lon"] = lon
    frames.append(df)

if not frames:
    raise SystemExit("No data returned - check dataset id, dates and login.")

data = pd.concat(frames, ignore_index=True)

# ----------------------------------------------------------------------
# 3. DERIVED QUANTITIES
# ----------------------------------------------------------------------

# uo = eastward component (m/s), vo = northward component (m/s)
data["speed_m_s"] = np.hypot(data["uo"], data["vo"])
data["speed_cm_s"] = data["speed_m_s"] * 100

# Oceanographic convention: direction the water is flowing TOWARD,
# degrees clockwise from true north.
data["direction_deg_toward"] = (np.degrees(np.arctan2(data["uo"], data["vo"])) + 360) % 360

# Rotate into up-coast / cross-shore using each station's OWN shore angle.
# The coast swings from ~073 deg true off Sinai to ~011 deg off Hadera, so one
# fixed angle would measure Sinai's alongshore flow on the wrong axis.
data["shore_azimuth_deg"] = data["station"].map(SHORE_AZIMUTH_DEG)
_az = np.radians(data["shore_azimuth_deg"])
data["upcoast_m_s"] = data["uo"] * np.sin(_az) + data["vo"] * np.cos(_az)
data["crossshore_m_s"] = -data["uo"] * np.cos(_az) + data["vo"] * np.sin(_az)
# positive up-coast = along the shore toward Israel (Egypt -> Gaza -> Israel)
# positive cross-shore = offshore, 90 deg counter-clockwise from up-coast

data["date"] = data["time"].dt.date

# ----------------------------------------------------------------------
# 4. DAILY SUMMARY
# ----------------------------------------------------------------------

daily = (
    data.groupby(["station", "date"])
    .agg(
        mean_speed_cm_s=("speed_cm_s", "mean"),
        max_speed_cm_s=("speed_cm_s", "max"),
        mean_upcoast_cm_s=("upcoast_m_s", lambda s: s.mean() * 100),
        mean_crossshore_cm_s=("crossshore_m_s", lambda s: s.mean() * 100),
        hours_flowing_upcoast=("upcoast_m_s", lambda s: int((s > 0).sum())),
        n_hours=("upcoast_m_s", "size"),
    )
    .reset_index()
)
daily["shore_azimuth_deg"] = daily["station"].map(SHORE_AZIMUTH_DEG)

# Rough displacement a passive particle would travel along the coast in that day, km.
daily["upcoast_travel_km"] = daily["mean_upcoast_cm_s"] / 100 * 86400 / 1000

# ----------------------------------------------------------------------
# 5. WRITE EXCEL
# ----------------------------------------------------------------------

with pd.ExcelWriter(OUTFILE, engine="openpyxl") as xl:
    data.to_excel(xl, sheet_name="hourly", index=False)
    daily.to_excel(xl, sheet_name="daily", index=False)

print(f"\nWrote {OUTFILE}: {len(data)} hourly rows, {len(daily)} daily rows.")
