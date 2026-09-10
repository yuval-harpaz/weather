"""
Shared geography for the SE Mediterranean current analysis.

POINTS               - the sampling stations, south -> north along the corridor.
SHORE_AZIMUTH_DEG    - local shore orientation at each station, degrees clockwise
                       from true north, pointing UP-COAST (Egypt -> Gaza -> Israel).
                       Regenerate with `python shore_angle.py`, which also writes
                       shore_angles.json and cross-checks against model bathymetry.
"""

import json
import pathlib

# (lat, lon). All seven sit on the 32-45 m isobath, so the transect samples the
# same part of the shelf everywhere. The two southern points were originally
# placed at 14 m and 20 m, inside the model's nearshore frictional layer, which
# damped their currents to a third of their neighbours'; they were walked out
# along their own shore-normal until the depth matched the rest.
POINTS = {
    "El_Arish_EG":  (31.269, 33.825),  # NE Sinai, 12 km offshore, 34.5 m
    "Rafah_Gaza":   (31.391, 34.160),  # 9 km offshore, 37.3 m
    "Gaza_City":    (31.55,  34.35),   # 45.1 m
    "Ashkelon_IL":  (31.68,  34.50),   # IOLR monitoring station area, 32.3 m
    "Ashdod_IL":    (31.83,  34.60),   # 33.3 m
    "Tel_Aviv_IL":  (32.10,  34.70),   # 39.8 m
    "Hadera_IL":    (32.47,  34.85),   # IOLR monitoring station area, 40.9 m
}

# Coastline tangent from Natural Earth 10m within 20 km of each station.
# The coast swings from ENE off Sinai to NNE off Israel - a single fixed angle
# for the whole corridor projects Sinai's alongshore flow onto the wrong axis.
SHORE_AZIMUTH_DEG = {
    "El_Arish_EG":  74.8,
    "Rafah_Gaza":   44.7,
    "Gaza_City":    39.1,
    "Ashkelon_IL":  31.1,
    "Ashdod_IL":    24.9,
    "Tel_Aviv_IL":  18.2,
    "Hadera_IL":    11.4,
}

# Additional chlorophyll sites: the Nile's two distributaries (the source term
# for the whole corridor), the Bardawil exchange, and an open-sea control -
# without the control there is no way to tell a local event from a basin-wide one.
# The two Nile seeds sit offshore of the mouths, not on them: the delta
# coastline doubles back on itself, so a point stepped along the local normal
# from the beach lands next to a different branch of the same coastline.
EXTRA_POINTS = {
    "Rosetta_mouth":  (31.55, 30.40),   # ~9 km off the Rosetta mouth, 13 m
    "Damietta_mouth": (31.60, 31.85),   # ~9 km off the Damietta mouth, 15 m
    "Port_Said_EG":   (31.32, 32.32),   # Suez Canal mouth, seeded ~7 km offshore
    "Bardawil_off":   (31.30, 33.10),
    "Open_sea_ref":   (32.50, 33.50),   # no shore-normal; sampled as a point
}

# Two more stations for sea surface temperature only, extending the line north
# past Haifa Bay to the Lebanese border. They are not in the chlorophyll set
# because they carry no transect: an L4 SST analysis is gap-filled and smooth on
# the 20 km scale, so there is no plume to profile, only a point to read. Both
# were stepped ~4 km due west of the beach and land 2.4-3.2 km from the nearest
# land pixel of the 1 km product, in family with the chlorophyll stations.
SST_EXTRA_POINTS = {
    "Haifa_IL":     (32.830, 34.912),   # west of the Carmel headland, open coast
    "Nahariya_IL":  (33.005, 35.047),
}

# Chlorophyll stations sit at a fixed distance off the coast, unlike the current
# stations which are matched to an isobath. Comparing chlorophyll between points
# 3 km and 12 km out compares distance from shore, not place along the coast.
CHL_OFFSHORE_KM = 4.0
POINT_ONLY = {"Open_sea_ref"}           # too far offshore for a shore transect

_CHL_JSON = pathlib.Path(__file__).with_name("chl_points.json")
CHL_POINTS = {}
if _CHL_JSON.exists():                  # written by `python shore_angle.py`
    CHL_POINTS = {k: (v["lat"], v["lon"])
                  for k, v in json.loads(_CHL_JSON.read_text()).items()}

_JSON = pathlib.Path(__file__).with_name("shore_angles.json")
if _JSON.exists():   # a fresh run of shore_angle.py wins over the values above
    SHORE_AZIMUTH_DEG.update(
        {k: v["shore_azimuth_deg"] for k, v in json.loads(_JSON.read_text()).items()}
    )
