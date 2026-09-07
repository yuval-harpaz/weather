"""
Work out the local shore orientation at each station, so "up-the-coast travel"
is measured along the coast that is actually there rather than a single fixed
angle for the whole corridor.

METHOD
    Primary   - tangent of the Natural Earth 10m coastline within RADIUS_KM of
                the station, by total least squares (SVD of the centred
                vertices). This is the shore angle proper.
    Check     - orientation of the local isobaths in the CMEMS model bathymetry
                (perpendicular to a plane fit of depth). Independent of the
                coastline vector data, and it is the geometry the model's own
                currents actually feel.
    Reported  - mean current direction, a third independent view: where the
                water goes on average has to lie near the coast tangent.

RUN:
    python shore_angle.py

OUTPUT:
    shore_angles.json   - picked up automatically by coast_points.py
"""

import json
import pathlib
import urllib.request

import numpy as np
import xarray as xr

from coast_points import POINTS, EXTRA_POINTS, CHL_OFFSHORE_KM, POINT_ONLY

COASTLINE_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/"
    "master/geojson/ne_10m_coastline.geojson"
)
CACHE = pathlib.Path.home() / ".cache" / "weather"
RADIUS_KM = 20.0        # coastline vertices to fit; 10-30 km changes it by <2 deg
BATHY_RADIUS_KM = 12.0
KM_PER_DEG = 111.19
OUTFILE = pathlib.Path(__file__).with_name("shore_angles.json")
CHL_OUT = pathlib.Path(__file__).with_name("chl_points.json")


def local_km(lons, lats, lat0, lon0):
    """Equirectangular km offsets about (lat0, lon0) - fine over tens of km."""
    x = (np.asarray(lons) - lon0) * KM_PER_DEG * np.cos(np.radians(lat0))
    y = (np.asarray(lats) - lat0) * KM_PER_DEG
    return x, y


def coastline_vertices():
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / "ne_10m_coastline.geojson"
    if not path.exists():
        print(f"downloading {COASTLINE_URL} ...")
        urllib.request.urlretrieve(COASTLINE_URL, path)
    # Box must span every site we might measure, the Nile mouths included -
    # a vertex set clipped to the Israeli stations puts Rosetta 187 km "offshore".
    sites = {**POINTS, **EXTRA_POINTS}
    lats = [p[0] for p in sites.values()]
    lons = [p[1] for p in sites.values()]
    box = (min(lons) - 1.5, max(lons) + 1.5, min(lats) - 1.5, max(lats) + 1.5)

    out = []
    for feat in json.loads(path.read_text())["features"]:
        geom = feat["geometry"]
        lines = (geom["coordinates"] if geom["type"] == "MultiLineString"
                 else [geom["coordinates"]])
        for line in lines:
            a = np.asarray(line, dtype=float)
            m = ((a[:, 0] > box[0]) & (a[:, 0] < box[1])
                 & (a[:, 1] > box[2]) & (a[:, 1] < box[3]))
            if m.any():
                out.append(a[m])
    return np.vstack(out)


def tangent_azimuth(x, y):
    """Azimuth (deg CW from north, mod 180) of the best-fit line through points."""
    pts = np.column_stack([x, y])
    pts = pts - pts.mean(axis=0)
    principal = np.linalg.svd(pts, full_matrices=False)[2][0]
    return np.degrees(np.arctan2(principal[0], principal[1])) % 180.0


MAX_SEGMENT_KM = 30.0    # longer "segments" join separate coastline pieces


def _foot_of_perpendicular(px, py, cx, cy):
    """Closest point on the coastline polyline to local-frame point (px, py)."""
    p = np.column_stack([cx, cy])
    a, b = p[:-1], p[1:]
    ab = b - a
    seg2 = (ab ** 2).sum(1)
    q = np.array([px, py])
    t = np.clip(((q - a) * ab).sum(1) / np.where(seg2 == 0, 1.0, seg2), 0.0, 1.0)
    foot = a + ab * t[:, None]
    dist = np.hypot(foot[:, 0] - px, foot[:, 1] - py)
    dist = np.where(np.sqrt(seg2) < MAX_SEGMENT_KM, dist, np.inf)
    i = int(np.argmin(dist))
    return float(dist[i]), foot[i]


def nearest_shore(lat, lon, coast):
    """Closest point ON the coastline polyline, in local km about (lat, lon).

    Distance to the nearest *vertex* overstates the distance to the coast -
    Natural Earth vertices are kilometres apart - so measure to the segments.
    Returns (distance_km, anchor_xy) with the anchor as a local km offset.
    """
    cx, cy = local_km(coast[:, 0], coast[:, 1], lat, lon)
    return _foot_of_perpendicular(0.0, 0.0, cx, cy)


def offshore_site(lat, lon, distance_km, coast, radius_km=RADIUS_KM, bathy=None):
    """Put a site exactly `distance_km` off the coast on the shore-normal.

    The alongshore position is kept (the anchor is the closest point on the
    coastline); only the offshore distance is standardised, so the stations
    become comparable to each other instead of sitting 3-12 km out.
    """
    seed_km, anchor = nearest_shore(lat, lon, coast)
    cx, cy = local_km(coast[:, 0], coast[:, 1], lat, lon)
    d_anchor = np.hypot(cx - anchor[0], cy - anchor[1])
    radius = radius_km
    while (d_anchor < radius).sum() < 4 and radius < 60:
        radius += 5.0
    sel = d_anchor < radius

    az = np.radians(tangent_azimuth(cx[sel], cy[sel]))
    normal = np.array([np.cos(az), -np.sin(az)])       # perpendicular to shore

    # Which way is out to sea? Distance to the coastline cannot answer that -
    # it is a line, and both sides of it are equally far away. Use the seed
    # point, which is known to be in water whenever it is a real offshore
    # station; only fall back to the model land mask when the seed sits on the
    # beach, as the Nile mouths do.
    if seed_km > 1.0:
        if np.dot(normal, -anchor) < 0:
            normal = -normal
    elif bathy is not None:
        wet = []
        for sign in (1.0, -1.0):
            p = anchor + normal * sign * distance_km
            la = lat + p[1] / KM_PER_DEG
            lo = lon + p[0] / (KM_PER_DEG * np.cos(np.radians(lat)))
            try:
                d = float(bathy.sel(latitude=la, longitude=lo, method="nearest"))
            except Exception:
                d = float("nan")
            wet.append(np.isfinite(d))
        if wet[1] and not wet[0]:
            normal = -normal

    site = anchor + normal * distance_km
    lat_new = lat + site[1] / KM_PER_DEG
    lon_new = lon + site[0] / (KM_PER_DEG * np.cos(np.radians(lat)))
    offshore_az = (np.degrees(np.arctan2(normal[0], normal[1]))) % 360.0
    return float(lat_new), float(lon_new), anchor, offshore_az


OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def osm_segments():
    """Coastline segments from OpenStreetMap, cached.

    Natural Earth 10m puts its vertices ~2 km apart, which is coarser than the
    features we are measuring against; OSM's median spacing here is ~20 m. Only
    complete segments are kept - never a join between the end of one way and the
    start of the next, which would cut straight across a bay.
    """
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / "osm_coastline.json"
    if not path.exists():
        sites = {**POINTS, **EXTRA_POINTS}
        lats = [p[0] for p in sites.values()]
        lons = [p[1] for p in sites.values()]
        query = (f"[out:json][timeout:300];\n"
                 f'way["natural"="coastline"]'
                 f"({min(lats) - 1.0},{min(lons) - 1.0},"
                 f"{max(lats) + 1.0},{max(lons) + 1.0});\nout geom;")
        print("fetching OSM coastline ...")
        req = urllib.request.Request(OVERPASS_URL, data=query.encode())
        with urllib.request.urlopen(req, timeout=300) as r:
            path.write_bytes(r.read())
    data = json.loads(path.read_text())
    segs = []
    for way in data["elements"]:
        g = way.get("geometry")
        if not g or len(g) < 2:
            continue
        pts = np.array([[p["lon"], p["lat"]] for p in g])
        pts = _smooth_way(pts, SMOOTH_KM)
        if len(pts) < 2:
            continue
        segs.append(np.stack([pts[:-1], pts[1:]], axis=1))
    return np.concatenate(segs, axis=0)


SMOOTH_KM = 2.0      # harbour works are shorter than this; real headlands are not


def _smooth_way(pts, smooth_km, step_km=0.1):
    """Resample a way to a uniform step, then run a moving average over it.

    Port breakwaters and power-station jetties are tagged natural=coastline in
    OSM, so the nearest point on the raw shoreline can be the tip of a 2 km
    structure - which swings the shore-normal by 15-20 deg (Ashdod, Hadera).
    Smoothing at 2 km rounds those off while leaving real headlands in place.
    """
    if len(pts) < 3 or smooth_km <= 0:
        return pts
    lat0 = float(pts[:, 1].mean())
    x = pts[:, 0] * KM_PER_DEG * np.cos(np.radians(lat0))
    y = pts[:, 1] * KM_PER_DEG
    run = np.concatenate([[0.0], np.cumsum(np.hypot(np.diff(x), np.diff(y)))])
    if run[-1] < smooth_km:
        return pts
    at = np.arange(0.0, run[-1], step_km)
    xs = np.interp(at, run, x)
    ys = np.interp(at, run, y)
    win = max(3, int(round(smooth_km / step_km)))
    if len(xs) > win:
        k = np.ones(win) / win
        xs = np.convolve(xs, k, mode="valid")
        ys = np.convolve(ys, k, mode="valid")
    return np.column_stack([xs / (KM_PER_DEG * np.cos(np.radians(lat0))),
                            ys / KM_PER_DEG])


def nearest_shore_osm(lat, lon, segs):
    """(distance_km, anchor) to the closest point on the OSM coastline."""
    ax, ay = local_km(segs[:, 0, 0], segs[:, 0, 1], lat, lon)
    bx, by = local_km(segs[:, 1, 0], segs[:, 1, 1], lat, lon)
    a = np.column_stack([ax, ay])
    ab = np.column_stack([bx - ax, by - ay])
    seg2 = (ab ** 2).sum(1)
    t = np.clip(-(a * ab).sum(1) / np.where(seg2 == 0, 1.0, seg2), 0.0, 1.0)
    foot = a + ab * t[:, None]
    dist = np.hypot(foot[:, 0], foot[:, 1])
    i = int(np.argmin(dist))
    return float(dist[i]), foot[i]


def offshore_site_exact(lat, lon, distance_km, segs, tol=0.01, max_iter=25):
    """Site exactly `distance_km` from the coast, on the true local normal.

    The line from a point to its closest point on the coastline IS the
    perpendicular - no tangent fit, no smoothing radius, no dependence on how
    far along the coast we choose to look. Iterating makes the distance exact
    even where the coast curves away, as it does at the Nile mouths.
    """
    la, lo = lat, lon
    for _ in range(max_iter):
        d, anchor = nearest_shore_osm(la, lo, segs)
        if d < 1e-6:
            return float(la), float(lo), float("nan"), False
        site = anchor * (1.0 - distance_km / d)          # anchor -> out to sea
        la_new = la + site[1] / KM_PER_DEG
        lo_new = lo + site[0] / (KM_PER_DEG * np.cos(np.radians(la)))
        moved = np.hypot(site[0], site[1])
        la, lo = la_new, lo_new
        if moved < tol:
            break
    d_final, anchor = nearest_shore_osm(la, lo, segs)
    normal = -anchor / max(np.hypot(*anchor), 1e-9)
    az = float(np.degrees(np.arctan2(normal[0], normal[1])) % 360.0)
    return float(la), float(lo), az, abs(d_final - distance_km) < 0.05


TANGENT_FIT_KM = 5.0
# Where geometry and the plume disagree, the plume wins - at a river mouth the
# shore-normal is not the direction the water actually goes.
MANUAL_OFFSHORE_AZ = {
    "Rosetta_mouth":  0.0,      # due north, straight across the green band
    "Damietta_mouth": 0.0,      # the 5 km fit says 344; the plume runs north
    "Bardawil_off":   352.2,    # nearest-point normal; the 5 km fit swings it
}                               # to 331 by picking up the lagoon's barrier bar


def offshore_site_tangent(lat, lon, distance_km, segs, name=None,
                          fit_km=TANGENT_FIT_KM, max_iter=25, tol=0.01):
    """Site `distance_km` off the coast, normal to a straight-line fit of the shore.

    Taking the normal straight from the nearest coastline point is exact but
    fragile: at Ashdod and Hadera the closest "coastline" is the tip of a port
    breakwater or a power-station jetty, which swings the normal ~15 deg. Fitting
    a line along `fit_km` of shore and turning 90 deg ignores those structures.
    """
    forced = MANUAL_OFFSHORE_AZ.get(name)
    seed_lat, seed_lon = lat, lon
    la, lo, n_az = lat, lon, float("nan")

    for _ in range(max_iter):
        _, anchor = nearest_shore_osm(la, lo, segs)
        a_lat = la + anchor[1] / KM_PER_DEG
        a_lon = lo + anchor[0] / (KM_PER_DEG * np.cos(np.radians(la)))

        if forced is not None:
            n_az = float(forced)
        else:
            ax, ay = local_km(segs[:, 0, 0], segs[:, 0, 1], a_lat, a_lon)
            d = np.hypot(ax, ay)
            r = fit_km
            while (d < r).sum() < 8 and r < 40:
                r += 2.5
            sel = d < r
            n_az = (tangent_azimuth(ax[sel], ay[sel]) - 90.0) % 360.0
            # Point it out to sea, using the seed station, which is in water.
            sea = np.array([(seed_lon - a_lon) * KM_PER_DEG * np.cos(np.radians(a_lat)),
                            (seed_lat - a_lat) * KM_PER_DEG])
            if np.dot([np.sin(np.radians(n_az)), np.cos(np.radians(n_az))], sea) < 0:
                n_az = (n_az + 180.0) % 360.0

        new_lat = a_lat + np.cos(np.radians(n_az)) * distance_km / KM_PER_DEG
        new_lon = a_lon + (np.sin(np.radians(n_az)) * distance_km
                           / (KM_PER_DEG * np.cos(np.radians(a_lat))))
        moved = np.hypot((new_lon - lo) * KM_PER_DEG * np.cos(np.radians(la)),
                         (new_lat - la) * KM_PER_DEG)
        la, lo = new_lat, new_lon
        if moved < tol:
            break

    d_final, _ = nearest_shore_osm(la, lo, segs)
    return float(la), float(lo), float(n_az), abs(d_final - distance_km) < 0.15


def osm_shore_azimuth(lat, lon, segs, radius_km=5.0):
    """Local coastline tangent from OSM vertices within `radius_km`, mod 180."""
    ax, ay = local_km(segs[:, 0, 0], segs[:, 0, 1], lat, lon)
    d = np.hypot(ax, ay)
    sel = d < radius_km
    while sel.sum() < 8 and radius_km < 40:
        radius_km += 5.0
        sel = d < radius_km
    return tangent_azimuth(ax[sel], ay[sel]), radius_km


def model_bathymetry():
    """Local CMEMS model bathymetry, cached. Returns None if it can't be fetched."""
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / "med_bathy_wide.nc"
    if not path.exists():
        try:
            import copernicusmarine as cm
        except ImportError:
            return None
        sites = {**POINTS, **EXTRA_POINTS}
        lats = [p[0] for p in sites.values()]
        lons = [p[1] for p in sites.values()]
        print("fetching model bathymetry for the isobath cross-check ...")
        ds = cm.open_dataset(
            dataset_id="cmems_mod_med_phy_my_4.2km_static", dataset_part="bathy",
            minimum_longitude=min(lons) - 1.0, maximum_longitude=max(lons) + 1.0,
            minimum_latitude=min(lats) - 1.0, maximum_latitude=max(lats) + 1.0,
        )
        ds[["deptho"]].to_netcdf(path)
    h = xr.open_dataset(path)["deptho"]
    return h.isel(depth=0, drop=True) if "depth" in h.dims else h


# ----------------------------------------------------------------------


def describe_point(lat, lon, ref, coast, bathy):
    """Shore angle, isobath angle and depth for one location."""
    cx, cy = local_km(coast[:, 0], coast[:, 1], lat, lon)
    dist = np.hypot(cx, cy)
    shore_km, _ = nearest_shore(lat, lon, coast)
    radius = RADIUS_KM
    while (dist < radius).sum() < 4 and radius < 60:
        radius += 5.0                                # coarse vertices off Sinai
    sel = dist < radius

    az = tangent_azimuth(cx[sel], cy[sel])
    # Orient it up-coast: flip if it points back toward Egypt.
    if np.dot([np.sin(np.radians(az)), np.cos(np.radians(az))], ref) < 0:
        az = (az + 180.0) % 360.0

    iso, depth = float("nan"), float("nan")
    if bathy is not None:
        blon, blat = np.meshgrid(bathy.longitude.values, bathy.latitude.values)
        bdep = bathy.values
        bx, by = local_km(blon, blat, lat, lon)
        bd = np.hypot(bx, by)
        m = (bd < BATHY_RADIUS_KM) & np.isfinite(bdep)
        if not m.any():          # site outside the bathymetry box - say so
            return dict(shore_azimuth_deg=round(float(az), 1),
                        isobath_azimuth_deg=float("nan"),
                        model_depth_m=float("nan"),
                        distance_to_shore_km=round(shore_km, 1),
                        fit_radius_km=radius, n_vertices=int(sel.sum()))
        A = np.column_stack([bx[m], by[m], np.ones(m.sum())])
        gx, gy, _ = np.linalg.lstsq(A, bdep[m], rcond=None)[0]
        iso = np.degrees(np.arctan2(-gy, gx)) % 180.0      # perpendicular to grad
        depth = float(bdep[np.isfinite(bdep)][np.argmin(bd[np.isfinite(bdep)])])

    return dict(shore_azimuth_deg=round(float(az), 1),
                isobath_azimuth_deg=round(float(iso), 1),
                model_depth_m=round(depth, 1),
                distance_to_shore_km=round(shore_km, 1),
                fit_radius_km=radius, n_vertices=int(sel.sum()))


def upcoast_reference(name, names):
    """Bearing from the previous station to the next one, as local km offsets.
    Used only to orient the tangent (defined mod 180), never to set its angle."""
    i = names.index(name)
    lat, lon = POINTS[name]
    prev_pt = POINTS[names[max(i - 1, 0)]]
    next_pt = POINTS[names[min(i + 1, len(names) - 1)]]
    rx, ry = local_km([prev_pt[1], next_pt[1]], [prev_pt[0], next_pt[0]], lat, lon)
    return np.array([rx[1] - rx[0], ry[1] - ry[0]])


def main():
    coast = coastline_vertices()
    bathy = model_bathymetry()
    names = sorted(POINTS, key=lambda n: POINTS[n][0])
    result = {}

    print(f"{'station':<13}{'shore_az':>9}{'isobath':>9}{'depth_m':>9}"
          f"{'d_shore':>9}{'n_vert':>7}")
    for name in names:
        lat, lon = POINTS[name]
        r = describe_point(lat, lon, upcoast_reference(name, names), coast, bathy)
        result[name] = r
        print(f"{name:<13}{r['shore_azimuth_deg']:9.1f}{r['isobath_azimuth_deg']:9.1f}"
              f"{r['model_depth_m']:9.1f}{r['distance_to_shore_km']:9.1f}"
              f"{r['n_vertices']:7d}")

    OUTFILE.write_text(json.dumps(result, indent=2))
    print(f"\nWrote {OUTFILE.name}. Shore azimuth is degrees clockwise from true "
          f"north, pointing up-coast (Egypt -> Israel).")

    # ---- chlorophyll sites: same alongshore position, uniform offshore distance
    segs = osm_segments()
    chl = {}
    print(f"\nChlorophyll sites, standardised to {CHL_OFFSHORE_KM:.1f} km offshore "
          f"(OSM coastline, exact normal)")
    print(f"{'site':<16}{'lat':>9}{'lon':>9}{'shore_km':>10}{'offshore_az':>12}"
          f"{'osm_az':>8}{'ne_az':>7}{'depth_m':>9}")
    for name, (lat, lon) in {**POINTS, **EXTRA_POINTS}.items():
        was, _ = nearest_shore_osm(lat, lon, segs)
        if name in POINT_ONLY:
            chl[name] = dict(lat=lat, lon=lon, transect=False,
                             distance_to_shore_km=round(was, 1))
            print(f"{name:<16}{lat:9.3f}{lon:9.3f}{was:10.1f}{'-':>12}{'-':>8}"
                  f"{'-':>7}{'-':>9}   point only")
            continue
        la, lo, off_az, ok = offshore_site_tangent(lat, lon, CHL_OFFSHORE_KM,
                                                   segs, name=name)
        got, _ = nearest_shore_osm(la, lo, segs)
        osm_az, rad = osm_shore_azimuth(la, lo, segs)
        r = describe_point(la, lo, upcoast_reference(name, names) if name in POINTS
                           else np.array([1.0, 0.0]), coast, bathy)
        chl[name] = dict(lat=round(la, 4), lon=round(lo, 4), transect=True,
                         distance_to_shore_km=round(got, 2),
                         offshore_azimuth_deg=round(off_az, 1),
                         osm_shore_azimuth_deg=round(float(osm_az), 1),
                         ne_shore_azimuth_deg=r["shore_azimuth_deg"],
                         osm_fit_radius_km=rad,
                         model_depth_m=r["model_depth_m"],
                         converged=bool(ok),
                         moved_from=dict(lat=lat, lon=lon,
                                         distance_to_shore_km=round(was, 1)))
        flag = "" if ok else "   NOT CONVERGED"
        print(f"{name:<16}{la:9.3f}{lo:9.3f}{got:10.2f}{off_az:12.1f}"
              f"{osm_az:8.1f}{r['shore_azimuth_deg']:7.1f}"
              f"{r['model_depth_m']:9.1f}{flag}")

    CHL_OUT.write_text(json.dumps(chl, indent=2))
    print(f"\nWrote {CHL_OUT.name}.")


if __name__ == "__main__":
    main()
