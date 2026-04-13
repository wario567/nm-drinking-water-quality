"""
Join EPA SDWIS PWS names/metadata onto pws_summary using:
  1. Geocode EPA SDWIS active PWS by zip code (pgeocode)
  2. Nearest-neighbor match between pws_summary centroids
     (derived from actual sampling points) and geocoded SDWIS PWS
  3. Apply a max-distance threshold to reject implausible matches
  4. Rebuild pws_summary.csv/.geojson and sampling_points_current with PWS names

Match logic:
  For each pws_summary centroid, find the closest SDWIS PWS within MAX_DIST_KM.
  Community Water Systems (CWS) are preferred over TNCWS/NTNCWS when tie-breaking.
"""

import csv
import json
import math
from pathlib import Path
from collections import defaultdict

import pgeocode
from scipy.spatial import cKDTree
import numpy as np

# ── Paths ────────────────────────────────────────────────────────────────────
AGOL_DIR     = Path("nm_dww_data/agol")
PWS_META     = Path("nm_dww_data/nm_pws_metadata.csv")
PWS_SUMMARY  = AGOL_DIR / "pws_summary.csv"
SP_CURRENT   = AGOL_DIR / "sampling_points_current.csv"

OUT_PWS_CSV  = AGOL_DIR / "pws_summary.csv"
OUT_PWS_GJ   = AGOL_DIR / "pws_summary.geojson"
OUT_SP_CSV   = AGOL_DIR / "sampling_points_current.csv"
OUT_SP_GJ    = AGOL_DIR / "sampling_points_current.geojson"

MAX_DIST_KM  = 50   # reject matches farther than this

# Type priority: CWS > NTNCWS > TNCWS (community systems preferred)
TYPE_PRIORITY = {"CWS": 0, "NTNCWS": 1, "TNCWS": 2}

# Fields to pull from SDWIS into the output layers
SDWIS_FIELDS = [
    "pwsid", "pws_name", "pws_type_code", "gw_sw_code",
    "primary_source_code", "population_served_count",
    "service_connections_count", "owner_type_code",
    "city_name", "zip_code", "phone_number", "email_addr",
    "org_name", "pws_activity_code",
]


# ── Haversine distance ───────────────────────────────────────────────────────
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (math.sin(d_lat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(d_lon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ── Load and geocode SDWIS PWS ───────────────────────────────────────────────
def load_and_geocode_pws(meta_path):
    """Load all PWS, geocode by zip, return list of dicts with lat/lon."""
    nomi = pgeocode.Nominatim("US")

    # Cache zip -> (lat, lon) so we only call pgeocode once per zip
    zip_cache = {}

    def zip_coords(z):
        z = str(z).strip().split("-")[0][:5]  # take first 5 digits
        if not z or len(z) < 5:
            return None, None
        if z not in zip_cache:
            r = nomi.query_postal_code(z)
            if r is not None and not math.isnan(r.latitude):
                zip_cache[z] = (float(r.latitude), float(r.longitude))
            else:
                zip_cache[z] = (None, None)
        return zip_cache[z]

    geocoded = []
    failed = 0
    with open(meta_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            lat, lon = zip_coords(row.get("zip_code", ""))
            if lat is None:
                failed += 1
                continue
            rec = {k: row.get(k, "") for k in SDWIS_FIELDS}
            rec["sdwis_lat"] = lat
            rec["sdwis_lon"] = lon
            rec["type_priority"] = TYPE_PRIORITY.get(row.get("pws_type_code", ""), 99)
            geocoded.append(rec)

    print(f"  Geocoded: {len(geocoded)}  Failed (no zip): {failed}")
    return geocoded


# ── Nearest-neighbor join using KD-tree ──────────────────────────────────────
def build_match_index(sdwis_records):
    """Build a KD-tree from SDWIS lat/lon for fast nearest-neighbor lookup."""
    coords = np.array([[r["sdwis_lat"], r["sdwis_lon"]] for r in sdwis_records])
    tree = cKDTree(coords)
    return tree, coords


def match_pws(summary_row, tree, coords, sdwis_records, k=5):
    """
    Find best SDWIS PWS match for a pws_summary centroid.
    Queries k nearest neighbors and picks the closest CWS within MAX_DIST_KM.
    """
    lat = float(summary_row["latitude"])
    lon = float(summary_row["longitude"])

    dists, idxs = tree.query([lat, lon], k=min(k, len(sdwis_records)))
    if isinstance(idxs, (int, np.integer)):
        idxs = [idxs]
        dists = [dists]

    candidates = []
    for dist_deg, idx in zip(dists, idxs):
        rec = sdwis_records[idx]
        km = haversine_km(lat, lon, rec["sdwis_lat"], rec["sdwis_lon"])
        if km <= MAX_DIST_KM:
            candidates.append((rec["type_priority"], km, rec))

    if not candidates:
        return None, None

    # Sort: prefer CWS, then closest
    candidates.sort(key=lambda x: (x[0], x[1]))
    best = candidates[0]
    return best[2], round(best[1], 2)


# ── Enrich rows ──────────────────────────────────────────────────────────────
def enrich_row(row, match, dist_km):
    """Add SDWIS fields to a pws_summary or sampling_point row."""
    if match is None:
        for f in SDWIS_FIELDS:
            row[f"sdwis_{f}"] = ""
        row["sdwis_match_dist_km"] = ""
        row["sdwis_match_quality"] = "no_match"
    else:
        for f in SDWIS_FIELDS:
            row[f"sdwis_{f}"] = match.get(f, "")
        row["sdwis_match_dist_km"] = dist_km
        row["sdwis_match_quality"] = (
            "good" if dist_km <= 10
            else "ok" if dist_km <= 25
            else "weak"
        )
    return row


# ── CSV / GeoJSON writers ─────────────────────────────────────────────────────
def write_csv(rows, path):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"  {path.name}: {len(rows):,} rows")


def write_geojson(rows, path, lat_field="latitude", lon_field="longitude"):
    features = []
    for r in rows:
        try:
            lat = float(r[lat_field])
            lon = float(r[lon_field])
        except (ValueError, TypeError, KeyError):
            continue
        props = {k: v for k, v in r.items() if k not in (lat_field, lon_field)}
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })
    gj = {"type": "FeatureCollection", "features": features}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(gj, f, allow_nan=False, default=str)
    print(f"  {path.name}: {len(features):,} features")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("Geocoding EPA SDWIS PWS by zip code...")
    sdwis = load_and_geocode_pws(PWS_META)

    print("Building KD-tree for nearest-neighbor matching...")
    tree, coords = build_match_index(sdwis)

    # ── Enrich pws_summary ──────────────────────────────────────────────────
    print("\nMatching pws_summary centroids to SDWIS PWS...")
    pws_rows = []
    match_quality = defaultdict(int)
    with open(PWS_SUMMARY, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            match, dist = match_pws(row, tree, coords, sdwis)
            enrich_row(row, match, dist)
            pws_rows.append(row)
            match_quality[row["sdwis_match_quality"]] += 1

    print(f"  Match quality: {dict(match_quality)}")

    # Build pws_key -> sdwis_pws_name lookup for sampling point enrichment
    pws_key_to_sdwis = {}
    for r in pws_rows:
        pws_key_to_sdwis[r["pws_key"]] = {
            f: r[f] for f in [f"sdwis_{sf}" for sf in SDWIS_FIELDS]
            + ["sdwis_match_dist_km", "sdwis_match_quality"]
        }

    # ── Enrich sampling_points_current ──────────────────────────────────────
    print("\nEnriching sampling_points_current with PWS names...")
    sp_rows = []
    with open(SP_CURRENT, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = row.get("pws_key", "")
            sdwis_data = pws_key_to_sdwis.get(key, {})
            row.update(sdwis_data)
            sp_rows.append(row)

    # ── Write outputs ────────────────────────────────────────────────────────
    print("\nWriting enriched outputs...")
    write_csv(pws_rows, OUT_PWS_CSV)
    write_geojson(pws_rows, OUT_PWS_GJ)
    write_csv(sp_rows, OUT_SP_CSV)
    write_geojson(sp_rows, OUT_SP_GJ)

    # ── Summary report ───────────────────────────────────────────────────────
    print()
    good = sum(1 for r in pws_rows if r["sdwis_match_quality"] == "good")
    ok   = sum(1 for r in pws_rows if r["sdwis_match_quality"] == "ok")
    weak = sum(1 for r in pws_rows if r["sdwis_match_quality"] == "weak")
    none_ = sum(1 for r in pws_rows if r["sdwis_match_quality"] == "no_match")
    total = len(pws_rows)
    cws  = sum(1 for r in pws_rows if r.get("sdwis_pws_type_code") == "CWS")

    print(f"PWS summary ({total} systems):")
    print(f"  Matched to CWS (community water system): {cws}")
    print(f"  Match quality good  (<10 km) : {good}  ({100*good/total:.0f}%)")
    print(f"  Match quality ok   (<25 km)  : {ok}   ({100*ok/total:.0f}%)")
    print(f"  Match quality weak (<50 km)  : {weak}  ({100*weak/total:.0f}%)")
    print(f"  No match (>50 km or no zip)  : {none_} ({100*none_/total:.0f}%)")

    # Sample matched names
    print()
    print("Sample matched water systems (High risk):")
    hi = [r for r in pws_rows if r.get("risk_tier") == "High"]
    for r in hi:
        print(f"  pws_key={r['pws_key']}  -> {r.get('sdwis_pws_name','(no match)')}"
              f"  ({r.get('sdwis_city_name','')})  dist={r.get('sdwis_match_dist_km','')} km"
              f"  pop={r.get('sdwis_population_served_count','')}")


if __name__ == "__main__":
    main()
