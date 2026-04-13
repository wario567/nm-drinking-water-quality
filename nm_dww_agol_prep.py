"""
Prepare ArcGIS Online-ready layers from normalized NM DWW data.

PWS identification:
  identification_cd format: SP + pws_key(6 digits) + point_seq(3 digits)
  e.g. SP001010011 -> pws_key = "001010", point = "011"
  All sampling points sharing the same pws_key belong to the same water system.

Outputs (all in nm_dww_data/agol/):
  sampling_points_current.csv    — one row per sampling point, wide format,
                                   most recent result + date per contaminant
  sampling_points_current.geojson
  pws_summary.csv                — one row per water system (pws_key),
                                   centroid lat/lon, risk tier, population served
  pws_summary.geojson
  exceedances_recent.csv         — individual exceedances last 5 years, for heatmap
  exceedances_recent.geojson

Column naming for AGOL pop-ups:
  <contaminant>_result      most recent detected value (normalized units)
  <contaminant>_date        date of most recent sample
  <contaminant>_unit        unit of the result
  <contaminant>_mcl         EPA MCL
  <contaminant>_exceeds     True/False/None
  <contaminant>_status      "Exceeds MCL" | "Below MCL" | "Not detected" | "No data"
"""

import csv
import json
import math
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone

INPUT_NORM    = Path("nm_dww_data/nm_dww_normalized.csv")
INPUT_PWS_META = Path("nm_dww_data/nm_pws_metadata.csv")
OUTPUT_DIR    = Path("nm_dww_data/agol")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RECENT_CUTOFF_YEAR = 2019  # "recent" = 2019 onward for exceedances layer

CONTAMINANTS = [
    "ARSENIC", "ATRAZINE", "DEHP", "HAA5", "NITRATE",
    "PCE", "RADIUM_COMBINED", "TCE", "TTHM", "URANIUM_COMBINED",
]

MCL = {
    "ARSENIC":          (10.0,  "ug/L"),
    "ATRAZINE":         (3.0,   "ug/L"),
    "DEHP":             (6.0,   "ug/L"),
    "HAA5":             (60.0,  "ug/L"),
    "NITRATE":          (10.0,  "mg/L"),
    "PCE":              (5.0,   "ug/L"),
    "RADIUM_COMBINED":  (5.0,   "pCi/L"),
    "TCE":              (5.0,   "ug/L"),
    "TTHM":             (80.0,  "ug/L"),
    "URANIUM_COMBINED": (30.0,  "ug/L"),
}

SHORT = {
    "ARSENIC": "Arsenic", "ATRAZINE": "Atrazine", "DEHP": "DEHP",
    "HAA5": "HAA5", "NITRATE": "Nitrate", "PCE": "PCE",
    "RADIUM_COMBINED": "Radium", "TCE": "TCE", "TTHM": "TTHM",
    "URANIUM_COMBINED": "Uranium",
}

RISK_WEIGHTS = {
    "ARSENIC": 3, "URANIUM_COMBINED": 3, "NITRATE": 2,
    "TTHM": 2, "HAA5": 2, "RADIUM_COMBINED": 2,
    "PCE": 2, "TCE": 2, "DEHP": 1, "ATRAZINE": 1,
}


def pws_key(sp_id):
    """Extract 6-digit PWS group key from sampling point identification_cd."""
    if sp_id and sp_id.startswith("SP") and len(sp_id) >= 8:
        return sp_id[2:8]
    return None


def safe_float(v):
    try:
        return float(v) if v not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None


def status_label(exceeds, result_type, result_normalized, contaminant):
    if result_type == "nondetect":
        return "Not detected"
    if exceeds is True:
        return "Exceeds MCL"
    if exceeds is False and result_normalized is not None:
        return "Below MCL"
    return "No data"


def risk_tier(exceeds_set, score):
    """Assign High/Medium/Low/Unknown risk tier."""
    if not exceeds_set:
        return "Unknown"
    if score >= 6:
        return "High"
    if score >= 3:
        return "Medium"
    return "Low"


def load_pws_meta():
    """Load EPA SDWIS PWS metadata. Returns dict keyed by pwsid."""
    meta = {}
    if not INPUT_PWS_META.exists():
        print(f"  WARNING: {INPUT_PWS_META} not found — PWS metadata will be empty")
        return meta
    with open(INPUT_PWS_META, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            meta[row["pwsid"]] = row
    return meta


def load_normalized():
    """
    Load normalized observations.
    Returns dict: sp_id -> contaminant -> list of observation dicts
    Also returns per-row for exceedances layer.
    """
    by_sp = defaultdict(lambda: defaultdict(list))
    all_rows = []

    with open(INPUT_NORM, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            c = row.get("contaminant", "")
            if c not in CONTAMINANTS:
                continue
            sp = row.get("sampling_point_id", "")
            if not sp:
                continue
            by_sp[sp][c].append(row)
            all_rows.append(row)

    return by_sp, all_rows


def most_recent(obs_list):
    """Return the observation with the most recent sample_date."""
    dated = [o for o in obs_list if o.get("sample_date", "").startswith("20") or
             o.get("sample_date", "").startswith("19")]
    if not dated:
        return obs_list[-1] if obs_list else None
    return max(dated, key=lambda o: o.get("sample_date", ""))


def build_sampling_point_rows(by_sp):
    """Build one wide-format row per sampling point."""
    rows = []

    for sp_id, by_c in by_sp.items():
        # Get lat/lon from any observation
        lat = lon = None
        desc = ""
        for obs_list in by_c.values():
            for o in obs_list:
                lat = safe_float(o.get("latitude"))
                lon = safe_float(o.get("longitude"))
                desc = o.get("sampling_point_desc", "")
                if lat and lon:
                    break
            if lat and lon:
                break

        if not lat or not lon:
            continue  # skip points with no geometry

        # Reject clearly invalid coordinates (outside NM bounding box with margin)
        # NM: lat 31.3-37.0, lon -109.1 to -103.0
        if not (30.5 <= lat <= 37.5 and -110.0 <= lon <= -102.0):
            continue

        row = {
            "sp_id":      sp_id,
            "pws_key":    pws_key(sp_id) or "",
            "sp_desc":    desc,
            "latitude":   lat,
            "longitude":  lon,
            "contaminants_sampled": "",
            "contaminants_exceeding": "",
            "risk_score": 0,
            "risk_tier":  "Unknown",
            "most_recent_sample": "",
        }

        exceeds_set = set()
        risk_score = 0
        sampled_set = set()
        all_dates = []

        for c in CONTAMINANTS:
            obs_list = by_c.get(c, [])
            short = SHORT[c]
            mcl_val, mcl_unit = MCL[c]

            if not obs_list:
                row[f"{short}_result"]  = None
                row[f"{short}_date"]    = None
                row[f"{short}_unit"]    = None
                row[f"{short}_mcl"]     = mcl_val
                row[f"{short}_status"]  = "No data"
                row[f"{short}_exceeds"] = None
                continue

            sampled_set.add(c)
            best = most_recent(obs_list)
            norm = safe_float(best.get("result_normalized"))
            exceeds = best.get("exceeds_mcl")
            rtype = best.get("result_type", "")
            suspect = best.get("suspect_outlier") == "True"
            date = (best.get("sample_date") or "")[:10]

            if exceeds == "True" and not suspect:
                exceeds_set.add(c)
                risk_score += RISK_WEIGHTS.get(c, 1)

            if date:
                all_dates.append(date)

            row[f"{short}_result"]  = round(norm, 4) if norm is not None else None
            row[f"{short}_date"]    = date
            row[f"{short}_unit"]    = best.get("result_unit_norm", mcl_unit)
            row[f"{short}_mcl"]     = mcl_val
            row[f"{short}_exceeds"] = (exceeds == "True") if exceeds != "" else None
            row[f"{short}_status"]  = status_label(
                exceeds == "True", rtype, norm, c
            )

        row["contaminants_sampled"]   = ", ".join(SHORT[c] for c in CONTAMINANTS if c in sampled_set)
        row["contaminants_exceeding"] = ", ".join(SHORT[c] for c in CONTAMINANTS if c in exceeds_set)
        row["risk_score"]  = risk_score
        row["risk_tier"]   = risk_tier(exceeds_set, risk_score)
        row["most_recent_sample"] = max(all_dates) if all_dates else ""

        rows.append(row)

    return rows


def build_pws_rows(sp_rows):
    """Aggregate sampling point rows to PWS level."""
    by_pws = defaultdict(list)
    for r in sp_rows:
        by_pws[r["pws_key"]].append(r)

    pws_rows = []
    for key, pts in by_pws.items():
        if not key:
            continue
        # Centroid of sampling points
        lats = [p["latitude"] for p in pts if p.get("latitude")]
        lons = [p["longitude"] for p in pts if p.get("longitude")]
        if not lats:
            continue
        lat = sum(lats) / len(lats)
        lon = sum(lons) / len(lons)

        # Aggregate risk
        all_exceeding = set()
        max_score = 0
        latest_date = ""
        for p in pts:
            exc = p.get("contaminants_exceeding", "")
            if exc:
                all_exceeding.update(exc.split(", "))
            max_score = max(max_score, p.get("risk_score", 0))
            d = p.get("most_recent_sample", "")
            if d > latest_date:
                latest_date = d

        # Contaminant coverage (any sampling point sampled it)
        sampled_union = set()
        for p in pts:
            sampled_union.update(p.get("contaminants_sampled", "").split(", "))
        sampled_union.discard("")

        row = {
            "pws_key":              key,
            "latitude":             round(lat, 6),
            "longitude":            round(lon, 6),
            "sampling_point_count": len(pts),
            "contaminants_sampled": ", ".join(sorted(sampled_union)),
            "contaminants_exceeding": ", ".join(sorted(all_exceeding)),
            "exceedance_count":     len(all_exceeding),
            "risk_score":           max_score,
            "risk_tier":            risk_tier(all_exceeding, max_score),
            "most_recent_sample":   latest_date,
            # EPA SDWIS fields (populated later)
            "pwsid":       "",
            "pws_name":    "",
            "pws_type":    "",
            "source_type": "",
            "population":  "",
            "city":        "",
            "zip":         "",
            "phone":       "",
        }

        pws_rows.append(row)

    return pws_rows


def build_exceedances_rows(all_obs_rows):
    """Recent exceedances only, for heatmap/density layer."""
    out = []
    for r in all_obs_rows:
        if r.get("exceeds_mcl") != "True":
            continue
        if r.get("suspect_outlier") == "True":
            continue
        date = r.get("sample_date", "")[:10]
        if not date:
            continue
        try:
            year = int(date[:4])
        except ValueError:
            continue
        if year < RECENT_CUTOFF_YEAR:
            continue
        lat = safe_float(r.get("latitude"))
        lon = safe_float(r.get("longitude"))
        if not lat or not lon:
            continue

        c = r["contaminant"]
        mcl_val, mcl_unit = MCL[c]
        norm = safe_float(r.get("result_normalized"))
        ratio = round(norm / mcl_val, 2) if norm and mcl_val else None

        out.append({
            "sp_id":        r.get("sampling_point_id", ""),
            "pws_key":      pws_key(r.get("sampling_point_id", "")) or "",
            "contaminant":  SHORT[c],
            "sample_date":  date,
            "result":       norm,
            "unit":         mcl_unit,
            "mcl":          mcl_val,
            "mcl_ratio":    ratio,
            "latitude":     lat,
            "longitude":    lon,
        })

    return out


def rows_to_geojson(rows, lat_field="latitude", lon_field="longitude"):
    features = []
    for r in rows:
        lat = r.get(lat_field)
        lon = r.get(lon_field)
        if lat is None or lon is None:
            continue
        props = {k: v for k, v in r.items() if k not in (lat_field, lon_field)}
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })
    return {"type": "FeatureCollection", "features": features}


def write_csv(rows, path):
    if not rows:
        print(f"  (no rows for {path})")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows):,} rows -> {path}")


def write_geojson(rows, path, lat_field="latitude", lon_field="longitude"):
    gj = rows_to_geojson(rows, lat_field, lon_field)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(gj, f, allow_nan=False, default=str)
    print(f"  Wrote {len(gj['features']):,} features -> {path}")


def main():
    print("Loading PWS metadata...")
    pws_meta = load_pws_meta()
    print(f"  {len(pws_meta)} PWS records loaded")

    print("\nLoading normalized observations...")
    by_sp, all_rows = load_normalized()
    print(f"  {len(by_sp):,} sampling points, {len(all_rows):,} observations")

    print("\nBuilding sampling point layer...")
    sp_rows = build_sampling_point_rows(by_sp)
    print(f"  {len(sp_rows):,} points with geometry")

    print("\nBuilding PWS summary layer...")
    pws_rows = build_pws_rows(sp_rows)
    print(f"  {len(pws_rows):,} water systems")

    print("\nBuilding recent exceedances layer...")
    exc_rows = build_exceedances_rows(all_rows)
    print(f"  {len(exc_rows):,} exceedances since {RECENT_CUTOFF_YEAR}")

    print("\nWriting outputs...")

    write_csv(sp_rows,  OUTPUT_DIR / "sampling_points_current.csv")
    write_geojson(sp_rows, OUTPUT_DIR / "sampling_points_current.geojson")

    write_csv(pws_rows, OUTPUT_DIR / "pws_summary.csv")
    write_geojson(pws_rows, OUTPUT_DIR / "pws_summary.geojson")

    write_csv(exc_rows, OUTPUT_DIR / "exceedances_recent.csv")
    write_geojson(exc_rows, OUTPUT_DIR / "exceedances_recent.geojson")

    # Summary stats
    tiers = defaultdict(int)
    for r in pws_rows:
        tiers[r["risk_tier"]] += 1
    print(f"\nPWS risk tier breakdown:")
    for tier in ["High", "Medium", "Low", "Unknown"]:
        print(f"  {tier}: {tiers[tier]}")

    print(f"\nDone. All outputs in {OUTPUT_DIR}/")
    print("AGOL upload checklist:")
    print("  1. sampling_points_current.geojson  -> feature layer (sampling point detail)")
    print("  2. pws_summary.geojson              -> feature layer (system-level summary, Near Me widget target)")
    print("  3. exceedances_recent.geojson        -> heat map layer (2019+)")


if __name__ == "__main__":
    main()
