"""
Build two public-facing AGOL layers with clean, plain-English fields.

sampling_points_popup.geojson  — one point per sampling location
heatmap_composite.geojson      — same points, weighted by composite risk score

Pop-up fields are named and formatted for general public readability.
Technical/internal fields are excluded entirely.
"""

import csv
import json
from pathlib import Path
from collections import defaultdict

INPUT_NORM = Path("nm_dww_data/nm_dww_normalized.csv")
INPUT_SP   = Path("nm_dww_data/agol/sampling_points_current.csv")
OUTPUT_DIR = Path("nm_dww_data/agol")

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

# Plain-English contaminant descriptions for pop-ups
CONTAMINANT_DESC = {
    "Arsenic":   "Arsenic (naturally occurring metal)",
    "Atrazine":  "Atrazine (herbicide)",
    "DEHP":      "DEHP (industrial plasticizer)",
    "HAA5":      "HAA5 (disinfection byproduct)",
    "Nitrate":   "Nitrate (fertilizer/waste runoff)",
    "PCE":       "PCE (dry cleaning solvent)",
    "Radium":    "Radium (naturally occurring radioactive)",
    "TCE":       "TCE (industrial solvent)",
    "TTHM":      "TTHM (disinfection byproduct)",
    "Uranium":   "Uranium (naturally occurring radioactive)",
}

PWS_TYPE = {
    "CWS":    "Community Water System",
    "NTNCWS": "Non-Community (Regular)",
    "TNCWS":  "Non-Community (Transient)",
}

SOURCE_TYPE = {
    "GW":   "Groundwater",
    "SW":   "Surface Water",
    "GW_P": "Purchased Groundwater",
    "SW_P": "Purchased Surface Water",
    "GUP":  "Groundwater Under Influence",
}

MCL_CAP   = 5.0
LAT_MIN, LAT_MAX = 30.5, 37.5
LON_MIN, LON_MAX = -110.0, -102.0


def safe_float(v):
    try:
        return float(v) if v not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None


def fmt_result(norm, mcl_val, mcl_unit, rtype, exceeds):
    """Format a result value as a readable string."""
    if rtype == "nondetect":
        return "Not detected"
    if norm is None:
        return "No data"
    ratio = norm / mcl_val
    unit  = mcl_unit.replace("ug/L", "µg/L").replace("pCi/L", "pCi/L")
    if exceeds:
        return f"EXCEEDS LIMIT: {norm:g} {unit} ({ratio:.1f}x the safe limit of {mcl_val:g} {unit})"
    else:
        return f"Safe: {norm:g} {unit}  (limit is {mcl_val:g} {unit})"


def load_pws_lookup():
    lookup = {}
    if not INPUT_SP.exists():
        return lookup
    with open(INPUT_SP, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sp = row.get("sp_id", "")
            lookup[sp] = {
                "system_name": row.get("sdwis_pws_name", "").title(),
                "city":        row.get("sdwis_city_name", "").title(),
                "system_type": PWS_TYPE.get(row.get("sdwis_pws_type_code", ""),
                               row.get("sdwis_pws_type_code", "")),
                "source_water": SOURCE_TYPE.get(row.get("sdwis_gw_sw_code", ""),
                                row.get("sdwis_gw_sw_code", "")),
                "population":  row.get("sdwis_population_served_count", ""),
                "phone":       row.get("sdwis_phone_number", ""),
                "email":       row.get("sdwis_email_addr", ""),
                "pwsid":       row.get("sdwis_pwsid", ""),
            }
    return lookup


def load_observations():
    by_sp = defaultdict(lambda: defaultdict(list))
    coords = {}
    with open(INPUT_NORM, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            c = row.get("contaminant", "")
            if c not in MCL:
                continue
            if row.get("suspect_outlier") == "True":
                continue
            sp = row.get("sampling_point_id", "")
            if not sp:
                continue
            lat = safe_float(row.get("latitude"))
            lon = safe_float(row.get("longitude"))
            if lat and lon and (LAT_MIN <= lat <= LAT_MAX) and (LON_MIN <= lon <= LON_MAX):
                coords[sp] = (lat, lon)
            by_sp[sp][c].append(row)
    return by_sp, coords


def most_recent(obs_list):
    dated = [o for o in obs_list if o.get("sample_date", "")]
    return max(dated, key=lambda o: o.get("sample_date", "")) if dated else obs_list[-1]


def build_layers(by_sp, coords, pws_lookup):
    popup_features   = []
    heatmap_features = []

    for sp_id, by_c in by_sp.items():
        if sp_id not in coords:
            continue
        lat, lon = coords[sp_id]

        pws = pws_lookup.get(sp_id, {})
        system_name = pws.get("system_name") or sp_id
        city        = pws.get("city", "")

        exceeding      = []
        sampled        = []
        composite_score = 0.0
        all_dates_list = []

        # Per-contaminant fields for popup (only include sampled ones)
        c_fields = {}
        c_hm     = {}

        for c in sorted(MCL.keys()):
            short    = SHORT[c]
            mcl_val, mcl_unit = MCL[c]
            obs_list = by_c.get(c, [])

            if not obs_list:
                c_hm[f"{short}_Score"] = 0.0
                continue

            sampled.append(short)
            best    = most_recent(obs_list)
            rtype   = best.get("result_type", "")
            norm    = safe_float(best.get("result_normalized"))
            exceeds = best.get("exceeds_mcl") == "True"
            date    = (best.get("sample_date") or "")[:10]

            if date:
                all_dates_list.append(date)

            # Composite score
            mcl_ratio = 0.0
            if norm and norm > 0 and rtype == "detected":
                mcl_ratio = norm / mcl_val
                composite_score += min(mcl_ratio, MCL_CAP)

            if exceeds:
                exceeding.append(short)

            desc = CONTAMINANT_DESC.get(short, short)
            c_fields[desc]               = fmt_result(norm, mcl_val, mcl_unit, rtype, exceeds)
            c_fields[f"{short}_Tested"]  = date if date else "Unknown"
            c_hm[f"{short}_Score"]       = round(min(mcl_ratio, MCL_CAP), 4)

        composite_score = round(composite_score, 4)

        if composite_score >= 8:
            risk_level = "High Risk"
        elif composite_score >= 3:
            risk_level = "Medium Risk"
        elif composite_score > 0:
            risk_level = "Low Risk"
        else:
            risk_level = "No Detections"

        last_sampled = max(all_dates_list) if all_dates_list else "Unknown"

        # ── Popup properties (clean, public-facing) ──────────────────────
        popup_props = {
            "Water_System":        system_name,
            "City":                city,
            "System_Type":         pws.get("system_type", ""),
            "Source_Water":        pws.get("source_water", ""),
            "Population_Served":   pws.get("population", ""),
            "Phone":               pws.get("phone", ""),
            "Risk_Level":          risk_level,
            "Contaminants_Tested": ", ".join(sampled) if sampled else "None",
            "Over_Safe_Limit":     ", ".join(exceeding) if exceeding else "None",
            "Number_Over_Limit":   len(exceeding),
            "Last_Sampled":        last_sampled,
        }
        popup_props.update(c_fields)

        # ── Heatmap properties ────────────────────────────────────────────
        hm_props = {
            "Water_System":    system_name,
            "City":            city,
            "Risk_Level":      risk_level,
            "Over_Safe_Limit": ", ".join(exceeding) if exceeding else "None",
            "Num_Over_Limit":  len(exceeding),
            "Composite_Score": composite_score,
            "Last_Sampled":    last_sampled,
        }
        hm_props.update(c_hm)

        popup_features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": popup_props,
        })
        heatmap_features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": hm_props,
        })

    return popup_features, heatmap_features


def write_geojson(features, path):
    gj = {"type": "FeatureCollection", "features": features}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(gj, f, allow_nan=False, default=str)
    import os
    sz = os.path.getsize(path) / 1024
    print(f"  {path.name}: {len(features):,} features  ({sz:.0f} KB)")


def main():
    print("Loading data...")
    pws_lookup      = load_pws_lookup()
    by_sp, coords   = load_observations()
    print(f"  {len(by_sp):,} sampling points  |  {len(coords):,} with valid coordinates")

    print("\nBuilding layers...")
    popup_features, heatmap_features = build_layers(by_sp, coords, pws_lookup)

    print("\nWriting files...")
    write_geojson(popup_features,   OUTPUT_DIR / "sampling_points_popup.geojson")
    write_geojson(heatmap_features, OUTPUT_DIR / "heatmap_composite.geojson")

    # Stats
    tiers = [f["properties"]["Risk_Level"] for f in heatmap_features]
    from collections import Counter
    print(f"\nRisk level breakdown:")
    for tier, n in Counter(tiers).most_common():
        print(f"  {tier}: {n:,}")

    print("\nSample popup for Sunset Acres (if present):")
    match = next((f for f in popup_features
                  if "Sunset" in f["properties"].get("Water_System", "")), None)
    if match:
        for k, v in match["properties"].items():
            if v and v != "No data" and v != "Not detected":
                print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
