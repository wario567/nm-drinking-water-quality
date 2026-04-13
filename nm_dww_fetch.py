"""
Fetch drinking water contaminant samples from the NM Drinking Water Watch
SensorThings API (FROST Server).

Endpoint: https://nmenv.newmexicowaterdata.org/FROST-Server/v1.1

Strategy: query Observations directly filtered by ObservedProperty, expanding
Thing/Locations inline. This avoids iterating 47K+ datastreams individually.

Output columns:
  contaminant, observed_property_id, sampling_point_id, sampling_point_desc,
  sample_date, result, unit, latitude, longitude, datastream_id, observation_id

Output: nm_dww_data/nm_dww_all_contaminants.csv + one CSV per contaminant
"""

import requests
import csv
import time
from pathlib import Path

BASE_URL = "https://nmenv.newmexicowaterdata.org/FROST-Server/v1.1"

CONTAMINANTS = {
    "ARSENIC":          3,
    "ATRAZINE":         123,
    "DEHP":             114,
    "HAA5":             670,
    "NITRATE":          35,
    "PCE":              352,
    "RADIUM_COMBINED":  394,
    "TCE":              357,
    "TTHM":             332,
    "URANIUM_COMBINED": 385,
}

OUTPUT_DIR = Path("nm_dww_data")
OUTPUT_DIR.mkdir(exist_ok=True)

PAGE_SIZE = 1000

FIELDNAMES = [
    "contaminant",
    "observed_property_id",
    "sampling_point_id",
    "sampling_point_desc",
    "sample_date",
    "result",
    "unit",
    "latitude",
    "longitude",
    "datastream_id",
    "observation_id",
]

# Expand: Datastream (unit) -> Thing (props, desc) -> Locations (coords)
EXPAND = (
    "Datastream("
        "$select=@iot.id,unitOfMeasurement;"
        "$expand=Thing("
            "$select=@iot.id,properties,description;"
            "$expand=Locations($select=location;$top=1)"
        ")"
    ")"
)


def get_json(url, retries=4):
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=45)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            wait = 2 ** attempt
            print(f"    Retry {attempt+1}/{retries} after {wait}s — {e}")
            time.sleep(wait)
    print(f"    FAILED: {url}")
    return None


def build_obs_url(op_id):
    return (
        f"{BASE_URL}/Observations"
        f"?$filter=Datastream/ObservedProperty/@iot.id eq {op_id}"
        f"&$select=@iot.id,result,phenomenonTime"
        f"&$expand={EXPAND}"
        f"&$top={PAGE_SIZE}"
        f"&$orderby=phenomenonTime asc"
    )


def parse_obs(obs, name, op_id):
    ds = obs.get("Datastream", {}) or {}
    thing = ds.get("Thing", {}) or {}
    props = thing.get("properties", {}) or {}
    locs = thing.get("Locations", []) or []

    coords = locs[0].get("location", {}).get("coordinates") if locs else None
    if coords and len(coords) >= 2:
        lat, lon = coords[1], coords[0]
    else:
        lat, lon = None, None

    raw_time = obs.get("phenomenonTime", "")
    sample_date = raw_time.split("/")[0] if raw_time else ""

    return {
        "contaminant": name,
        "observed_property_id": op_id,
        "sampling_point_id": props.get("identification_cd", ""),
        "sampling_point_desc": props.get("description_text", thing.get("description", "")),
        "sample_date": sample_date,
        "result": obs.get("result", ""),
        "unit": ds.get("unitOfMeasurement", {}).get("symbol", ""),
        "latitude": lat,
        "longitude": lon,
        "datastream_id": ds.get("@iot.id", ""),
        "observation_id": obs.get("@iot.id", ""),
    }


def fetch_contaminant(name, op_id, writer):
    print(f"\n{'='*60}")
    print(f"Fetching: {name} (ObservedProperty ID={op_id})")

    url = build_obs_url(op_id)
    page = 0
    total = 0

    while url:
        page += 1
        data = get_json(url)
        if not data:
            break

        rows = data.get("value", [])
        for obs in rows:
            writer.writerow(parse_obs(obs, name, op_id))
        total += len(rows)

        url = data.get("@iot.nextLink")
        print(f"  page {page:3d} — {total:6,} observations so far", end="\r")
        if url:
            time.sleep(0.1)

    print(f"  Done — {total:,} observations total             ")
    return total


def main():
    combined_path = OUTPUT_DIR / "nm_dww_all_contaminants.csv"
    grand_total = 0

    with open(combined_path, "w", newline="", encoding="utf-8") as cf:
        combined_writer = csv.DictWriter(cf, fieldnames=FIELDNAMES)
        combined_writer.writeheader()

        for name, op_id in CONTAMINANTS.items():
            per_path = OUTPUT_DIR / f"nm_dww_{name.lower()}.csv"
            with open(per_path, "w", newline="", encoding="utf-8") as pf:
                per_writer = csv.DictWriter(pf, fieldnames=FIELDNAMES)
                per_writer.writeheader()

                class TeeWriter:
                    def writerow(self, row):
                        per_writer.writerow(row)
                        combined_writer.writerow(row)

                count = fetch_contaminant(name, op_id, TeeWriter())
                grand_total += count

    print(f"\n{'='*60}")
    print(f"Done. {grand_total:,} total observations.")
    print(f"Combined : {combined_path}")
    print(f"Per-file : {OUTPUT_DIR}/nm_dww_<name>.csv")


if __name__ == "__main__":
    main()
