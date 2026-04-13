"""
Pull all NM Public Water System (PWS) metadata from EPA SDWIS via data.epa.gov.

Fetches:
  - All NM state-regulated active+inactive water systems (primacy = NM)
  - All EPA-regulated NM water systems (primacy = 06, tribal/federal)

Outputs:
  nm_dww_data/nm_pws_metadata.csv   — full EPA SDWIS metadata per PWS
"""

import requests
import csv
import time
from pathlib import Path

OUTPUT = Path("nm_dww_data/nm_pws_metadata.csv")

BASE = "https://data.epa.gov/efservice/WATER_SYSTEM"

PAGE = 1000   # max rows per request

FIELDNAMES = [
    "pwsid", "pws_name", "pws_activity_code", "pws_type_code",
    "gw_sw_code", "primary_source_code", "population_served_count",
    "service_connections_count", "owner_type_code",
    "city_name", "zip_code", "state_code", "primacy_agency_code",
    "org_name", "admin_name", "email_addr", "phone_number",
    "address_line1", "address_line2",
    "is_school_or_daycare_ind", "is_wholesaler_ind",
    "pws_deactivation_date",
]


def get_json(url, timeout=45, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(3 ** attempt)
            else:
                print(f"  FAILED: {e}")
                return None


def fetch_systems(primacy_code, label):
    """Fetch all water systems for a given primacy agency code."""
    # Get total count first
    count_url = f"{BASE}/PRIMACY_AGENCY_CODE/{primacy_code}/count/JSON"
    count_data = get_json(count_url)
    if not count_data:
        print(f"  Could not get count for {primacy_code}")
        return []
    total = count_data[0].get("TOTALQUERYRESULTS", 0)
    print(f"  {label}: {total} systems")

    records = []
    offset = 0
    while offset < total:
        url = f"{BASE}/PRIMACY_AGENCY_CODE/{primacy_code}/rows/{offset}:{offset+PAGE-1}/JSON"
        data = get_json(url)
        if not data:
            break
        records.extend(data)
        offset += len(data)
        print(f"    fetched {len(records)}/{total}", end="\r")
        time.sleep(0.3)
    print(f"    fetched {len(records)}/{total} — done")
    return records


def main():
    all_records = []

    print("Fetching NM state-regulated systems (primacy=NM)...")
    all_records.extend(fetch_systems("NM", "NM state-regulated"))

    print("Fetching EPA Region 6 tribal/federal NM systems (primacy=06)...")
    region6 = fetch_systems("06", "EPA Region 6 tribal/federal")
    # Filter to NM only (Region 6 covers TX, NM, OK, LA, AR)
    nm_region6 = [r for r in region6 if r.get("state_code") == "NM"]
    print(f"  Filtered to NM: {len(nm_region6)}")
    all_records.extend(nm_region6)

    # Deduplicate by pwsid
    seen = set()
    deduped = []
    for r in all_records:
        pid = r.get("pwsid")
        if pid not in seen:
            seen.add(pid)
            deduped.append(r)

    print(f"\nTotal unique NM water systems: {len(deduped)}")

    # Summary by activity code and type
    from collections import Counter
    active = Counter(r.get("pws_activity_code") for r in deduped)
    types  = Counter(r.get("pws_type_code") for r in deduped)
    print(f"Activity codes: {dict(active)}")
    print(f"System types  : {dict(types)}")

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for r in deduped:
            writer.writerow({k: r.get(k, "") for k in FIELDNAMES})

    print(f"\nSaved to {OUTPUT}")


if __name__ == "__main__":
    main()
