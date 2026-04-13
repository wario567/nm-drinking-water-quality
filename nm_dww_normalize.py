"""
Normalize NM DWW contaminant data to consistent units and flag EPA MCL exceedances.

Unit normalization targets:
  Arsenic, Atrazine, DEHP, HAA5, PCE, TCE, TTHM  -> ug/L
  Nitrate                                           -> mg/L (as N)
  Radium (combined)                                 -> pCi/L
  Uranium (combined)                                -> ug/L  (pCi/L converted via 1.5 ug/pCi)

EPA MCLs applied:
  Arsenic          10   ug/L
  Atrazine          3   ug/L
  DEHP              6   ug/L
  HAA5             60   ug/L
  Nitrate          10   mg/L
  PCE               5   ug/L
  Radium (226+228)  5   pCi/L
  TCE               5   ug/L
  TTHM             80   ug/L
  Uranium          30   ug/L

Non-detect handling:
  - result_numeric set to 0 (conservative; use result_dl for the detection limit)
  - exceeds_mcl = False for non-detects regardless of stated limit

Outputs:
  nm_dww_data/nm_dww_normalized.csv        — all contaminants combined
  nm_dww_data/nm_dww_exceedances.csv       — rows where exceeds_mcl = True
  nm_dww_data/nm_dww_normalization_report.txt
"""

import csv
import re
import sys
from pathlib import Path
from collections import defaultdict

INPUT  = Path("nm_dww_data/nm_dww_all_contaminants.csv")
OUT_NORM  = Path("nm_dww_data/nm_dww_normalized.csv")
OUT_EXCEED = Path("nm_dww_data/nm_dww_exceedances.csv")
OUT_REPORT = Path("nm_dww_data/nm_dww_normalization_report.txt")

# ── EPA MCLs ────────────────────────────────────────────────────────────────
MCL = {
    "ARSENIC":          (10.0,   "ug/L"),
    "ATRAZINE":         (3.0,    "ug/L"),
    "DEHP":             (6.0,    "ug/L"),
    "HAA5":             (60.0,   "ug/L"),
    "NITRATE":          (10.0,   "mg/L"),
    "PCE":              (5.0,    "ug/L"),
    "RADIUM_COMBINED":  (5.0,    "pCi/L"),
    "TCE":              (5.0,    "ug/L"),
    "TTHM":             (80.0,   "ug/L"),
    "URANIUM_COMBINED": (30.0,   "ug/L"),
}

# Target unit per contaminant
TARGET_UNIT = {c: u for c, (_, u) in MCL.items()}

# ── Unit conversion factors to target unit ──────────────────────────────────
# Returns multiplier to convert `from_unit` -> target for that contaminant.
# None means the unit is incompatible / cannot be auto-converted.
def conversion_factor(contaminant, from_unit):
    target = TARGET_UNIT[contaminant]
    fu = from_unit.strip().lower()
    tu = target.strip().lower()

    if fu == tu:
        return 1.0

    if tu == "ug/l":
        if fu == "mg/l":  return 1000.0
        if fu == "ug/l":  return 1.0
        # Uranium pCi/L -> ug/L: use 1.5 ug per pCi (natural isotope distribution)
        if fu == "pci/l" and contaminant == "URANIUM_COMBINED":
            return 1.5
        return None

    if tu == "mg/l":
        if fu == "ug/l":  return 0.001
        if fu == "mg/l":  return 1.0
        return None

    if tu == "pci/l":
        if fu == "pci/l": return 1.0
        return None

    return None


# ── Result string parser ─────────────────────────────────────────────────────
ND_PAT = re.compile(
    r'(<\s*mrl|<\s*mdl|\bnd\b|not\s+detected|bdl|below\s+det)', re.I
)
NUM_PAT = re.compile(r'[-+]?[\d]*\.?[\d]+(?:[eE][-+]?\d+)?')

def parse_result(raw):
    """
    Returns (numeric_value, detection_limit, result_type)
    result_type: 'detected' | 'nondetect' | 'empty' | 'unparseable'
    detection_limit: float if nondetect, else None
    numeric_value: float if detected or nondetect (0.0), else None
    """
    if raw is None:
        return None, None, 'empty'
    s = str(raw).strip()
    if not s:
        return None, None, 'empty'

    # Non-detect patterns
    if ND_PAT.search(s) or re.match(r'^<\s*[\d.]', s):
        m = NUM_PAT.search(s)
        dl = float(m.group()) if m else None
        return 0.0, dl, 'nondetect'

    # Pure numeric
    m = NUM_PAT.search(s)
    if m:
        return float(m.group()), None, 'detected'

    return None, None, 'unparseable'


# ── Main ─────────────────────────────────────────────────────────────────────
FIELDNAMES_OUT = [
    "contaminant",
    "observed_property_id",
    "sampling_point_id",
    "sampling_point_desc",
    "sample_date",
    "result_raw",
    "result_type",           # detected | nondetect | empty | unparseable
    "result_numeric",        # 0 for nondetect, None for empty/unparseable
    "result_dl",             # detection limit for nondetects
    "result_unit_orig",      # original unit
    "result_normalized",     # value in target unit
    "result_unit_norm",      # target unit
    "unit_conversion",       # factor applied (or 'incompatible'/'no_unit')
    "mcl",
    "mcl_unit",
    "exceeds_mcl",           # True/False/None (None = cannot evaluate)
    "suspect_outlier",       # True if value is >100x MCL after unit conversion (likely data entry error)
    "latitude",
    "longitude",
    "datastream_id",
    "observation_id",
]

def main():
    stats = defaultdict(lambda: {
        "n": 0, "detected": 0, "nondetect": 0, "empty": 0, "unparseable": 0,
        "converted": 0, "incompatible_unit": 0, "exceeds": 0, "suspect": 0,
        "values": [],
    })
    skipped_stray = 0

    valid_contaminants = set(MCL.keys())

    with (
        open(INPUT, encoding="utf-8") as fin,
        open(OUT_NORM,   "w", newline="", encoding="utf-8") as fnorm,
        open(OUT_EXCEED, "w", newline="", encoding="utf-8") as fexceed,
    ):
        reader = csv.DictReader(fin)
        norm_writer   = csv.DictWriter(fnorm,   fieldnames=FIELDNAMES_OUT)
        exceed_writer = csv.DictWriter(fexceed, fieldnames=FIELDNAMES_OUT)
        norm_writer.writeheader()
        exceed_writer.writeheader()

        for row in reader:
            contaminant = row.get("contaminant", "")
            if contaminant not in valid_contaminants:
                skipped_stray += 1
                continue
            if None in row:  # shifted row
                skipped_stray += 1
                continue

            s = stats[contaminant]
            s["n"] += 1

            raw_result  = row.get("result", "")
            raw_unit    = (row.get("unit") or "").strip()
            mcl_val, mcl_unit = MCL[contaminant]

            numeric, dl, rtype = parse_result(raw_result)
            s[rtype] += 1

            # Unit conversion
            factor = conversion_factor(contaminant, raw_unit) if raw_unit else None

            if raw_unit == "" or raw_unit is None:
                unit_conv_label = "no_unit"
                normalized = None
            elif factor is None:
                unit_conv_label = "incompatible"
                normalized = None
                s["incompatible_unit"] += 1
            else:
                unit_conv_label = str(factor)
                normalized = round(numeric * factor, 6) if numeric is not None else None
                if factor != 1.0 and numeric is not None:
                    s["converted"] += 1

            # MCL exceedance
            if rtype == "nondetect":
                exceeds = False
            elif normalized is not None:
                exceeds = normalized > mcl_val
            else:
                exceeds = None  # can't evaluate (incompatible unit or empty)

            # Suspect outlier: >100x MCL after conversion (likely unit labeling error)
            suspect = (
                normalized is not None
                and rtype == "detected"
                and normalized > mcl_val * 100
            )

            if exceeds:
                s["exceeds"] += 1
            if suspect:
                s.setdefault("suspect", 0)
                s["suspect"] += 1
            if normalized is not None and rtype == "detected":
                s["values"].append(normalized)

            out_row = {
                "contaminant":       contaminant,
                "observed_property_id": row.get("observed_property_id", ""),
                "sampling_point_id": row.get("sampling_point_id", ""),
                "sampling_point_desc": row.get("sampling_point_desc", ""),
                "sample_date":       row.get("sample_date", ""),
                "result_raw":        raw_result,
                "result_type":       rtype,
                "result_numeric":    numeric,
                "result_dl":         dl,
                "result_unit_orig":  raw_unit,
                "result_normalized": normalized,
                "result_unit_norm":  TARGET_UNIT[contaminant],
                "unit_conversion":   unit_conv_label,
                "mcl":               mcl_val,
                "mcl_unit":          mcl_unit,
                "exceeds_mcl":       exceeds,
                "suspect_outlier":   suspect,
                "latitude":          row.get("latitude", ""),
                "longitude":         row.get("longitude", ""),
                "datastream_id":     row.get("datastream_id", ""),
                "observation_id":    row.get("observation_id", ""),
            }

            norm_writer.writerow(out_row)
            if exceeds:
                exceed_writer.writerow(out_row)

    # ── Report ───────────────────────────────────────────────────────────────
    lines = []
    lines.append("=" * 72)
    lines.append("NM DRINKING WATER WATCH -- NORMALIZATION & MCL EXCEEDANCE REPORT")
    lines.append("=" * 72)
    lines.append(f"Input  : {INPUT}")
    lines.append(f"Output : {OUT_NORM}")
    lines.append(f"Stray/corrupted rows skipped: {skipped_stray}")
    lines.append("")

    grand_n = grand_exceed = grand_incompat = 0

    for contaminant in sorted(stats):
        s = stats[contaminant]
        n = s["n"]
        mcl_val, mcl_unit = MCL[contaminant]
        vals = s["values"]
        exceed_pct = 100 * s["exceeds"] / n if n else 0
        detect_pct = 100 * s["detected"] / n if n else 0
        nd_pct     = 100 * s["nondetect"] / n if n else 0

        grand_n      += n
        grand_exceed += s["exceeds"]
        grand_incompat += s["incompatible_unit"]

        lines.append(f"  {contaminant}")
        lines.append(f"    EPA MCL            : {mcl_val} {mcl_unit}")
        lines.append(f"    Observations       : {n:,}")
        lines.append(f"    Detected           : {s['detected']:,}  ({detect_pct:.1f}%)")
        lines.append(f"    Non-detect         : {s['nondetect']:,}  ({nd_pct:.1f}%)")
        lines.append(f"    Empty/unparseable  : {s['empty'] + s['unparseable']}")
        lines.append(f"    Incompatible unit  : {s['incompatible_unit']}")
        lines.append(f"    Unit conversions   : {s['converted']} rows scaled")
        if vals:
            lines.append(f"    Detected range     : {min(vals):.4g} - {max(vals):.4g} {TARGET_UNIT[contaminant]}")
            lines.append(f"    Detected median    : {sorted(vals)[len(vals)//2]:.4g} {TARGET_UNIT[contaminant]}")
        lines.append(f"    Exceeds MCL        : {s['exceeds']:,}  ({exceed_pct:.1f}% of all obs)")
        lines.append(f"    Suspect outliers   : {s.get('suspect', 0)}  (>100x MCL -- likely unit error)")
        lines.append("")

    lines.append("-" * 72)
    lines.append(f"  TOTALS")
    lines.append(f"    Total observations : {grand_n:,}")
    lines.append(f"    Total exceedances  : {grand_exceed:,}")
    lines.append(f"    Incompatible units : {grand_incompat}")
    lines.append("=" * 72)

    report = "\n".join(lines)
    print(report)
    OUT_REPORT.write_text(report, encoding="utf-8")
    print(f"\nReport saved to {OUT_REPORT}")
    print(f"Normalized data : {OUT_NORM}")
    print(f"Exceedances only: {OUT_EXCEED}")


if __name__ == "__main__":
    main()
