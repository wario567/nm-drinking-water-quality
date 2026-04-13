# New Mexico Drinking Water Quality

An interactive water quality mapping tool for New Mexico, built with publicly available state and federal drinking water data. The project fetches, normalizes, and visualizes contaminant sampling results from regulated public water systems across the state.

The live map is available at: [AGOL Link](https://nmhu.maps.arcgis.com/apps/instant/basic/index.html?appid=bfc0690e23fd4279b557511bf7cc3a25)

---

## What It Does

- Pulls water quality sample data from the NM Environment Department Drinking Water Watch system via the NMWDI SensorThings API
- Pulls public water system metadata (names, populations, contact info) from the EPA Safe Drinking Water Information System (SDWIS)
- Normalizes results to consistent units and flags exceedances against EPA Maximum Contaminant Levels (MCLs)
- Produces GeoJSON layers ready for upload to ArcGIS Online
- Outputs a public-facing interactive map where users can enter their address and see nearby water quality results

---

## Contaminants Covered

| Contaminant | EPA MCL | Health Concern |
|---|---|---|
| Arsenic | 10 µg/L | Naturally occurring metal, long-term cancer risk |
| Atrazine | 3 µg/L | Herbicide |
| DEHP | 6 µg/L | Industrial plasticizer |
| HAA5 | 60 µg/L | Disinfection byproduct |
| Nitrate | 10 mg/L | Fertilizer/waste runoff |
| PCE | 5 µg/L | Dry cleaning solvent |
| Radium (combined) | 5 pCi/L | Naturally occurring radioactive |
| TCE | 5 µg/L | Industrial solvent |
| TTHM | 80 µg/L | Disinfection byproduct |
| Uranium (combined) | 30 µg/L | Naturally occurring radioactive |

---

## Data Sources

**NM Drinking Water Watch**
- Operator: NM Environment Department
- Access: NMWDI SensorThings API
- Endpoint: `https://nmenv.newmexicowaterdata.org/FROST-Server/v1.1`
- Contains lab sample results from all regulated public water systems in NM reported under the Safe Drinking Water Act

**EPA Safe Drinking Water Information System (SDWIS)**
- Operator: US Environmental Protection Agency
- Access: EPA Envirofacts API
- Endpoint: `https://data.epa.gov/efservice/WATER_SYSTEM`
- Contains water system names, population served, system type, contact info, and source water type

Data retrieved: April 2026

---

## Scripts

Run in this order:

| Script | What It Does |
|---|---|
| `nm_dww_fetch.py` | Fetches all sample observations from the SensorThings API for all 10 contaminants |
| `nm_dww_normalize.py` | Normalizes units to a consistent standard, flags MCL exceedances, marks suspect outliers |
| `nm_dww_pull_pws_metadata.py` | Downloads all NM public water system records from EPA SDWIS |
| `nm_dww_agol_prep.py` | Aggregates observations to the sampling point level, builds initial AGOL layers |
| `nm_dww_join_pws_names.py` | Geocodes EPA water systems by zip code and joins names/metadata to sampling points by proximity |
| `nm_dww_composite_layers.py` | Builds the two final public-facing layers: a popup points layer and a composite risk heat map |

---

## Output Files

All outputs written to `nm_dww_data/agol/`:

| File | Description |
|---|---|
| `sampling_points_popup.geojson` | One point per sampling location, all contaminant results, plain-English labels |
| `heatmap_composite.geojson` | Same points weighted by composite MCL ratio score for heat map rendering |

---

## Requirements

```
pip install requests pgeocode scipy numpy
```

Python 3.9+

---

## Limitations

- Covers **regulated public water systems only**. Private residential wells are not tested or reported under this system and are not included.
- Most recent sample dates vary by location and contaminant — some sites have not been sampled since the early 2000s.
- Water system name matching uses zip code proximity, not a direct ID join, so occasional mismatches are possible in areas with multiple overlapping systems.
- Uranium pCi/L values are converted to µg/L using an approximate factor (1.5 µg/pCi) based on natural isotope distribution.

---

## License

MIT License. Data is publicly available from NM Environment Department and US EPA.
