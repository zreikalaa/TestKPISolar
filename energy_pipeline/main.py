# main_simple.py
from pipeline import run

# --- Define your paths here once ---
PATHS = {
    "inverter_yields":       "CSVs/inverter_yields.csv",
    "static_inverter_info":  "CSVs/static_inverter_info.csv",
    "sldc_events":           "CSVs/sldc_events.csv",
    "site_median_reference": "CSVs/site_median_reference.csv",
}
OUTPUT_PATH = "output/parquet"   # where to write parquet

def main():
    run(PATHS, OUTPUT_PATH, mode="overwrite")

if __name__ == "__main__":
    main()
