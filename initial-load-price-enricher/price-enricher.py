import os
import re
import pandas as pd
from pathlib import Path

INPUT_DIR = Path("./price")
OUTPUT_DIR = Path("./price")

def extract_year(filename: str):
    """
    Extract the start year from filenames like:
    DayAheadPrices_DK1_202301010000-202401010000.csv
    → returns 2023
    """
    match = re.search(r"_(\d{4})\d{8}-", filename)
    return int(match.group(1)) if match else None

def load_and_transform(filepath: Path):
    """Load a CSV and convert it to the desired schema."""
    df = pd.read_csv(filepath)

    # Extract timestamp (first date in MTU (UTC))
    df["timestamp"] = df["MTU (UTC)"].str.extract(r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})")
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%d/%m/%Y %H:%M:%S", utc=True)

    # Extract DK area number (1 or 2)
    df["dkArea"] = df["Area"].str.extract(r"DK(\d)").astype(int)

    # Rename price column
    df = df.rename(columns={"Day-ahead Price (EUR/MWh)": "price (EUR/MWh)"})

    # Keep selected columns
    df = df[["timestamp", "dkArea", "price (EUR/MWh)"]]

    return df

def main():
    # Find all CSVs
    files = list(INPUT_DIR.glob("*.csv"))

    # Group by year
    year_to_files = {}
    for f in files:
        year = extract_year(f.name)
        if year:
            year_to_files.setdefault(year, []).append(f)

    # Process each year
    for year, file_list in year_to_files.items():
        print(f"Processing year {year} with files: {[f.name for f in file_list]}")

        merged_df = pd.concat(
            (load_and_transform(f) for f in file_list),
            ignore_index=True
        ).sort_values("timestamp")

        output_path = OUTPUT_DIR / f"{year}.csv"
        merged_df.to_csv(output_path, index=False)
        print(f"→ Saved merged file: {output_path}")

if _name_ == "_main_":
    main()