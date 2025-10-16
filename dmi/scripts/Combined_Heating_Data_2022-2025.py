import requests
import csv
import sys
from datetime import datetime


START_YEAR = 2022  # Start from 2022 (data begins September 2022)
END_YEAR = 2025    # Go up to 2025 to get the latest data
DESKTOP_PATH = "/Users/asn-mac/Desktop"
COMBINED_OUTPUT_FILE = f"{DESKTOP_PATH}/private_heating_consumption_2022-2025_combined.csv"

BASE_URL = "https://api.energidataservice.dk/dataset/PrivateConsumptionHeatingHour"


def fetch_month_data(year, month):
    """Henter data for en specifik måned."""
    # Calculate month boundaries
    if month == 12:
        next_month = 1
        next_year = year + 1
    else:
        next_month = month + 1
        next_year = year
    
    start = f"{year}-{month:02d}-01T00:00"
    end = f"{next_year}-{next_month:02d}-01T00:00"
    
    print(f"[INFO] Fetching data for {year}-{month:02d}...")
    
    all_records = []
    offset = 0
    limit = 20000
    
    while True:
        print(f"[INFO] {year}-{month:02d}: Fetching batch with offset {offset}...")
        
        params = {
            "start": start,
            "end": end,
            "timezone": "dk",
            "limit": limit,
            "offset": offset
        }

        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            records = data.get("records", [])
            
            if not records:
                print(f"[INFO] {year}-{month:02d}: No data found.")
                break
                
            all_records.extend(records)
            print(f"[INFO] {year}-{month:02d}: Fetched {len(records)} records. Total: {len(all_records)}")
            
            if len(records) < limit:
                print(f"[INFO] {year}-{month:02d}: Finished.")
                break
                
            offset += limit
            
        except Exception as e:
            print(f"[WARNING] {year}-{month:02d}: Error during fetch: {e}")
            break
    
    if all_records:
        print(f"[INFO] {year}-{month:02d}: Total {len(all_records)} data points.")
    return all_records


def fetch_all_years_combined():
    """Fetches data for all years and months and combines them into one large dataset."""
    print(f"[INFO] Starting download of combined dataset from {START_YEAR} to {END_YEAR}...")
    
    all_combined_records = []
    total_months_processed = 0
    successful_months = 0
    
    for year in range(START_YEAR, END_YEAR + 1):
        print(f"\n[INFO] ======== STARTING YEAR {year} ========")
        
        # For 2022, start from September (month 9) since earlier months have no data
        start_month = 9 if year == 2022 else 1
        
        for month in range(start_month, 13):  # Up to December
            total_months_processed += 1
            month_records = fetch_month_data(year, month)
            
            if month_records:
                # Save individual month file
                month_filename = f"{DESKTOP_PATH}/heating_consumption_{year}_{month:02d}.csv"
                save_to_csv(month_records, month_filename)
                print(f"[INFO] Saved month {year}-{month:02d} to {month_filename}")
                
                all_combined_records.extend(month_records)
                successful_months += 1
            
            current_total = len(all_combined_records)
            print(f"[INFO] Progress: {total_months_processed} months processed, {successful_months} with data. Total records: {current_total:,}")
        
        print(f"[INFO] ======== FINISHED YEAR {year} ========")
    
    print(f"\n[INFO] COMBINED RESULTS:")
    print(f"[INFO] - Processed months: {total_months_processed}")
    print(f"[INFO] - Months with data: {successful_months}")
    print(f"[INFO] - Total records: {len(all_combined_records):,}")
    
    return all_combined_records


def save_to_csv(records, filename):
    """Saves data to CSV."""
    if not records:
        print("[WARNING] No data to save.")
        return

    # Find column headers
    keys = sorted(records[0].keys())
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)

    print(f"[INFO] Saved data to file: {filename}")


def main():
    try:
        # Fetch all years combined
        all_records = fetch_all_years_combined()
        
        if all_records:
            # Save the massive combined file
            save_to_csv(all_records, COMBINED_OUTPUT_FILE)
            print(f"\n[SUCCESS] Saved complete combined dataset to {COMBINED_OUTPUT_FILE}")
            print(f"[SUCCESS] Total records spanning {START_YEAR}-{END_YEAR}: {len(all_records):,}")
            
            # Calculate file size estimate
            estimated_size_mb = len(all_records) * 200 / (1024 * 1024)  # Rough estimate
            print(f"[INFO] Estimated file size: ~{estimated_size_mb:.1f} MB")
        else:
            print("[WARNING] No data found for any months!")
        
        print("\n[DONE] Finished combined download!")
        
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] API error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()