"""
Quick Data Cleaning Script - Test Version
Processes first 500 records with simplified validation
Used for rapid testing of data cleaning pipeline before full processing
"""

import csv
import os
from datetime import datetime
from config import (
    YELLOW_TRIPDATA_CSV,
    CLEANED_TRIPS_CSV,
    EXCLUDED_RECORDS_LOG,
    MAX_ROWS_TO_PROCESS,
)

print("=" * 70)
print(f"QUICK DATA CLEANING TEST - MAX {MAX_ROWS_TO_PROCESS or 'ALL'} RECORDS")
print("=" * 70)

cleaned_count = 0
excluded_count = 0
valid_zones = set(range(1, 264))

try:
    with open(YELLOW_TRIPDATA_CSV, "r", encoding="utf-8") as infile, \
         open(CLEANED_TRIPS_CSV, "w", newline="", encoding="utf-8") as outfile, \
         open(EXCLUDED_RECORDS_LOG, "w", encoding="utf-8") as logfile:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        logfile.write("index,reason\n")

        for idx, row in enumerate(reader, start=1):
            if idx > MAX_ROWS_TO_PROCESS:
                break

            # Simple validation
            errors = []

            # Check required fields exist
            try:
                pu_time = row.get("tpep_pickup_datetime", "").strip()
                do_time = row.get("tpep_dropoff_datetime", "").strip()
                pu_zone = int(row.get("PULocationID", 0))
                do_zone = int(row.get("DOLocationID", 0))
                distance = float(row.get("trip_distance", 0))
                fare = float(row.get("fare_amount", 0))
                pcount = int(row.get("passenger_count", 0))

                # Basic checks
                if not pu_time or not do_time:
                    errors.append("invalid_datetime")
                elif pu_zone not in valid_zones:
                    errors.append("invalid_pu_zone")
                elif do_zone not in valid_zones:
                    errors.append("invalid_do_zone")
                elif distance < 0:
                    errors.append("negative_distance")
                elif fare < 0:
                    errors.append("negative_fare")
                elif pcount <= 0:
                    errors.append("zero_passengers")
                elif distance > 50:
                    errors.append("distance_exceeds_max")
                elif fare > 500:
                    errors.append("fare_exceeds_max")

                if errors:
                    excluded_count += 1
                    logfile.write(f"{idx},{errors[0]}\n")
                else:
                    writer.writerow(row)
                    cleaned_count += 1

                if idx % 100 == 0:
                    print(f"  ✓ Processed {idx} records...")

            except Exception as e:
                excluded_count += 1
                logfile.write(f"{idx},parse_error\n")

    print(f"\n✓ Data cleaning complete!")
    print(f"  Total processed: {cleaned_count + excluded_count}")
    print(f"  Valid records: {cleaned_count}")
    print(f"  Excluded: {excluded_count}")
    print(f"\n✓ Output files:")
    print(f"  Cleaned: {CLEANED_TRIPS_CSV}")
    print(f"  Log: {EXCLUDED_RECORDS_LOG}")

except FileNotFoundError as e:
    print(f"✗ Error: {e}")
except Exception as e:
    print(f"✗ Unexpected error: {e}")
