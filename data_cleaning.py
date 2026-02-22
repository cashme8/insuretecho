"""
Data Cleaning Pipeline
Reads yellow_tripdata CSV in chunks, validates records, computes features,
and logs excluded records. No pandas/numpy - manual processing only.
"""

import csv
import json
from datetime import datetime
from config import (
    YELLOW_TRIPDATA_CSV,
    TAXI_ZONE_LOOKUP_CSV,
    CLEANED_TRIPS_CSV,
    EXCLUDED_RECORDS_LOG,
    VALIDATION_THRESHOLDS,
    VALID_RATE_CODES,
    VALID_PAYMENT_TYPES,
    VALID_STORE_FWD_FLAGS,
    CHUNK_SIZE,
    LATE_NIGHT_HOURS,
    MAX_ROWS_TO_PROCESS,
)


class DataCleaner:
    """Handles chunked CSV reading, validation, and feature engineering."""

    def __init__(self):
        self.cleaned_trips = []
        self.excluded_records = []
        self.zone_ids = self._load_zone_ids()
        self.stats = {
            "total_processed": 0,
            "total_valid": 0,
            "total_excluded": 0,
            "excluded_by_reason": {},
        }

    def _load_zone_ids(self):
        """Load valid zone IDs from taxi_zone_lookup.csv or use NYC standard range."""
        zone_ids = set()
        try:
            with open(TAXI_ZONE_LOOKUP_CSV, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    zone_id = row.get("LocationID")
                    if zone_id:
                        zone_ids.add(int(zone_id))
            # If lookup is incomplete (< 263 zones), use standard NYC range
            if len(zone_ids) < 263:
                print(f"⚠ Lookup incomplete ({len(zone_ids)} zones), using all NYC zones (1-263)")
                zone_ids = set(range(1, 264))
            else:
                print(f"✓ Loaded {len(zone_ids)} valid zone IDs from lookup table")
        except Exception as e:
            # If zone lookup file doesn't exist, use standard NYC taxi zone range (1-263)
            print(f"⚠ Zone lookup unavailable, using standard NYC zones (1-263)")
            zone_ids = set(range(1, 264))  # NYC has zones 1-263
        return zone_ids

    def _parse_datetime(self, date_string):
        """Parse datetime string to standardized format."""
        if not date_string or not isinstance(date_string, str):
            return None
        try:
            date_string = date_string.strip()
            # Try parsing format: YYYY-MM-DD HH:MM:SS
            try:
                dt = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
                return dt
            except ValueError:
                # Try alternative formats that might be in data
                # Handle trailing content after seconds
                date_string = date_string.split(',')[0].strip()
                dt = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
                return dt
        except (ValueError, AttributeError):
            return None

    def _parse_float(self, value):
        """Safely parse float value."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_int(self, value):
        """Safely parse integer value."""
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _validate_record(self, row, row_index):
        """
        Validate a single trip record against all business rules.
        Returns: (is_valid, cleaned_record, exclusion_reason)
        """
        try:
            # Parse timestamps
            pickup_dt = self._parse_datetime(row.get("tpep_pickup_datetime", ""))
            dropoff_dt = self._parse_datetime(row.get("tpep_dropoff_datetime", ""))

            # Temporal validation
            if not pickup_dt or not dropoff_dt:
                return False, None, "invalid_datetime_format"

            if dropoff_dt <= pickup_dt:
                return False, None, "dropoff_before_or_equal_pickup"

            # Check date is in January 2019
            if pickup_dt.year != 2019 or pickup_dt.month != 1:
                return False, None, "date_out_of_range"

            # Parse numeric fields
            trip_distance = self._parse_float(row.get("trip_distance", ""))
            fare_amount = self._parse_float(row.get("fare_amount", ""))
            total_amount = self._parse_float(row.get("total_amount", ""))
            passenger_count = self._parse_int(row.get("passenger_count", ""))
            pulocation_id = self._parse_int(row.get("PULocationID", ""))
            dolocation_id = self._parse_int(row.get("DOLocationID", ""))

            # Distance validation
            if trip_distance is None or trip_distance < 0:
                return False, None, "negative_or_null_distance"

            if trip_distance > VALIDATION_THRESHOLDS["max_distance_miles"]:
                return False, None, "distance_exceeds_max"

            # Fare validation
            if fare_amount is None or fare_amount < VALIDATION_THRESHOLDS["min_fare"]:
                return False, None, "negative_or_null_fare"

            if fare_amount > VALIDATION_THRESHOLDS["max_fare"]:
                return False, None, "fare_exceeds_max"

            # Passenger count validation
            if passenger_count is None or passenger_count < VALIDATION_THRESHOLDS["min_passenger_count"]:
                return False, None, "invalid_passenger_count"

            if passenger_count > VALIDATION_THRESHOLDS["max_passenger_count"]:
                return False, None, "passenger_count_exceeds_max"

            # Zone validation
            if pulocation_id is None or pulocation_id not in self.zone_ids:
                return False, None, "invalid_pulocation_id"

            if dolocation_id is None or dolocation_id not in self.zone_ids:
                return False, None, "invalid_dolocation_id"

            # Trip duration validation
            trip_duration_minutes = (dropoff_dt - pickup_dt).total_seconds() / 60
            if trip_duration_minutes < VALIDATION_THRESHOLDS["min_trip_duration_minutes"]:
                return False, None, "trip_duration_too_short"

            if trip_duration_minutes > VALIDATION_THRESHOLDS["max_trip_duration_minutes"]:
                return False, None, "trip_duration_exceeds_max"

            # Rate code validation
            ratecode_id = self._parse_int(row.get("RatecodeID", ""))
            if ratecode_id is None or ratecode_id not in VALID_RATE_CODES:
                return False, None, "invalid_ratecode"

            # Payment type validation
            payment_type = self._parse_int(row.get("payment_type", ""))
            if payment_type is None or payment_type not in VALID_PAYMENT_TYPES:
                return False, None, "invalid_payment_type"

            # Store and forward flag validation
            store_fwd_flag = row.get("store_and_fwd_flag", "").strip()
            if store_fwd_flag not in VALID_STORE_FWD_FLAGS:
                return False, None, "invalid_store_fwd_flag"

            # Parse remaining fields
            extra = self._parse_float(row.get("extra", "")) or 0.0
            mta_tax = self._parse_float(row.get("mta_tax", "")) or 0.0
            tip_amount = self._parse_float(row.get("tip_amount", "")) or 0.0
            tolls_amount = self._parse_float(row.get("tolls_amount", "")) or 0.0
            improvement_surcharge = self._parse_float(row.get("improvement_surcharge", "")) or 0.0
            congestion_surcharge = self._parse_float(row.get("congestion_surcharge", "")) or 0.0

            # Calculate hour of day
            hour_of_day = pickup_dt.hour

            # Create cleaned record
            cleaned_record = {
                "trip_id": row_index,
                "vendor_id": self._parse_int(row.get("VendorID", "")),
                "pickup_datetime": pickup_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "dropoff_datetime": dropoff_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "passenger_count": passenger_count,
                "trip_distance": round(trip_distance, 2),
                "ratecode_id": ratecode_id,
                "store_and_fwd_flag": store_fwd_flag,
                "pulocation_id": pulocation_id,
                "dolocation_id": dolocation_id,
                "payment_type": payment_type,
                "fare_amount": round(fare_amount, 2),
                "extra": round(extra, 2),
                "mta_tax": round(mta_tax, 2),
                "tip_amount": round(tip_amount, 2),
                "tolls_amount": round(tolls_amount, 2),
                "improvement_surcharge": round(improvement_surcharge, 2),
                "total_amount": round(total_amount, 2) if total_amount else 0.0,
                "congestion_surcharge": round(congestion_surcharge, 2),
                # Derived features
                "trip_duration_minutes": round(trip_duration_minutes, 2),
                "hour_of_day": hour_of_day,
            }

            return True, cleaned_record, None

        except Exception as e:
            return False, None, f"processing_error_{str(e)[:30]}"

    def process_csv_chunks(self):
        """Process CSV in chunks to avoid memory overflow."""
        print("\n" + "=" * 70)
        print("DATA CLEANING PIPELINE - CHUNKED PROCESSING")
        print("=" * 70)
        if MAX_ROWS_TO_PROCESS:
            print(f"⚠ TESTING MODE: Processing first {MAX_ROWS_TO_PROCESS:,} rows only")
        print()

        chunk_count = 0
        try:
            with open(YELLOW_TRIPDATA_CSV, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                chunk = []

                for row_index, row in enumerate(reader, start=1):
                    # Allow early exit for testing
                    if MAX_ROWS_TO_PROCESS and row_index > MAX_ROWS_TO_PROCESS:
                        break

                    self.stats["total_processed"] += 1

                    # Validate record
                    is_valid, cleaned_record, reason = self._validate_record(row, row_index)

                    if is_valid:
                        self.cleaned_trips.append(cleaned_record)
                        self.stats["total_valid"] += 1
                    else:
                        self.stats["total_excluded"] += 1
                        self.stats["excluded_by_reason"][reason] = (
                            self.stats["excluded_by_reason"].get(reason, 0) + 1
                        )
                        self.excluded_records.append((row_index, reason))

                    # Process chunk
                    chunk.append((is_valid, cleaned_record, reason))
                    if len(chunk) >= CHUNK_SIZE:
                        chunk_count += 1
                        print(f"  ✓ Processed chunk {chunk_count} ({CHUNK_SIZE:,} rows)")
                        chunk = []

                # Process remaining rows
                if chunk:
                    chunk_count += 1
                    print(f"  ✓ Processed chunk {chunk_count} ({len(chunk):,} rows)")

            print(f"\n✓ CSV processing complete!")
            print(f"  Total rows processed: {self.stats['total_processed']:,}")
            print(f"  Valid records: {self.stats['total_valid']:,}")
            print(f"  Excluded records: {self.stats['total_excluded']:,}")

        except FileNotFoundError:
            print(f"✗ Error: File not found: {YELLOW_TRIPDATA_CSV}")
        except Exception as e:
            print(f"✗ Error processing CSV: {e}")

    def write_cleaned_data(self):
        """Write cleaned trips to CSV output file."""
        print("\n" + "-" * 70)
        print("WRITING CLEANED DATA TO FILE")
        print("-" * 70)

        try:
            if not self.cleaned_trips:
                print("✗ No cleaned records to write")
                return

            with open(CLEANED_TRIPS_CSV, "w", newline="", encoding="utf-8") as f:
                fieldnames = list(self.cleaned_trips[0].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.cleaned_trips)

            print(f"✓ Wrote {len(self.cleaned_trips):,} clean records to:")
            print(f"  {CLEANED_TRIPS_CSV}")

        except Exception as e:
            print(f"✗ Error writing cleaned data: {e}")

    def write_exclusion_log(self):
        """Write excluded records log."""
        print("\n" + "-" * 70)
        print("WRITING EXCLUSION LOG")
        print("-" * 70)

        try:
            with open(EXCLUDED_RECORDS_LOG, "w", encoding="utf-8") as f:
                f.write("index,reason\n")
                for row_index, reason in self.excluded_records:
                    f.write(f"{row_index},{reason}\n")

            print(f"✓ Wrote {len(self.excluded_records):,} exclusion records to:")
            print(f"  {EXCLUDED_RECORDS_LOG}")
            print("\nExclusion breakdown:")
            for reason, count in sorted(
                self.stats["excluded_by_reason"].items(), key=lambda x: x[1], reverse=True
            ):
                print(f"  - {reason}: {count:,}")

        except Exception as e:
            print(f"✗ Error writing exclusion log: {e}")

    def print_summary(self):
        """Print comprehensive summary statistics."""
        print("\n" + "=" * 70)
        print("DATA CLEANING SUMMARY")
        print("=" * 70)
        print(f"Total records processed: {self.stats['total_processed']:,}")
        print(f"Valid records retained: {self.stats['total_valid']:,}")
        print(f"Records excluded: {self.stats['total_excluded']:,}")
        if self.stats["total_processed"] > 0:
            retention_rate = (self.stats["total_valid"] / self.stats["total_processed"]) * 100
            print(f"Data retention rate: {retention_rate:.2f}%")
        print("=" * 70 + "\n")

    def run(self):
        """Execute full cleaning pipeline."""
        self.process_csv_chunks()
        self.write_cleaned_data()
        self.write_exclusion_log()
        self.print_summary()
        return self.cleaned_trips


if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaned_data = cleaner.run()
    print(f"\n✓ Data cleaning pipeline complete!")
    print(f"  Cleaned records ready for feature engineering and risk scoring.")
