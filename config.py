"""
Configuration for Insurtech Data Pipeline
Centralized constants, thresholds, and paths for data cleaning and risk computation.
"""

import os

# ============================================================================
# FILE PATHS
# ============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# Create directories if they don't exist
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, LOGS_DIR]:
    os.makedirs(directory, exist_ok=True)

# Input files
YELLOW_TRIPDATA_CSV = os.path.join(RAW_DATA_DIR, "yellow_tripdata_2019-01.csv")
TAXI_ZONE_LOOKUP_CSV = os.path.join(RAW_DATA_DIR, "taxi_zone_lookup.csv")

# Output files
CLEANED_TRIPS_CSV = os.path.join(PROCESSED_DATA_DIR, "cleaned_trips.csv")
ZONE_HOUR_METRICS_JSON = os.path.join(PROCESSED_DATA_DIR, "zone_hour_metrics.json")
ZONE_REVENUE_METRICS_JSON = os.path.join(PROCESSED_DATA_DIR, "zone_revenue_metrics.json")
ZONE_RISK_SCORES_JSON = os.path.join(PROCESSED_DATA_DIR, "zone_risk_scores.json")
EXCLUDED_RECORDS_LOG = os.path.join(LOGS_DIR, "excluded_records.log")

# ============================================================================
# DATA VALIDATION THRESHOLDS
# ============================================================================
VALIDATION_THRESHOLDS = {
    "max_distance_miles": 50,       # NYC taxi trips beyond 50 miles are suspicious
    "min_fare": 0,                  # Negative fares are invalid
    "max_fare": 500,                # Fares over $500 are suspicious
    "min_trip_duration_minutes": 1, # Trips under 1 minute are invalid
    "max_trip_duration_minutes": 1440,  # Max 24 hours per trip
    "min_passenger_count": 1,       # Must have at least 1 passenger
    "max_passenger_count": 8,       # Reasonable upper bound
}

# Valid rate codes (standard NYC taxi rate codes)
VALID_RATE_CODES = [1, 2, 3, 4, 5]

# Valid payment types
VALID_PAYMENT_TYPES = [1, 2, 3, 4, 5]  # 1=credit, 2=cash, 3=no charge, 4=dispute, 5=unknown

# Valid store_and_fwd flags
VALID_STORE_FWD_FLAGS = ["Y", "N"]

# ============================================================================
# RISK SCORING WEIGHTS
# ============================================================================
RISK_WEIGHTS = {
    "density": 0.4,        # Trip density contribution (40%)
    "late_night": 0.3,     # Late night hour contribution (30%)
    "volatility": 0.3,     # Revenue volatility contribution (30%)
}

# Late night hours (higher risk periods)
LATE_NIGHT_HOURS = [22, 23, 0, 1, 2, 3, 4, 5]

# ============================================================================
# PROCESSING PARAMETERS
# ============================================================================
CHUNK_SIZE = 50000  # Process 50k rows at a time to avoid memory overflow
EXPECTED_ZONE_COUNT = 263  # Expected number of taxi zones in NYC
MAX_ROWS_TO_PROCESS = None  # Set to None to process all; set to number for testing

# ============================================================================
# FEATURE ENGINEERING DERIVATIONS
# ============================================================================
FEATURE_SETTINGS = {
    "trip_duration_minutes": "Calculated from dropoff_datetime - pickup_datetime",
    "hour_of_day": "Extracted from pickup_datetime (0-23)",
    "exposure_density_score": "Trip count per zone-hour combination",
    "revenue_volatility": "Standard deviation of fares per zone",
}

# ============================================================================
# DATABASE CONFIGURATION (For Task 2)
# ============================================================================
DB_PATH = os.path.join(DATA_DIR, "insurtech_taxi.db")
DB_TYPE = "sqlite"  # Change to 'postgresql' or 'mysql' as needed

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
LOG_FORMAT = "{index},{reason},{affected_fields}"
LOG_HEADER = "index,reason,affected_fields"
