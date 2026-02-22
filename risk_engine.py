"""
Risk Engine
Computes exposure metrics, revenue volatility, and risk scores from cleaned trip data.
Manual implementations: variance, density aggregation, sorting (no built-ins).
"""

import json
import csv
import math
from config import (
    CLEANED_TRIPS_CSV,
    ZONE_HOUR_METRICS_JSON,
    ZONE_REVENUE_METRICS_JSON,
    ZONE_RISK_SCORES_JSON,
    RISK_WEIGHTS,
    LATE_NIGHT_HOURS,
)


class RiskEngine:
    """Computes exposure metrics and risk scores from cleaned trip data."""

    def __init__(self):
        self.cleaned_trips = []
        self.zone_hour_metrics = {}
        self.zone_revenue_metrics = {}
        self.zone_risk_scores = {}
        self.trip_fares_by_zone = {}

    def load_cleaned_data(self):
        """Load cleaned trips from CSV."""
        print("\n" + "=" * 70)
        print("RISK ENGINE - FEATURE COMPUTATION")
        print("=" * 70)
        print("\nLoading cleaned trip data...")

        try:
            with open(CLEANED_TRIPS_CSV, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.cleaned_trips.append(row)

            print(f"✓ Loaded {len(self.cleaned_trips):,} cleaned records")
        except FileNotFoundError:
            print(f"✗ Error: Cleaned data file not found: {CLEANED_TRIPS_CSV}")
            print("  Run data_cleaning.py first")
        except Exception as e:
            print(f"✗ Error loading cleaned data: {e}")

    def _compute_manual_variance(self, values):
        """
        Manually compute variance and standard deviation (no numpy/statistics).
        Variance = sum((x - mean)^2) / n
        StdDev = sqrt(variance)
        """
        if not values or len(values) == 0:
            return 0.0, 0.0

        # Step 1: Calculate mean
        mean = sum(values) / len(values)

        # Step 2: Calculate sum of squared differences
        sum_squared_diff = 0
        for value in values:
            diff = value - mean
            sum_squared_diff += diff * diff

        # Step 3: Calculate variance
        variance = sum_squared_diff / len(values)

        # Step 4: Calculate standard deviation (square root)
        std_dev = math.sqrt(variance)

        return variance, std_dev

    def compute_exposure_density(self):
        """
        Compute zone-hour trip density (exposure score).
        Manual aggregation: no Counter, no pandas groupby.
        """
        print("\n" + "-" * 70)
        print("STEP 1: COMPUTING EXPOSURE DENSITY SCORES")
        print("-" * 70)

        # Manual aggregation using nested dictionaries
        zone_hour_trips = {}

        for trip in self.cleaned_trips:
            zone_id = int(trip["pulocation_id"])
            hour = int(trip["hour_of_day"])
            duration = float(trip["trip_duration_minutes"])

            # Create key for zone-hour combination
            key = (zone_id, hour)

            # Initialize if first time seeing this zone-hour
            if key not in zone_hour_trips:
                zone_hour_trips[key] = {
                    "count": 0,
                    "total_duration": 0.0,
                    "trips": []
                }

            # Aggregate
            zone_hour_trips[key]["count"] += 1
            zone_hour_trips[key]["total_duration"] += duration
            zone_hour_trips[key]["trips"].append(trip)

        # Convert to metrics dictionary
        for (zone_id, hour), data in zone_hour_trips.items():
            trip_count = data["count"]
            avg_duration = data["total_duration"] / trip_count if trip_count > 0 else 0

            self.zone_hour_metrics[(zone_id, hour)] = {
                "zone_id": zone_id,
                "hour": hour,
                "trip_count": trip_count,
                "avg_trip_duration": round(avg_duration, 2),
                "exposure_score": trip_count,  # Raw trip count as exposure
            }

        print(f"✓ Computed {len(self.zone_hour_metrics)} zone-hour combinations")
        print(f"  Sample: Zone 42, Hour 8 = {len([t for t in self.cleaned_trips if int(t['pulocation_id']) == 42 and int(t['hour_of_day']) == 8])} trips")

    def compute_revenue_volatility(self):
        """
        Compute revenue volatility per zone using manual variance calculation.
        Manual aggregation: no pandas, no numpy, no statistics library.
        """
        print("\n" + "-" * 70)
        print("STEP 2: COMPUTING REVENUE VOLATILITY (MANUAL VARIANCE)")
        print("-" * 70)

        # First pass: collect all fares per zone
        zone_fares = {}

        for trip in self.cleaned_trips:
            zone_id = int(trip["pulocation_id"])
            total_amount = float(trip["total_amount"])

            if zone_id not in zone_fares:
                zone_fares[zone_id] = []

            zone_fares[zone_id].append(total_amount)

        # Second pass: compute statistics for each zone
        for zone_id, fares in zone_fares.items():
            if not fares:
                continue

            # Manual mean calculation
            mean_fare = sum(fares) / len(fares)

            # Manual variance and std dev calculation
            variance, std_dev = self._compute_manual_variance(fares)

            # Stability score (inverse of volatility: low std_dev = high stability)
            # Normalize to 0-1 scale
            # Higher stability = lower std_dev
            max_possible_std = mean_fare * 2 if mean_fare > 0 else 1
            stability_score = max(0, 1 - (std_dev / max_possible_std))

            self.zone_revenue_metrics[zone_id] = {
                "zone_id": zone_id,
                "avg_revenue": round(mean_fare, 2),
                "revenue_variance": round(variance, 2),
                "revenue_std_dev": round(std_dev, 2),
                "stability_score": round(stability_score, 4),
            }

        print(f"✓ Computed revenue metrics for {len(self.zone_revenue_metrics)} zones")
        print(f"  Variance calculated manually using: sum((x - mean)^2) / n")
        print(f"  Stability score = 1 - (std_dev / max_std)")

    def compute_risk_scores(self):
        """
        Compute risk scores for each zone-hour combination.
        Risk = (density_weight × normalized_density) +
               (late_night_weight × late_night_factor) +
               (volatility_weight × normalized_volatility)
        """
        print("\n" + "-" * 70)
        print("STEP 3: COMPUTING RISK SCORES")
        print("-" * 70)

        # Find normalization factors
        max_trip_count = max(
            (m["trip_count"] for m in self.zone_hour_metrics.values()),
            default=1
        )

        max_volatility = max(
            (m["revenue_std_dev"] for m in self.zone_revenue_metrics.values()),
            default=1
        )

        # Preprocess normalization
        if max_volatility == 0:
            max_volatility = 1

        # Compute risk for each zone-hour combination
        for (zone_id, hour), metrics in self.zone_hour_metrics.items():
            # Step 1: Normalize trip density (0-1)
            trip_count = metrics["trip_count"]
            density_normalized = trip_count / max_trip_count if max_trip_count > 0 else 0

            # Step 2: Late night factor (1.0 if late night, else 0.0)
            late_night_factor = 1.0 if hour in LATE_NIGHT_HOURS else 0.0

            # Step 3: Normalize volatility (0-1)
            volatility_normalized = 0.0
            if zone_id in self.zone_revenue_metrics:
                std_dev = self.zone_revenue_metrics[zone_id]["revenue_std_dev"]
                volatility_normalized = std_dev / max_volatility if max_volatility > 0 else 0

            # Step 4: Weighted risk calculation
            risk_score = (
                RISK_WEIGHTS["density"] * density_normalized
                + RISK_WEIGHTS["late_night"] * late_night_factor
                + RISK_WEIGHTS["volatility"] * volatility_normalized
            )

            # Clamp to 0-1 range
            risk_score = max(0, min(1, risk_score))

            self.zone_risk_scores[(zone_id, hour)] = {
                "zone_id": zone_id,
                "hour": hour,
                "trip_count": trip_count,
                "density_component": round(RISK_WEIGHTS["density"] * density_normalized, 4),
                "late_night_component": round(RISK_WEIGHTS["late_night"] * late_night_factor, 4),
                "volatility_component": round(RISK_WEIGHTS["volatility"] * volatility_normalized, 4),
                "risk_score": round(risk_score, 4),
            }

        print(f"✓ Computed risk scores for {len(self.zone_risk_scores)} zone-hour combinations")
        print(f"  Formula: {RISK_WEIGHTS['density']:.1%} density + "
              f"{RISK_WEIGHTS['late_night']:.1%} late-night + "
              f"{RISK_WEIGHTS['volatility']:.1%} volatility")

        # Find highest & lowest risk
        if self.zone_risk_scores:
            highest_risk = max(
                self.zone_risk_scores.items(),
                key=lambda x: x[1]["risk_score"]
            )
            lowest_risk = min(
                self.zone_risk_scores.items(),
                key=lambda x: x[1]["risk_score"]
            )
            print(f"  Highest risk: Zone {highest_risk[0][0]}, Hour {highest_risk[0][1]} "
                  f"(score: {highest_risk[1]['risk_score']:.4f})")
            print(f"  Lowest risk: Zone {lowest_risk[0][0]}, Hour {lowest_risk[0][1]} "
                  f"(score: {lowest_risk[1]['risk_score']:.4f})")

    def write_metrics_to_json(self):
        """Write computed metrics to JSON files."""
        print("\n" + "-" * 70)
        print("WRITING METRICS TO JSON FILES")
        print("-" * 70)

        try:
            # Convert zone-hour tuples to strings for JSON serialization
            zone_hour_metrics_serialized = {}
            for (zone_id, hour), metrics in self.zone_hour_metrics.items():
                key = f"{zone_id}_{hour}"
                zone_hour_metrics_serialized[key] = metrics

            with open(ZONE_HOUR_METRICS_JSON, "w", encoding="utf-8") as f:
                json.dump(zone_hour_metrics_serialized, f, indent=2)
            print(f"✓ Wrote {len(zone_hour_metrics_serialized)} zone-hour metrics to:")
            print(f"  {ZONE_HOUR_METRICS_JSON}")

            with open(ZONE_REVENUE_METRICS_JSON, "w", encoding="utf-8") as f:
                json.dump(self.zone_revenue_metrics, f, indent=2)
            print(f"✓ Wrote {len(self.zone_revenue_metrics)} zone revenue metrics to:")
            print(f"  {ZONE_REVENUE_METRICS_JSON}")

            # Convert zone-hour tuples for risk scores
            zone_risk_scores_serialized = {}
            for (zone_id, hour), score in self.zone_risk_scores.items():
                key = f"{zone_id}_{hour}"
                zone_risk_scores_serialized[key] = score

            with open(ZONE_RISK_SCORES_JSON, "w", encoding="utf-8") as f:
                json.dump(zone_risk_scores_serialized, f, indent=2)
            print(f"✓ Wrote {len(zone_risk_scores_serialized)} risk scores to:")
            print(f"  {ZONE_RISK_SCORES_JSON}")

        except Exception as e:
            print(f"✗ Error writing metrics: {e}")

    def print_summary(self):
        """Print comprehensive summary."""
        print("\n" + "=" * 70)
        print("RISK ENGINE SUMMARY")
        print("=" * 70)
        print(f"Zone-hour combinations: {len(self.zone_hour_metrics)}")
        print(f"Unique zones: {len(set(zone_id for zone_id, _ in self.zone_hour_metrics.keys()))}")
        print(f"Risk scores computed: {len(self.zone_risk_scores)}")
        print(f"Revenue volatility analysis: {len(self.zone_revenue_metrics)} zones")
        print("=" * 70 + "\n")

    def run(self):
        """Execute full risk engine pipeline."""
        self.load_cleaned_data()
        if not self.cleaned_trips:
            print("✗ No data to process")
            return None

        self.compute_exposure_density()
        self.compute_revenue_volatility()
        self.compute_risk_scores()
        self.write_metrics_to_json()
        self.print_summary()

        return {
            "zone_hour_metrics": self.zone_hour_metrics,
            "zone_revenue_metrics": self.zone_revenue_metrics,
            "zone_risk_scores": self.zone_risk_scores,
        }


if __name__ == "__main__":
    engine = RiskEngine()
    results = engine.run()
    print(f"✓ Risk engine pipeline complete!")
    if results:
        print(f"  All metrics computed and saved to JSON files.")
