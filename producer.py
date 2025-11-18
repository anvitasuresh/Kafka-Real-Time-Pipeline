import time
import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import numpy as np

fake = Faker()


def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculate distance in km using Haversine formula"""
    R = 6371  # Earth's radius in km

    lat1_rad = np.radians(lat1)
    lat2_rad = np.radians(lat2)
    delta_lat = np.radians(lat2 - lat1)
    delta_lon = np.radians(lon2 - lon1)

    a = (
        np.sin(delta_lat / 2) ** 2
        + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(delta_lon / 2) ** 2
    )
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    return R * c


def generate_synthetic_trip():
    """Generates synthetic ride-sharing trip data."""
    # Cities with coordinates
    cities = {
        "New York": {"lat": 40.7128, "lon": -74.0060},
        "Los Angeles": {"lat": 34.0522, "lon": -118.2437},
        "Chicago": {"lat": 41.8781, "lon": -87.6298},
        "Houston": {"lat": 29.7604, "lon": -95.3698},
        "Phoenix": {"lat": 33.4484, "lon": -112.0740},
        "San Francisco": {"lat": 37.7749, "lon": -122.4194},
        "Boston": {"lat": 42.3601, "lon": -71.0589},
        "Seattle": {"lat": 47.6062, "lon": -122.3321},
    }

    statuses = ["Completed", "Cancelled", "In Progress"]

    # Select random city
    city_name = random.choice(list(cities.keys()))
    city_coords = cities[city_name]

    # Generate pickup location (random offset within city)
    pickup_lat = city_coords["lat"] + random.uniform(-0.09, 0.09)
    pickup_lon = city_coords["lon"] + random.uniform(-0.09, 0.09)

    # Generate dropoff location (within reasonable distance)
    dropoff_lat = pickup_lat + random.uniform(-0.05, 0.05)
    dropoff_lon = pickup_lon + random.uniform(-0.05, 0.05)

    # Calculate distance
    distance_km = calculate_distance(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon)
    distance_km = max(0.5, distance_km)  # Minimum 0.5 km

    # Calculate duration (avg speed 30-40 km/h in city traffic)
    avg_speed = random.uniform(30, 40)
    base_duration = (distance_km / avg_speed) * 60  # minutes
    duration_minutes = base_duration * random.uniform(0.8, 1.5)

    # Calculate surge multiplier based on time
    hour = datetime.now().hour
    if (7 <= hour <= 9) or (17 <= hour <= 19):  # Rush hours
        surge = round(random.uniform(1.5, 2.5), 2)
    elif hour >= 23 or hour <= 3:  # Late night
        surge = round(random.uniform(1.3, 1.8), 2)
    else:  # Normal hours
        surge = round(random.uniform(1.0, 1.2), 2)

    # Calculate fare
    base_fare = 3.0  # Base fare
    per_km_rate = 1.5
    per_minute_rate = 0.3

    fare = (
        base_fare + (distance_km * per_km_rate) + (duration_minutes * per_minute_rate)
    )
    total_fare = round(fare * surge, 2)

    # Trip status (weighted: 85% completed, 10% cancelled, 5% in progress)
    status = random.choices(statuses, weights=[85, 10, 5], k=1)[0]

    # Ratings (only for completed trips)
    driver_rating = (
        round(random.uniform(4.0, 5.0), 1) if status == "Completed" else None
    )
    passenger_rating = (
        round(random.uniform(4.0, 5.0), 1) if status == "Completed" else None
    )

    # Inject anomalies (5% chance)
    is_anomaly = False
    if random.random() < 0.05:
        anomaly_type = random.choice(["fare", "duration", "distance"])
        if anomaly_type == "fare":
            total_fare *= random.uniform(3, 5)
        elif anomaly_type == "duration":
            duration_minutes *= random.uniform(2, 4)
        elif anomaly_type == "distance":
            distance_km *= random.uniform(1.5, 2.5)
        is_anomaly = True
        total_fare = round(total_fare, 2)

    return {
        "trip_id": str(uuid.uuid4())[:8],
        "driver_id": f"driver_{random.randint(1, 100):04d}",
        "passenger_id": str(uuid.uuid4()),
        "status": status,
        "city": city_name,
        "pickup_lat": round(pickup_lat, 6),
        "pickup_lon": round(pickup_lon, 6),
        "dropoff_lat": round(dropoff_lat, 6),
        "dropoff_lon": round(dropoff_lon, 6),
        "distance_km": round(distance_km, 2),
        "duration_minutes": round(duration_minutes, 2),
        "base_fare": round(fare, 2),
        "surge_multiplier": surge,
        "total_fare": total_fare,
        "request_time": datetime.now().isoformat(),
        "driver_rating": driver_rating,
        "passenger_rating": passenger_rating,
        "is_anomaly": is_anomaly,
    }


def run_producer():
    """Kafka producer that sends synthetic trips to the 'trips' topic."""
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")

        count = 0
        while True:
            trip = generate_synthetic_trip()
            print(f"[Producer] Sending trip #{count}: {trip['trip_id']}")
            print(
                f"  City: {trip['city']} | Distance: {trip['distance_km']}km | "
                f"Fare: ${trip['total_fare']} | Status: {trip['status']}"
            )

            future = producer.send("trips", value=trip)
            record_metadata = future.get(timeout=10)
            print(
                f"[Producer] ✓ Sent to partition {record_metadata.partition} at offset {record_metadata.offset}"
            )

            producer.flush()
            count += 1

            sleep_time = random.uniform(0.5, 2.0)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_producer()
