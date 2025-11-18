import json
import psycopg2
from kafka import KafkaConsumer


def run_consumer():
    """Consumes messages from Kafka and inserts them into PostgreSQL."""
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "trips",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="trips-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5433",  # Updated to match your docker-compose
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        # Create trips table with all the ride-sharing fields
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trips (
                trip_id VARCHAR(50) PRIMARY KEY,
                driver_id VARCHAR(50),
                passenger_id VARCHAR(100),
                status VARCHAR(50),
                city VARCHAR(100),
                pickup_lat NUMERIC(10, 6),
                pickup_lon NUMERIC(10, 6),
                dropoff_lat NUMERIC(10, 6),
                dropoff_lon NUMERIC(10, 6),
                distance_km NUMERIC(10, 2),
                duration_minutes NUMERIC(10, 2),
                base_fare NUMERIC(10, 2),
                surge_multiplier NUMERIC(4, 2),
                total_fare NUMERIC(10, 2),
                request_time TIMESTAMP,
                driver_rating NUMERIC(3, 1),
                passenger_rating NUMERIC(3, 1),
                is_anomaly BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        print("[Consumer] ✓ Table 'trips' ready.")
        print("[Consumer] 🎧 Listening for messages...\n")

        message_count = 0
        for message in consumer:
            try:
                trip_data = message.value

                insert_query = """
                    INSERT INTO trips (
                        trip_id, driver_id, passenger_id, status, city,
                        pickup_lat, pickup_lon, dropoff_lat, dropoff_lon,
                        distance_km, duration_minutes, base_fare,
                        surge_multiplier, total_fare, request_time,
                        driver_rating, passenger_rating, is_anomaly
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (trip_id) DO NOTHING;
                """

                cur.execute(
                    insert_query,
                    (
                        trip_data["trip_id"],
                        trip_data["driver_id"],
                        trip_data["passenger_id"],
                        trip_data["status"],
                        trip_data["city"],
                        trip_data["pickup_lat"],
                        trip_data["pickup_lon"],
                        trip_data["dropoff_lat"],
                        trip_data["dropoff_lon"],
                        trip_data["distance_km"],
                        trip_data["duration_minutes"],
                        trip_data["base_fare"],
                        trip_data["surge_multiplier"],
                        trip_data["total_fare"],
                        trip_data["request_time"],
                        trip_data.get("driver_rating"),
                        trip_data.get("passenger_rating"),
                        trip_data.get("is_anomaly", False),
                    ),
                )

                message_count += 1

                # Show anomaly flag if present
                anomaly_flag = " ⚠️ ANOMALY" if trip_data.get("is_anomaly") else ""

                print(
                    f"[Consumer] ✓ #{message_count} Inserted trip {trip_data['trip_id']}"
                )
                print(
                    f"  {trip_data['city']} | {trip_data['distance_km']}km | "
                    f"${trip_data['total_fare']} | {trip_data['status']}{anomaly_flag}"
                )

            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_consumer()
