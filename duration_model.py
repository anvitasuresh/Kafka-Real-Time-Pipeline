"""
Simple Trip Duration Prediction Model
Uses Random Forest to predict trip duration based on distance and surge
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import joblib
import psycopg2
from psycopg2.extras import RealDictCursor


def load_data_from_db():
    """Load completed trips from database"""
    conn = psycopg2.connect(
        dbname="kafka_db",
        user="kafka_user",
        password="kafka_password",
        host="localhost",
        port="5433",
    )

    query = """
        SELECT distance_km, surge_multiplier, duration_minutes, city
        FROM trips 
        WHERE status = 'Completed' 
        AND duration_minutes > 0 
        AND distance_km > 0
    """

    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query)
    data = cur.fetchall()

    cur.close()
    conn.close()

    return pd.DataFrame(data)


def train_model():
    """Train the duration prediction model"""
    print("=" * 60)
    print("TRAINING TRIP DURATION PREDICTION MODEL")
    print("=" * 60)

    # Load data
    print("\n📊 Loading data from database...")
    df = load_data_from_db()

    if len(df) < 50:
        print(f"❌ Not enough data! Need at least 50 trips, have {len(df)}")
        print("   Keep the producer running to collect more trips.")
        return None

    print(f"✅ Loaded {len(df)} completed trips")

    # Prepare features
    X = df[["distance_km", "surge_multiplier"]].values
    y = df["duration_minutes"].values

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Train model
    print("\n🔄 Training Random Forest model...")
    model = RandomForestRegressor(
        n_estimators=100, max_depth=10, random_state=42, n_jobs=-1
    )
    model.fit(X_train, y_train)

    # Evaluate
    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)
    predictions = model.predict(X_test)
    mae = mean_absolute_error(y_test, predictions)

    print("\n✅ Model Training Complete!")
    print(f"   Train R² Score: {train_score:.3f}")
    print(f"   Test R² Score: {test_score:.3f}")
    print(f"   Mean Absolute Error: {mae:.2f} minutes")
    print(f"   Average actual duration: {y_test.mean():.2f} minutes")

    # Save model
    joblib.dump(model, "duration_model.pkl")
    print("\n💾 Model saved to 'duration_model.pkl'")

    return model


def predict_duration(distance_km, surge_multiplier):
    """Predict trip duration for given distance and surge"""
    try:
        model = joblib.load("duration_model.pkl")
        prediction = model.predict([[distance_km, surge_multiplier]])
        return prediction[0]
    except:
        return None


if __name__ == "__main__":
    train_model()
