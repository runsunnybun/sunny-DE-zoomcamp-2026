import pandas as pd
from sqlalchemy import create_engine

df = pd.read_parquet('green_tripdata_2025-11.parquet')

# FORCE lowercase column names (fixes Postgres quoting)
df.columns = df.columns.str.lower()
df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

print(f"Shape: {df.shape}")
print("Final columns:", df.columns.tolist())

engine = create_engine('postgresql://postgres:postgres@localhost:5433/ny_taxi')

# Nuclear option: recreate table from DataFrame
df.head(0).to_sql('green_taxi_trips', engine, if_exists='replace', index=False)

# Bulk load
df.to_sql('green_taxi_trips', engine, if_exists='append', index=False, chunksize=5000)

print(f"✅ Loaded {len(df):,} trips!")

queries = {
    'q3_short_trips': """
       SELECT COUNT(*) as short_trips_le_1_mile FROM green_taxi_trips 
       WHERE lpep_pickup_datetime >= '2025-11-01' 
            AND lpep_pickup_datetime < '2025-12-01'
            AND trip_distance <= 1;

    """,
    'q4_longest_trip_pu_day': """
        SELECT DATE(lpep_pickup_datetime) as pickup_date,
            MAX(trip_distance) as max_distance
       FROM green_taxi_trips 
       WHERE trip_distance < 100
            AND lpep_pickup_datetime >= '2025-11-01' 
            AND lpep_pickup_datetime < '2025-12-01'
       GROUP BY DATE(lpep_pickup_datetime)
       ORDER BY max_distance DESC
       LIMIT 1;

    """,
    'q5_max_total_amount_pu_time': """
        SELECT tz.Zone,
            SUM(gt.total_amount) as total_revenue
        FROM green_taxi_trips gt
        JOIN taxi_zones tz ON gt.pulocationid = tz.LocationID
        WHERE DATE(gt.lpep_pickup_datetime) = '2025-11-18'
        GROUP BY tz.Zone
        ORDER BY total_revenue DESC
        LIMIT 1;

    """,
    'q6_largest_tip_dropoff_from_East_Harlem_North': """
       SELECT tz_dropoff.Zone as dropoff_zone,
            MAX(gt.tip_amount) as max_tip
       FROM green_taxi_trips gt
       JOIN taxi_zones tz_pickup ON gt.PULocationID = tz_pickup.LocationID
       JOIN taxi_zones tz_dropoff ON gt.DOLocationID = tz_dropoff.LocationID
       WHERE tz_pickup.Zone = 'East Harlem North'
            AND gt.lpep_pickup_datetime >= '2025-11-01' 
            AND gt.lpep_pickup_datetime < '2025-12-01'
       GROUP BY tz_dropoff.Zone
       ORDER BY max_tip DESC
       LIMIT 1;

    """

}

for name, sql in queries.items():
    df = pd.read_sql(sql, engine)
    df.to_csv(f'{name}.csv', index=False)
    print(f"Saved {name}: {df.iloc[0,0]}")

