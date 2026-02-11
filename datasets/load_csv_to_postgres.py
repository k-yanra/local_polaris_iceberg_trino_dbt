import pandas as pd
from sqlalchemy import create_engine, text


engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:15435/postgres")

with engine.connect() as connection:
    # Use 'IF NOT EXISTS' for idempotency (prevents error if schema exists)
    connection.execute(text("CREATE SCHEMA IF NOT EXISTS flights"))
    connection.commit()

df = pd.read_csv("./datasets/airlines.csv")
print("Read airlines.csv")
# df.to_parquet("airlines_p.parquet", engine="pyarrow")
df.to_sql("airlines", schema="flights", con=engine, if_exists="replace", index=False, chunksize=10000)
print("Loaded airlines.csv to Postgres\n")

df = pd.read_csv("./datasets/airports.csv")
print("Read airports.csv")
# df.to_parquet("airports_p.parquet", engine="pyarrow")
df.to_sql("airports", schema="flights", con=engine, if_exists="replace", index=False, chunksize=1000)
print("Loaded airports.csv to Postgres\n")

df = pd.read_csv("./datasets/cancellation_codes.csv")
print("Read cancellation_codes.csv")
# df.to_parquet("cancellation_codes_p.parquet", engine="pyarrow")
df.to_sql("cancellation_codes", schema="flights", con=engine, if_exists="replace", index=False, chunksize=1000)
print("Loaded cancellation_codes.csv to Postgres\n")

dtype_dict = {
    "ORIGIN_AIRPORT": "string",
    "DESTINATION_AIRPORT": "string",
}
df = pd.read_csv("./datasets/flights_light_10k_rows.csv", dtype=dtype_dict)
print("Read flights_light_10k_rows.csv")
# df.to_parquet("flights_p.parquet", engine="pyarrow")
df.to_sql("flights", schema="flights", con=engine, if_exists="replace", index=False, chunksize=50000)
print("Loaded flights_light_10k_rows.csv to Postgres")
