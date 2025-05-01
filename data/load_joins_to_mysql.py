import pandas as pd
import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="dsci351"
)
cursor = conn.cursor()

ev_df = pd.read_csv("electric_vehicle_cafv.csv")
ev_df = ev_df.rename(columns={
    "VIN (1-10)": "vin",
    "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "cafv_eligibility"
})

ev_df = ev_df.drop_duplicates(subset="vin")

for _, row in ev_df.iterrows():
    cursor.execute("INSERT INTO electric_vehicle_cafv (vin, cafv_eligibility) VALUES (%s, %s)", tuple(row))

imdb_df = pd.read_csv("imdb_runtime.csv")
imdb_df = imdb_df.rename(columns={
    "Series_Title": "title",
    "Runtime": "runtime"
})

imdb_df = imdb_df.drop_duplicates(subset="title")

for _, row in imdb_df.iterrows():
    cursor.execute("INSERT INTO imdb_runtime (title, runtime) VALUES (%s, %s)", tuple(row))

air_df = pd.read_csv("air_quality_geotype.csv")
air_df = air_df.rename(columns={
    "Unique ID": "unique_id",
    "Geo Type Name": "geo_type"
})

air_df = air_df.drop_duplicates(subset="unique_id")

for _, row in air_df.iterrows():
    cursor.execute("INSERT INTO air_quality_geotype (unique_id, geo_type) VALUES (%s, %s)", tuple(row))

conn.commit()
cursor.close()
conn.close()
print("Join tables loaded successfully into MySQL.")
