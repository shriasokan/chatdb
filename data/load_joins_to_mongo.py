import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["dsci351"]

ev_df = pd.read_csv("electric_vehicle_cafv.csv")
ev_df = ev_df.rename(columns={
    "VIN (1-10)": "vin",
    "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "cafv_eligibility"
})
db.electric_vehicle_cafv.drop()
db.electric_vehicle_cafv.insert_many(ev_df.to_dict(orient="records"))
print("Inserted electric_vehicle_cafv into MongoDB")


imdb_df = pd.read_csv("imdb_runtime.csv")
imdb_df = imdb_df.rename(columns={
    "Series_Title": "title",
    "Runtime": "runtime"
})
db.imdb_runtime.drop()
db.imdb_runtime.insert_many(imdb_df.to_dict(orient="records"))
print("Inserted imdb_runtime into MongoDB")


air_df = pd.read_csv("air_quality_geotype.csv")
air_df = air_df.rename(columns={
    "Unique ID": "unique_id",
    "Geo Type Name": "geo_type"
})
db.air_quality_geotype.drop()
db.air_quality_geotype.insert_many(air_df.to_dict(orient="records"))
print("Inserted air_quality_geotype into MongoDB")

client.close()
print("Join collections loaded successfully into MongoDB.")
