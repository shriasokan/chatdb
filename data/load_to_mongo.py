import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["dsci351"]

# Load EV data
ev_df = pd.read_csv("Electric_Vehicle_Population_Data.csv")

ev_df = ev_df.rename(columns={
    'VIN (1-10)': 'vin',
    'Postal Code': 'postal_code',
    'Model Year': 'model_year',
    'Electric Vehicle Type': 'vehicle_type',
    'Clean Alternative Fuel Vehicle (CAFV) Eligibility': 'cafv_eligibility',
    'Electric Range': 'electric_range',
    'Base MSRP': 'base_msrp',
    'Legislative District': 'legislative_district',
    'DOL Vehicle ID': 'dol_vehicle_id',
    'Vehicle Location': 'vehicle_location',
    'Electric Utility': 'electric_utility',
    '2020 Census Tract': 'census_tract'
})

ev_df = ev_df.where(pd.notnull(ev_df), None)
ev_data = ev_df.to_dict(orient="records")

db.electric_vehicles.drop()
db.electric_vehicles.insert_many(ev_data)
print("Inserted Electric Vehicles into MongoDB.")

# Load IMDB Data
imdb_df = pd.read_csv("imdb_top_1000.csv", on_bad_lines="skip")

imdb_df = imdb_df.rename(columns={
    "Series_Title": "title",
    "Released_Year": "release_year",
    "Certificate": "certificate",
    "Runtime": "runtime",
    "Genre": "genre",
    "IMDB_Rating": "imdb_rating",
    "Overview": "overview",
    "Meta_score": "meta_score",
    "Director": "director",
    "Star1": "star1",
    "Star2": "star2",
    "Star3": "star3",
    "Star4": "star4",
    "No_of_Votes": "no_of_votes",
    "Gross": "gross"
})

imdb_df = imdb_df[imdb_df["release_year"].apply(lambda x: str(x).isdigit())]
imdb_df["release_year"] = imdb_df["release_year"].astype(int)
imdb_df["imdb_rating"] = pd.to_numeric(imdb_df["imdb_rating"], errors="coerce")
imdb_df["meta_score"] = pd.to_numeric(imdb_df["meta_score"], errors="coerce")
imdb_df["no_of_votes"] = pd.to_numeric(imdb_df["no_of_votes"], errors="coerce")
imdb_df = imdb_df.where(pd.notnull(imdb_df), None)

imdb_data = imdb_df.to_dict(orient="records")

db.imdb_top_1000.drop()
db.imdb_top_1000.insert_many(imdb_data)
print("Inserted IMDB Top 1000 into MongoDB")

# Load air quality data
air_df = pd.read_csv("updated_air_quality_data.csv")

air_df = air_df.rename(columns={
    "Unique ID": "unique_id",
    "Name": "name",
    "Measure": "measure",
    "Geo Type Name": "geo_type",
    "Geo Place Name": "geo_place",
    "Time Period": "time_period",
    "Start_Date": "start_date",
    "Data Value": "data_value",
    "Air Quality Category": "air_quality_category"
})

air_df = air_df.where(pd.notnull(air_df), None)
air_data = air_df.to_dict(orient="records")

db.air_quality.drop()
db.air_quality.insert_many(air_data)
print("Inserted Air Quality Data into MongoDB")

client.close()
print("All datasets loaded into MongoDB successfully.")