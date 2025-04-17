import pandas as pd
import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="dsci351"
)
cursor = conn.cursor()

# Load Electric Vehicles
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
})

ev_df = ev_df[[
    'vin', 'County', 'City', 'State', 'postal_code', 'model_year',
    'Make', 'Model', 'vehicle_type', 'cafv_eligibility',
    'electric_range', 'base_msrp', 'legislative_district',
    'dol_vehicle_id', 'vehicle_location', 'electric_utility'
]]

ev_df.drop_duplicates(subset='vin', inplace=True)
ev_df = ev_df.where(pd.notnull(ev_df), None)

for _, row in ev_df.iterrows():
    values = [None if pd.isna(x) else x for x in row.values.tolist()]
    cursor.execute("""
        INSERT INTO electric_vehicles (
            vin, county, city, state, postal_code, model_year,
            make, model, vehicle_type, cafv_eligibility,
            electric_range, base_msrp, legislative_district,
            dol_vehicle_id, vehicle_location, electric_utility                  
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
    """, values)

# Load IMDB
imdb_df = pd.read_csv("imdb_top_1000.csv", on_bad_lines='skip')

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

#Filter valid release years and data types based on faulty integer values
imdb_df = imdb_df[imdb_df["release_year"].apply(lambda x: str(x).isdigit())]
imdb_df["release_year"] = imdb_df["release_year"].astype(int)
imdb_df["imdb_rating"] = pd.to_numeric(imdb_df["imdb_rating"], errors="coerce")
imdb_df["meta_score"] = pd.to_numeric(imdb_df["meta_score"], errors="coerce")
imdb_df["no_of_votes"] = pd.to_numeric(imdb_df["no_of_votes"], errors="coerce")
imdb_df = imdb_df.where(pd.notnull(imdb_df), None)

imdb_df = imdb_df[[
    'Poster_Link', 'title', 'release_year', 'certificate', 'runtime',
    'genre', 'imdb_rating', 'overview', 'meta_score',
    'director', 'star1', 'star2', 'star3', 'star4',
    'no_of_votes', 'gross'
]]

for _, row in imdb_df.iterrows():
    values = [None if pd.isna(x) else x for x in row.values.tolist()]
    cursor.execute("""
        INSERT INTO imdb_top_1000 (
            poster_link, title, release_year, certificate, runtime,
            genre, imdb_rating, overview, meta_score,
            director, star1, star2, star3, star4,
            no_of_votes, gross       
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, values)

# Load Air Quality
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

air_df = air_df[[
    'unique_id', 'name', 'measure', 'geo_type', 'geo_place',
    'time_period', 'start_date', 'data_value', 'air_quality_category'
]]

for _, row in air_df.iterrows():
    values = [None if pd.isna(x) else x for x in row.values.tolist()]
    cursor.execute("""
        INSERT INTO air_quality (
            unique_id, name, measure, geo_type, geo_place,
            time_period, start_date, data_value, air_quality_category       
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, values)

conn.commit()
cursor.close()
conn.close()

print("All datasets loaded successfully into MySQL.")