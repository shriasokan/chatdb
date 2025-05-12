import sys
import streamlit as st
import pandas as pd
import os
import json
import query_router as qr
from backend.nlp.parser import parse_with_gemini

PARENT_DATA_PATH = os.path.abspath(os.path.join(os.getcwd(), os.pardir, "data"))

datasets = [
    'imdb_top_1000',
    'electric_vehicles',
    'air_quality'
]

st.set_page_config(page_title="NLP to SQL/NoSQL Converter", layout="centered")
st.title("NLP to SQL/NoSQL Converter")

#taking input from user
user_dataset = st.selectbox('Choose a dataset:', datasets)

example_text = {
    "imdb_top_1000": "e.g., Show director with highest imdb ratings released between 2001 and 2004",
    "electric_vehicles": "e.g., Show vehicles with a base MSRP of less than 50,000",
    "air_quality": "e.g., Find the highest data value for bad air quality category"
}

display_text = example_text.get(user_dataset)

user_input = st.text_area("What query do you want to run?", height=100, placeholder= display_text)

db_select = st.selectbox("Select the database you want to run the query on:", ["sql", "nosql"])

parse_with_gemini(user_input, db_select, user_dataset)

if st.button("Generate Query"):
    # Placeholder logic
    if not user_input.strip():
        st.warning("Please enter a valid natural language query.")
    else:
        # calls queries accordingly
        try:
            result = qr.handle_query(nl_query=user_input, db_type=db_select, table=user_dataset)
            if isinstance(result, pd.DataFrame):
                st.success("Query executed successfully:")
                st.dataframe(result)

            elif isinstance(result, list):
                st.success("MongoDB query executed successfully:")
                st.dataframe(pd.DataFrame(result))

            elif isinstance(result, str):
                if result.lower().startswith("error"):
                    st.error(result)
                else:
                    st.text_area("Query Output", result, height=200)

            else:
                st.warning("Query returned an unsupported format.")

        except Exception as e:
            st.error(f"Error during query handling: {e}")
