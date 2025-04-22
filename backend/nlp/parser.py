import os
import json
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

def parse_nl_query(nl_query: str, db_type: str):
    # Creating our own parse rules to try first and resort to LLMs if they fail
    if db_type == "sql":
        if "movies released after 2015" in nl_query.lower():
            return {"sql": "SELECT * FROM imdb_top_1000 WHERE release_year > 2015"}
    
    if db_type == "nosql":
        if "evs in california" in nl_query.lower():
            return {
                "db": "dsci351",
                "collection": "electric_vehicles",
                "pipeline": [{"$match": {"State": "CA"}}]
            }
        
    # Resort to openai LLM if our rules don't work
    return parse_with_gemini(nl_query, db_type)

def parse_with_gemini(nl_query: str, db_type: str):
    prompt = f"""
    Convert this natural language request into a {db_type.upper()} query only.
    - If SQL: return a single SQL string only.
    - If MongoDB: return a Python dict with keys "db", "collection", and "pipeline" (list of Mongo aggregation stages).
    - Do NOT include markdown formatting like ```sql.

    Natural Language Query:
    \"\"\"{nl_query}\"\"\"

    Output:
    """

    try:
        model = genai.GenerativeModel("models/gemini-1.5-pro-latest")
        response = model.generate_content(prompt)
        output = response.text.strip()

        output = output.replace("```sql", "").replace("```", "").strip()

        if db_type == "sql":
            output = output.replace("FROM movies", "FROM imdb_top_1000")
            output = output.replace("from movies", "from imdb_top_1000")
            output = output.replace(" year ", " release_year ")
            output = output.replace("year>", "release_year>")
            output = output.replace("year <", "release_year <")
            output = output.replace("year=", "release_year=")

            return {"sql": output}
        
        elif db_type == "nosql":
            return json.loads(output)
        
    except Exception as e:
        return {"error": str(e)}