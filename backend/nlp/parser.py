import os
import json
import re
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

def parse_nl_query(nl_query: str, db_type: str, table: str = ""):
    query_lower = nl_query.lower()

    # Creating our own parse rules to try first and resort to LLMs if they fail

    # Schema exploration logic

    if "what tables" in query_lower or "list tables" in query_lower or "show tables" in query_lower:
        return {"schema_explore": "list_tables"}
    
    if ("what columns" in query_lower or "show columns" in query_lower or "describe" in query_lower) and table:
        return {"schema_explore": "describe_table", "table": table}

    if ("show sample" in query_lower or "sample rows" in query_lower or "show me rows" in query_lower or "preview" in query_lower):
        if not table:
            if "air_quality" in query_lower:
                table = "air_quality"
            elif "electric_vehicles" in query_lower:
                table = "electric_vehicles"
            elif "imdb_top_1000" in query_lower:
                table = "imdb_top_1000"
        if table:
            return {"schema_explore": "sample_rows", "table": table}
        else:
            return {"error": "Could not determine table for sample rows request."}
        
    # Data modification rules
    if query_lower.startswith("insert") or "add a new" in query_lower:
        return {"modification": "insert", "sql": parse_with_gemini(nl_query, db_type, table).get("sql")}

    if query_lower.startswith("delete") or "remove" in query_lower:
        return {"modification": "delete", "sql": parse_with_gemini(nl_query, db_type, table).get("sql")}

    if query_lower.startswith("update") or "set" in query_lower:
        return {"modification": "update", "sql": parse_with_gemini(nl_query, db_type, table).get("sql")}
    
    # SQL rules
    if db_type == "sql":

        if "what tables" in query_lower or "list tables" in query_lower:
            return {"schema_explore": "SHOW TABLES"}

        match = re.search(r"columns in (.+)", query_lower)
        if match:
            table_name = match.group(1).strip()
            return {"schema_explore": f"DESCRIBE {table_name}"}

        match = re.search(r"sample rows from (.+)", query_lower)
        if match:
            table_name = match.group(1).strip()
            return {"schema_explore": f"SELECT * FROM {table_name} LIMIT 5"}


        # imdb_top_1000 rules
        if "movies released after 2015" in nl_query.lower():
            return {"sql": "SELECT * FROM imdb_top_1000 WHERE release_year > 2015"}
        
        # electric_vehicles rules
        if "evs in washington" in query_lower:
            return {"sql": "SELECT * FROM electric_vehicles WHERE state = 'WA'"}

        if "evs made by tesla" in query_lower or "tesla evs" in query_lower:
            return {"sql": "SELECT * FROM electric_vehicles WHERE make = 'TESLA'"}

        if "average electric range by make" in query_lower:
            return {"sql": "SELECT make, AVG(electric_range) AS avg_range FROM electric_vehicles GROUP BY make ORDER BY avg_range DESC"}

        if "top 5 most expensive evs" in query_lower:
            return {"sql": "SELECT * FROM electric_vehicles ORDER BY base_msrp DESC LIMIT 5"}
        
        # air_quality rules
        if "average data value by air quality category" in query_lower:
            return {"sql": "SELECT air_quality_category, AVG(data_value) AS avg_value FROM air_quality GROUP BY air_quality_category"}

        if "air quality measures in the bronx" in query_lower:
            return {"sql": "SELECT * FROM air_quality WHERE geo_place LIKE '%Bronx%'"}

        if "highest pollution" in query_lower or "top 10 pollution" in query_lower:
            return {"sql": "SELECT * FROM air_quality ORDER BY data_value DESC LIMIT 10"}

        if "start dates in 2020" in query_lower:
            return {"sql": "SELECT * FROM air_quality WHERE start_date LIKE '2020%'"}


    # NoSQL Rules
    if db_type == "nosql":
        if "evs in california" in nl_query.lower():
            return {
                "db": "dsci351",
                "collection": "electric_vehicles",
                "pipeline": [{"$match": {"State": "CA"}}]
            }
        
    # Resort to openai LLM if our rules don't work
    return parse_with_gemini(nl_query, db_type, table)

def parse_with_gemini(nl_query: str, db_type: str, table: str = ""):
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
        '''
        # output = response.text.strip()

        # Extract first line that looks like an actual SQL command
        lines = output.splitlines()
        sql_lines = [line.strip() for line in lines if line.strip().lower().startswith(("insert", "update", "delete", "select"))]
        if sql_lines:
            output = sql_lines[0]
        else:
            output = output.strip()

        # Add semicolon if missing
        if not output.endswith(";"):
            output += ";"

        # output = output.replace("```sql", "").replace("```", "").strip()
        '''

        raw_text = response.text.strip()
        print("Gemini Raw Output:", repr(raw_text))

        # Grab first SQL-looking line
        lines = raw_text.splitlines()
        sql_lines = [line.strip() for line in lines if line.strip().lower().startswith(("insert", "update", "delete", "select"))]
        output = sql_lines[0] if sql_lines else raw_text

        if not output.endswith(";"):
            output += ";"
            
        if db_type == "sql":
            # Regex substitutions for incorrect Gemini query interpretations

            if table == "imdb_top_1000":
                output = re.sub(r"\bFROM\s+movies\b", "FROM imdb_top_1000", output, flags=re.IGNORECASE)
                output = re.sub(r"\byear\b", "release_year", output, flags=re.IGNORECASE)
                output = re.sub(r"genre\s*=\s*['\"](\w+)['\"]", r"genre LIKE '%\1%'", output, flags=re.IGNORECASE)

            elif table == "electric_vehicles":
                output = re.sub(r"\bmanufacturer\b", "make", output, flags=re.IGNORECASE)
                output = re.sub(r"\brange\b", "electric_range", output, flags=re.IGNORECASE)
                output = re.sub(r"\bprice\b", "base_msrp", output, flags=re.IGNORECASE)
                output = re.sub(r"\bFROM\s+vehicles\b", "FROM electric_vehicles", output, flags=re.IGNORECASE)
                output = re.sub(r"\byear_of_manufacture\b", "model_year", output, flags=re.IGNORECASE)
                output = re.sub(r"\byear\b", "model_year", output, flags=re.IGNORECASE)
                output = re.sub(r"(powertrain|fuel_type|vehicle_type|type)\s*=\s*['\"]?electric['\"]?", "vehicle_type = 'Battery Electric Vehicle (BEV)'", output, flags=re.IGNORECASE)
                output = re.sub(r"(powertrain|fuel_type|vehicle_type|type)\s*=\s*['\"]?hybrid['\"]?", "vehicle_type = 'Plug-in Hybrid Electric Vehicle (PHEV)'", output, flags=re.IGNORECASE)
                output = re.sub(r"make\s*=\s*['\"]Ford['\"]", "make = 'FORD'", output, flags=re.IGNORECASE)
                output = re.sub(r"\bINTO\s+EVs\b", "INTO electric_vehicles", output, flags=re.IGNORECASE)
                output = re.sub(r"\bVIN\b", "vin", output)
                output = re.sub(r"\bMake\b", "make", output)
                output = re.sub(r"\bModel\b", "model", output)
                output = re.sub(r"\bYear\b", "model_year", output)
                output = re.sub(r"\bRange\b", "electric_range", output)
                

            elif table == "air_quality":
                output = re.sub(r"\bair_quality_readings\b", "air_quality", output, flags=re.IGNORECASE)
                output = re.sub(r"\bmeasurement\b", "data_value", output, flags=re.IGNORECASE)
                output = re.sub(r"\bvalue\b", "data_value", output, flags=re.IGNORECASE)
                output = re.sub(r"\blocation\b", "geo_place", output, flags=re.IGNORECASE)
                output = re.sub(r"\bcategory\b", "air_quality_category", output, flags=re.IGNORECASE)


            print("Cleaned Gemini SQL:", output)

            return {"sql": output}
        
        elif db_type == "nosql":
            return json.loads(output)
        
    except Exception as e:
        return {"error": str(e)}