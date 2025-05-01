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
    if db_type == "sql":
        if "cafv eligibility" in query_lower and "electric vehicles" in query_lower:
            return {
                "sql": """
                    SELECT ev.*, cafv.cafv_eligibility
                    FROM electric_vehicles ev
                    JOIN electric_vehicle_cafv cafv ON ev.vin = cafv.vin;
                """
            }

        if "runtime" in query_lower and "movie" in query_lower:
            return {
                "sql": """
                    SELECT imdb.*, rt.runtime
                    FROM imdb_top_1000 imdb
                    JOIN imdb_runtime rt ON imdb.title = rt.title;
                """
            }

        if "geo type" in query_lower and "air quality" in query_lower:
            return {
                "sql": """
                    SELECT aq.*, gt.geo_type
                    FROM air_quality aq
                    JOIN air_quality_geotype gt ON aq.unique_id = gt.unique_id;
                """
            }
    if db_type == "nosql":
        if "cafv eligibility" in query_lower and "electric vehicles" in query_lower:
            return {
                "db": "dsci351",
                "collection": "electric_vehicles",
                "pipeline": [
                    {
                        "$lookup": {
                            "from": "electric_vehicle_cafv",
                            "localField": "vin",
                            "foreignField": "vin",
                            "as": "cafv_info"
                        }
                    },
                    { "$unwind": "$cafv_info" },
                    {
                        "$project": {
                            "_id": 0,
                            "vin": 1,
                            "Make": 1,
                            "Model": 1,
                            "cafv_info.cafv_eligibility": 1
                        }
                    }
                ]
            }
        if "runtime" in query_lower and ("movie" in query_lower or "film" in query_lower):
            return {
                "db": "dsci351",
                "collection": "imdb_top_1000",
                "pipeline": [
                    {
                        "$lookup": {
                            "from": "imdb_runtime",
                            "localField": "title",
                            "foreignField": "title",
                            "as": "runtime_info"
                        }
                    },
                    { "$unwind": "$runtime_info" },
                    {
                        "$project": {
                            "_id": 0,
                            "title": 1,
                            "genre": 1,
                            "imdb_rating": 1,
                            "runtime_info.runtime": 1
                        }
                    }
                ]
            }
        if "geo type" in query_lower and "air quality" in query_lower:
            return {
                "db": "dsci351",
                "collection": "air_quality",
                "pipeline": [
                    {
                        "$lookup": {
                            "from": "air_quality_geotype",
                            "localField": "unique_id",
                            "foreignField": "unique_id",
                            "as": "geo_info"
                        }
                    },
                    { "$unwind": "$geo_info" },
                    {
                        "$project": {
                            "_id": 0,
                            "unique_id": 1,
                            "geo_place": 1,
                            "data_value": 1,
                            "geo_info.geo_type": 1
                        }
                    }
                ]
            }
        



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
        if not table:
            return {"error": "Could not determine collection/table for sample rows."}
        if db_type == "sql":
            return {"schema_explore": "sample_rows", "table": table}
        elif db_type == "nosql":
            return {"schema_explore": "sample_documents", "collection": table}
        
    # SQL Data modification rules
    if db_type == "sql":
        if query_lower.startswith("insert") or "add a new" in query_lower:
            return {"modification": "insert", "sql": parse_with_gemini(nl_query, db_type, table).get("sql")}

        if query_lower.startswith("delete") or "remove" in query_lower:
            return {"modification": "delete", "sql": parse_with_gemini(nl_query, db_type, table).get("sql")}

        if query_lower.startswith("update") or "set" in query_lower:
            return {"modification": "update", "sql": parse_with_gemini(nl_query, db_type, table).get("sql")}
    
    # NoSQL Data modification rules
    if db_type == "nosql":
        if any(kw in query_lower for kw in ["insert", "add a new"]):
            return parse_with_gemini(nl_query, db_type, table)
        
        if any(kw in query_lower for kw in ["delete", "remove"]):
            return parse_with_gemini(nl_query, db_type, table)
        
        if any(kw in query_lower for kw in ["update", "set"]):
            return parse_with_gemini(nl_query, db_type, table)
    
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
        if "what collections" in query_lower or "list collections" in query_lower:
            return {"schema_explore": "list_collections"}
    
        if "sample document" in query_lower or "example from" in query_lower:
            if "electric_vehicles" in query_lower:
                return {"schema_explore": "sample_documents", "collection": "electric_vehicles"}
            if "air_quality" in query_lower:
                return {"schema_explore": "sample_documents", "collection": "air_quality"}
            
        match = re.search(r"list (\d+)\s+(electric vehicles|movies|air quality readings)", query_lower)

    # Dynamic limit handling in MongoDB
    if match:
        limit = int(match.group(1))
        keyword = match.group(2)

        collection = ""
        if "electric" in keyword:
            collection = "electric_vehicles"
        elif "movie" in keyword:
            collection = "imdb_top_1000"
        elif "air" in keyword:
            collection = "air_quality"

        return {
            "db": "dsci351",
            "collection": collection,
            "pipeline": [{"$limit": limit}]
        }
        
    # Resort to openai LLM if our rules don't work
    return parse_with_gemini(nl_query, db_type, table)

def parse_with_gemini(nl_query: str, db_type: str, table: str = ""):
    prompt = f"""
    Convert this natural language request into a {db_type.upper()} query only.
    - If SQL: return a single SQL string only.
    - If MongoDB: return a Python dict with keys "db", "collection", and "pipeline" (list of Mongo aggregation stages). Do NOT include comments, markdown, or extra explanations. Just valid machine-readable output.
    - Output must be valid JSON or SQL and parseable.

    Natural Language Query:
    \"\"\"{nl_query}\"\"\"

    Output:
    """

    try:
        model = genai.GenerativeModel("models/gemini-1.5-pro-latest")
        response = model.generate_content(prompt)
        raw_text = response.text.strip()
        print("Gemini Raw Output:", repr(raw_text))

        # Cleaned queries
        cleaned = re.sub(r"```(?:json|python)?", "", raw_text).replace("```", "")
        cleaned = re.sub(r"^\s*sql\s+", "", cleaned, flags=re.IGNORECASE) # added by vismay
        cleaned = re.sub(r"#.*", "", cleaned)
        cleaned = re.sub(r",\s*([}\]])", r"\1", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        #print("CLEANED JSON:", cleaned)

        if db_type == "sql":
            output = cleaned
            if not output.endswith(";"):
                output += ";"
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
        
        # Fix Gemini putting modification commands inside a pipeline
        
        parsed = json.loads(cleaned)

        pipeline = parsed.get("pipeline", [])

        # Handle $merge-based pseudo-inserts
        if (
            isinstance(pipeline, list)
            and len(pipeline) >= 3
            and "$merge" in pipeline[-1]
            and "$addFields" in pipeline[1]
        ):
            doc_key = next(iter(pipeline[1]["$addFields"]), None)
            doc_val = pipeline[1]["$addFields"][doc_key] if doc_key else None
            if isinstance(doc_val, dict):
                parsed = {
                    "db": parsed.get("db", "dsci351"),
                    "collection": parsed.get("collection", ""),
                    "insertOne": {
                        "document": doc_val
                    }
                }

        # Handle insert/update/deleteOne pipelines
        elif (
            isinstance(pipeline, list)
            and len(pipeline) == 1
            and isinstance(pipeline[0], dict)
        ):
            for mod_op in ["$insertOne", "$insertMany", "$updateOne", "$deleteOne"]:
                if mod_op in pipeline[0]:
                    parsed = {
                        "db": parsed.get("db", "dsci351"),
                        "collection": parsed.get("collection", ""),
                        mod_op.lstrip("$"): pipeline[0][mod_op]
                    }




        # Override the db name
        parsed["db"] = "dsci351"


        # Normalize Gemini’s collection names to match ours
        collection_map = {
            "vehicles": "electric_vehicles",
            "evs": "electric_vehicles",
            "cars": "electric_vehicles",
            "electricvehicledata": "electric_vehicles",
            "air_quality_data": "air_quality",
            "measurements": "air_quality",
            "records": "air_quality",
            "air_quality_readings": "air_quality",
            "airquality": "air_quality",
            "pollution_data": "air_quality",
            "movies": "imdb_top_1000",
            "films": "imdb_top_1000",
            "imdb": "imdb_top_1000",
            "top_movies": "imdb_top_1000",
            "cities": "city_info"
        }
        
        if table:
            parsed["collection"] = table
        else:
            parsed["collection"] = collection_map.get(parsed.get("collection", ""), parsed.get("collection", ""))


        # Field/value normalization mappings
        field_rename_map = {
            "make": "Make",
            "manufacturer": "Make",
            "model": "Model",
            "year": "model_year",
            "range": "electric_range",
            "fuel_type": "vehicle_type",
            "powertrain": "vehicle_type",
            "type": "vehicle_type",
            "vin": "vin",
            "price": "base_msrp",
            "location": "geo_place",
            "pollutionvalue": "data_value",
            "pollution_value": "data_value",
            "value": "data_value",
            "measurement": "data_value",
            "category": "air_quality_category",
            "airqualitycategory": "air_quality_category",
            "air_quality_category": "air_quality_category",
            "location_name": "geo_place",  
            "city_name": "geo_place" 
        }
        value_normalize_map = {
            "Make": {
                "Ford": "FORD",
                "Tesla": "TESLA",
                "Jeep": "JEEP"
            },
            "vehicle_type": {
                "electric": "Battery Electric Vehicle (BEV)",
                "hybrid": "Plug-in Hybrid Electric Vehicle (PHEV)",
                "EV": "Battery Electric Vehicle (BEV)"
            }
        }

        # Normalize pipeline stages
        for stage in parsed.get("pipeline", []):
            if "$match" in stage:
                new_match = {}
                for k, v in stage["$match"].items():
                    normalized_key = field_rename_map.get(k.lower(), k)
                    # Canonicalize value
                    canon_values = value_normalize_map.get(normalized_key, {})
                    new_match[normalized_key] = canon_values.get(v, v)
                stage["$match"] = new_match
            elif "$project" in stage:
                new_project = {}
                for k, v in stage["$project"].items():
                    normalized_key = field_rename_map.get(k.lower(), k)
                    new_project[normalized_key] = v
                stage["$project"] = new_project
            elif "$group" in stage:
                group_stage = stage["$group"]
                new_group_stage = {}
                for k, v in group_stage.items():
                    if isinstance(v, dict):  # e.g. "$avg": "$pollutionValue"
                        new_v = {}
                        for op, field in v.items():
                            # Normalize field inside aggregation
                            field_clean = field.lstrip("$").lower()
                            normalized_field = field_rename_map.get(field_clean, field_clean)
                            new_v[op] = f"${normalized_field}"
                        new_group_stage[k] = new_v
                    elif isinstance(v, str) and v.startswith("$"):
                        field_clean = v.lstrip("$").lower()
                        normalized_field = field_rename_map.get(field_clean, field_clean)
                        new_group_stage[k] = f"${normalized_field}"
                    else:
                        new_group_stage[k] = v
                stage["$group"] = new_group_stage
            elif "$sort" in stage:
                new_sort = {}
                for k, v in stage["$sort"].items():
                    normalized_key = field_rename_map.get(k.lower(), k)
                    new_sort[normalized_key] = v
                stage["$sort"] = new_sort
            elif "$lookup" in stage:
                lookup_stage = stage["$lookup"]

                # Normalize collection name
                lookup_stage["from"] = collection_map.get(lookup_stage.get("from", ""), lookup_stage.get("from", ""))

                # Normalize field names
                for key in ["localField", "foreignField", "as"]:
                    if key in lookup_stage:
                        normalized_key = field_rename_map.get(lookup_stage[key].lower(), lookup_stage[key])
                        lookup_stage[key] = normalized_key

                stage["$lookup"] = lookup_stage


        for mod_key in ["insertOne", "insertMany", "updateOne", "deleteOne"]:
            if mod_key in parsed:
                mod_obj = parsed[mod_key]
                for key in ["document", "filter", "update"]:
                    if key in mod_obj and isinstance(mod_obj[key], dict):
                        normalized = {}
                        for k, v in mod_obj[key].items():
                            new_k = field_rename_map.get(k.lower(), k)
                            val_map = value_normalize_map.get(new_k, {})
                            normalized[new_k] = val_map.get(v, v)
                        mod_obj[key] = normalized
                parsed[mod_key] = mod_obj
                print("Final MongoDB Modification JSON:", json.dumps(parsed, indent=2))
                return parsed

        return parsed
        
    except Exception as e:
        return {"error": f"parse_with_gemini() failed: {str(e)}"}