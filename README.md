# ChatDB: Natural Language Interface for SQL and NoSQL Databases
ChatDB is a unified natural language interface that allows users to query, explore, and modify both SQL (MySQL) and NoSQL (MongoDB) databases using plain English. The system uses Google Gemini (via the Generative AI API) to translate natural language into executable database queries and includes custom parsing logic to optimize performance and accuracy.

## Features
- Natural language query parsing via Google Gemini
- Supports SQL (MySQL) and NoSQL (MongoDB)
- Schema exploration: list tables, show columns, preview data
- Data modification: `insert`, `update`, `delete` operations
- Full JOIN support for both SQL (`JOIN`) and NoSQL (`$lookup`)
- Custom regex replacements for clean, normalized queries
- Schema aware parsing and field name normalization

## Requirements
 - Python 3.9+
 - MySQL running with database named `dsci351`
 - MongoDB running locally on port 27017
 - Google Gemini API key

## Setup Instructions

### 1. Clone and Set Up Virtual Environment
```bash
git clone https://github.com/your-username/chatdb.git
cd chatdb
python -m venv .venv
source .venv/bin/activate # On Windows: .venv\Scripts\activate
```
### 2. Install Dependencies
```bash
pip install -r requirements.txt
```
### 3. Add Your Gemini API Key
Create a file named `.env` in the root directory:
```ini
GEMINI_API_KEY=your_gemini_api_key_here
```

## Load the Databases

### 4. Load Tables into MySQL
Ensure you've created the necessary tables manually in your MySQL database.
```bash
python data/load_to_mysql.py
python data/load_joins_to_mysql.py
```

### 5. Load Tables into MongoDB
```bash
python data/load_to_mongo.py
python data/load_joins_to_mongo.py
```

## Running Natural Language Queries
To load up the frontend interface, run:
```bash
streamlit run frontend_app.py
```
Use the dropdown menus to select your dataset and database.

## Sample Queries
- "Show movie with the highest IMDb rating in 2006"
- "List all electric vehicles with model year 2023"
- "Insert a new air quality reading with Geo place Queens and data value 0.9"
- "Show the CAFV eligibility for all electric vehicles"
