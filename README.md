# GitHub Events Lakehouse Pipeline

This project is a simple, real-world style **data lakehouse pipeline** built using GitHub event data. It shows how raw data can be ingested, cleaned, corrected, and explored using modern data tools.

The main idea is to take raw JSON event data, store it properly, clean it into usable form, fix mistakes using time-travel, and finally provide a small UI to explore everything.

---

## Overview

This project demonstrates:

* Storing raw data in a **Bronze layer**
* Cleaning and structuring it into a **Silver layer**
* Handling **schema changes**
* Fixing bad data using **time-travel**
* Exploring data through a simple **web UI + API**

---

## Architecture (Simple Flow)

Source Data → Bronze → Silver → Corrected Silver → Explorer UI

* **Source**: GitHub event JSON files
* **Bronze**: Raw data stored as-is
* **Silver**: Cleaned and structured data
* **Silver Corrected**: Fixed version after removing bad data

---

## Tech Stack

* Python 3.11
* Delta Lake (delta-rs)
* DuckDB
* Flask (Backend API)
* React (Frontend UI)
* Docker

---

## Getting Started

### Run Locally

Install dependencies:

```bash
pip install -r requirements.txt
```

Run ingestion for each day:

```bash
python -m pipeline.ingest --day 1
python -m pipeline.ingest --day 2
python -m pipeline.ingest --day 3
python -m pipeline.ingest --day 4
python -m pipeline.ingest --day 5
```

Run correction:

```bash
python -m pipeline.correct_data
```

---

### Run with Docker

```bash
cp .env.example .env
docker-compose up --build -d
```

App runs at:

```
http://localhost:8000
```

---

## Ingestion Pipeline

The ingestion process:

* Reads JSON data for each day
* Validates it using models
* Stores raw data in **Bronze**
* Converts it into structured format in **Silver**

### Special Cases

* **Day 3**: New field added → shows schema evolution
* **Day 5**: Some data intentionally corrupted → used for correction demo

---

## Time-Travel Correction

This is one of the key features.

Steps:

1. Go back to a **previous clean version**
2. Reload correct Day 5 data
3. Merge old + corrected data
4. Remove duplicates
5. Save as `silver_corrected`

Run:

```bash
python -m pipeline.correct_data
```

---

## Explorer API

Base URL:

```
http://localhost:8000
```

### Health Check

```
GET /api/health
```

### Get Table Versions

```
GET /api/tables/silver/versions
```

### Get Version Details

```
GET /api/tables/silver/versions/1
```

Returns row count and schema.

---

## Explorer UI

Features:

* Select table (bronze / silver / corrected)
* View versions
* Check schema and row count
* Run simple SQL queries

Example query:

```sql
SELECT type, COUNT(*) 
FROM table 
GROUP BY type 
ORDER BY COUNT(*) DESC;
```

---

## Screenshots

(Add your screenshots here)

* Versions list
* Schema view
* Query results

---

## Demo Video

(Add your video link here)

---

## Project Structure

```
data-lakehouse-pipeline/
├── .env
├── .env.example
├── Dockerfile
├── docker-compose.yml
├── docker-entrypoint.sh
├── requirements.txt
├── data/
│   ├── source/
│   │   ├── day_1.json.gz
│   │   ├── day_2.json.gz
│   │   ├── day_3.json.gz
│   │   ├── day_4.json.gz
│   │   └── day_5.json.gz
│   └── lakehouse/
│       ├── bronze/
│       ├── silver/
│       └── silver_corrected/
├── pipeline/
│   ├── __init__.py
│   ├── models.py
│   ├── ingest.py
│   └── correct_data.py
├── explorer/
│   ├── api/
│   │   └── app.py
│   └── ui/
│       ├── package.json
│       ├── src/
│       └── public/
└── docs/
    └── transaction_log_analysis.md

```

---

## Conclusion

This project shows how a basic lakehouse works in practice. It covers data ingestion, cleaning, handling errors, and exploring data using simple tools. It’s a good starting point for understanding real-world data engineering workflows.
