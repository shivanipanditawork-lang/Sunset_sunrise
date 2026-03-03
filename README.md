# Sunset_sunrise
Production-style ETL pipeline that extracts historical sunrise and sunset data from the Sunrise-Sunset.org API, converts UTC to IST, stores it in SQLite, and performs time-based analytics using Jupyter. Demonstrates end-to-end data engineering including extraction, transformation, storage, and analysis.

## Design and Develop a Data Pipeline 
Source: Sunrise-Sunset.org API — a free, no-auth API that returns astronomical data by location and date.

### Data Extraction 
1. Retrieve sunrise and sunset times for your current location. 
2. All times must be converted to and stored in IST (UTC+5:30). 
3. Collect data from January 1, 2020 through the current date.
   
### ETL Framework 
1. Use Singer.io or Meltano to ingest data into SQLite. 
2. If no native tap exists, build a custom Singer tap
   
## Query
Answer the following questions. 

1.Longest and shortest day Per calendar year

2.Latest sunrise time Per month

3.Earliest sunset time Per month

