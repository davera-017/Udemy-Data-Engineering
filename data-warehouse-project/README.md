# Data Warehouse Project

1st project of the Data Warehouse (DW) section from Udacity NanoDegree in Data Engineering.

## Project Summary
An implementation of a DW using AWS Redshift for the music startup Sparkify. The ETL pipleine of this project, consists of: (i) the extraction of data from S3, (ii) stagging into Redshift, and (iii) the transformation of the data into a star-schema to enhance loads of information.

## Contents
1. Details about the setup and connection to AWS RedShift in `dwh.cfg`.
2. Methods to create the tabls for the star-schema in `create_tables.py`.
3. Processsing of the data from S3 to RedShift in `etl.py`.
4. List of all the queries used in the above scripts in `sql_queries.py`.

## Database schema

The followinf table sintetizes the structure of the database schema:

| Table | Description |
| ---- | ---- |
| staging_events | Staging table for event data |
| staging_songs | Staging table for song data |
| songplays | Song reproduction information (e.g. how they were played, by which user, etc.) |
| users | User information (e.g. name, age, gender and level) |
| songs | Song information (e.g. name, artist and duration) |
| artists | Artist name and location (geo and textual location) |
| time | Info for timestamps |

