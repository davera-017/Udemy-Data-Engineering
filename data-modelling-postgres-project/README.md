# Data Modelling with PostgreSQL

The purpose of this exercise is to offer Sparkify with a database to enhance the collection and analysis of the data of their new music streaming app.

## Step-by-step for the ETL pipeline execution

The following steps must be performed before executing the ETL pipeline:

1. Create the empty tables by executing `create_tables.py`.
2. Check that the tables exist by running `test.py`.
3. Populate the tables with the data in the `data` directory by executing `etl.py`.
4. Check that the information has been inserted into the tables by running `test.py`.

> **NOTE:** A python script can be executing from the command line with the command `python <your-file-name>.py`.

## Directory contend

1. `test.ipynb` displays the first few rows of each table to let you check your database.
2. `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
3. `etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. `etl.py` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
5. `sql_queries.py` contains all your sql queries, and is imported into the last three files above.
6. `data` directory contains the datasets `log_data`and `song_data`.

 ## Schema design and ETL pipeline.


This database follows an star schema, where the fact table, `songplays` contains the records in log data associated with song plays. And it has four ralated dimension tables: `users`, `songs`, `artists`, and `time`. This design allows the execution of query statements with a few JOINS and is optimized for data agregations —a neccesity for Sparkify.
