# Spark and Data Lakes - Capstone Project

This project intended to design and deploy a Data Lakehouse requested by STEDI team. Generrally speaking, the Data Lakehouse implemented three different zones to process the data from this company: *landing*, which recieves the raw data from the STEDI app, *trusted*, where filters such as a privacy policy filter are applied, and *curated*, where the data is processed to be ready for analysis.

The hole deployment was done in AWS. The data was storaged in an S3 bucket, and it was then processed using *Data Catalog* and *AWS Glues Studio*.