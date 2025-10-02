# Beer Reviews Analytics ELT Pipeline

## Project Overview
This project implements a scalable ELT pipeline to process **4.5 million beer reviews** from a PostgreSQL source into an analytics-ready star schema and marts.  
Airflow orchestrates the pipeline while DBT handles transformations, storing processed data in **BigQuery** for efficient analytics and reporting.

The architecture is designed with future plans to integrate **online and offline feature stores**, enabling real-time and batch feature generation for machine learning workflows.

## Tech Stack
- **Source DB**: PostgreSQL  
- **Orchestration**: Airflow  
- **Transformation**: DBT  
- **Storage**: Google Cloud Storage (GCS)  
- **Analytics**: BigQuery  
- **Machine Learning**: Feature Store (planned)

## Features
- Ingests and processes 4.5M beer reviews with Airflow scheduling
- Applies DBT transformations to create a star schema for analytical queries
- Stores data marts in BigQuery for scalable analytics
- Designed for integration with online and offline feature stores

## Future Work
- Implement online and offline feature stores
- Integrate real-time data processing for machine learning pipelines
