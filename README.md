# 🍺 Beer Reviews Analytics & Recommender System Pipeline

## Project Overview
This project implements a **scalable ELT pipeline** to process **4.5 million beer reviews** sourced from Kaggle into analytics-ready formats, and extends into a **hybrid recommender system** using PySpark.  

The pipeline is orchestrated by **Airflow**, transformed with **DBT**, stored in **BigQuery**, and enhanced with a hybrid recommendation engine built with **MLflow**, **DVC**, **FAISS**, and **Git** for reproducibility.  

Future plans include integration with **Vertex AI** for real-time recommendation serving and feature stores.

---

## Data Sources
The dataset is aggregated from the following Kaggle sources:

1. [RateBeer Dataset — 3.0M+ reviews](https://www.kaggle.com/datasets/mahmoodtopiwala/ratebeer)  
2. [Beer Advocate Dataset — 1.5M reviews](https://www.kaggle.com/datasets/thedevastator/1-5-million-beer-reviews-from-beer-advocate)  

**Total:** ~4.5 million beer reviews including metadata and user ratings.

---

## Architecture

### Core Pipeline
1. **Data Ingestion**  
   Kaggle datasets → PostgreSQL → Google Cloud Storage via Airflow.

2. **Data Transformation**  
   DBT models transform raw data into a **star schema** stored in BigQuery.

3. **Analytics & Reporting**  
   BigQuery hosts the data marts for efficient querying and dashboarding.

---

### Recommender System
- **Data Loading**: PySpark loads transformed data from BigQuery.
- **Modeling**: Hybrid recommendation model combining collaborative filtering and content-based approaches.
- **Tools**:
  - **MLflow** for experiment tracking and model management.
  - **DVC** for dataset versioning and reproducibility.
  - **Git** for code version control.
  - **FAISS** for efficient similarity search and nearest neighbor retrieval.
- **Storage**: Model artifacts stored in MLflow, data stored in DVC.
  
---

## ⚙️ Tech Stack

| Layer | Technology |
|-------|------------|
| Source DB | PostgreSQL |
| Orchestration | Airflow |
| Transformation | DBT |
| Storage | Google Cloud Storage (GCS), BigQuery |
| Data Processing | PySpark |
| Machine Learning | MLflow, DVC, FAISS |
| Version Control | Git |
| Future Enhancements | Vertex AI, Online/Offline Feature Stores |

---

## Features

- **ELT Pipeline**
  - Processes ~4.5M beer reviews from Kaggle with Airflow orchestration.
  - DBT transformations produce a **star schema** for analytics.
  - Data marts stored in BigQuery for high-performance queries.

- **Hybrid Recommender System**
  - Collaborative filtering + content-based recommendations.
  - Scalable similarity search using FAISS.
  - Experiment tracking with MLflow.
  - Dataset and model version control with DVC and Git.
  - Spark-based pipeline for big data scalability.

---

## Future Work

- **Vertex AI Integration**  
  Deploy models for real-time serving and scaling.

- **Feature Store Implementation**  
  Integrate online/offline feature stores for ML pipelines.

- **Real-Time Processing**  
  Incorporate streaming data for live recommendations.

---

## Setup & Installation

### Prerequisites
- Python 3.9+
- Apache Airflow
- DBT
- Google Cloud SDK
- PySpark
- MLflow
- DVC
- FAISS
- Git

### Installation Steps
1. Clone the repo:  
   ```bash
   git clone https://github.com/your-org/beer-reviews-pipeline.git
   cd beer-reviews-pipeline
