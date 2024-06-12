# MLOps_A2
 This is a repo to store the Airflow DAG generated metadata for a data pipeline (MLOps course assignment).

1. This repo contains the solution to the data pipeline assignment. It includes:

2. An Airflow DAG script that extracts data from DAWN and BBC, preprocesses the text data, and stores it on Google Drive

3. Data preprocessing code that converts text data to lowercase, removes punctuation, extra whitespace, numbers, and common stopwords

4. DVC setup that tracks versions of the data and pushes changes to this Github repo

5. A report is also included as the markdown file MLOps_A2_20I-0782.md

[ NOTE: All code is in the Python File (i.e. A2.py) ]

The workflow is designed to run daily and ensures data consistency and quality throughout the pipeline. It also implements Data Version Control (DVC) to track versions of the data and pushes changes to this Github repo.
