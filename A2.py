import re
import string
import requests
from bs4 import BeautifulSoup
import dvc
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

def extract_data(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    links = [a['href'] for a in soup.find_all('a', href=True)]
    titles = [h2.text for h2 in soup.find_all('h2')]
    descriptions = [p.text for p in soup.find_all('p')]
    
    return links, titles, descriptions

def preprocess_text(text):
    # Convert to lowercase
    text = text.lower()
    
    # Remove punctuation
    text = re.sub('['+string.punctuation+']', '', text)
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove numbers
    text = re.sub(r'\d+', '', text)
    
    # Remove common stopwords (e.g., "the", "and", etc.)
    stopwords = ['the', 'and', 'a', 'of', 'to', 'in', 'for', 'is', 'on', 'that', 'with']
    text = ' '.join([word for word in text.split() if word not in stopwords])
    
    return text

def transform_data(**kwargs):
    # Extract data from dawn.com and bbc.com
    dawn_links, dawn_titles, dawn_descriptions = extract_data('https://www.dawn.com')
    bbc_links, bbc_titles, bbc_descriptions = extract_data('https://www.bbc.com')
    
    # Preprocess and transform the extracted data
    dawn_titles = [preprocess_text(title) for title in dawn_titles]
    dawn_descriptions = [preprocess_text(description) for description in dawn_descriptions]
    bbc_titles = [preprocess_text(title) for title in bbc_titles]
    bbc_descriptions = [preprocess_text(description) for description in bbc_descriptions]
    
    return dawn_links, dawn_titles, dawn_descriptions, bbc_links, bbc_titles, bbc_descriptions

def add_google_drive_remote():
    # Construct the DVC command to add the remote
    command = ["dvc", "remote", "add", "-d", "google_drive_remote", f"gdrive://15O-_4K4xvYGkS2HrcYD6JQOl1MFzySRx"]

    # Execute the command
    subprocess.run(command, check=True)

def dvc_upload_to_google_drive():
    # DVC push to Google Drive
    subprocess.run(["dvc", "push", "-r", "google_drive_remote"], check=False)

def upload_to_github(repo):
    # Copy the DAG file to the DAGs directory
    subprocess.run(["cp", "/opt/airflow/dags/A2.py", "/opt/airflow/dags"], check=True)

    # Add the DAG file and DVC metadata to Git
    subprocess.run(["git", "add", "/opt/airflow/dags/A2.py", "/opt/airflow/.dvc"], check=True)

    # Commit changes
    subprocess.run(["git", "commit", "-m", "Add DAG file and DVC metadata"], check=True)

    # Push changes to GitHub
    subprocess.run(["git", "push", repo], check=True)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

dvc_task = PythonOperator(
    task_id='dvc_upload_to_google_drive',
    python_callable=dvc_upload_to_google_drive,
    dag=dag
)

upload_to_github_task = PythonOperator(
    task_id='upload_to_github',
    python_callable=upload_to_github,
    op_args=['Ali20782/MLOps_A2'],
    dag=dag
)

# Call the function to add the Google Drive remote
add_google_drive_remote()

extract_task >> transform_task >> dvc_task >> upload_to_github_task
