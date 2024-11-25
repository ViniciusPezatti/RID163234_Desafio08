# airflow_settings.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from datetime import datetime

# Definição das paths para os arquivos locais
raw_data_path = "../../raw/raw_data.csv"
bronze_path = '../../raw/bronze/raw_data.csv'
silver_path = '../../raw/silver/cleaned_data.csv'
gold_path = '../../raw/gold/analytics_data.csv'

def upload_raw_data_to_bronze():
    with open(raw_data_path, 'rb') as file:
        with open(bronze_path, 'wb') as out_file:
            out_file.write(file.read())

def calculate_age(birthdate):
    if pd.isnull(birthdate):
        return None
    birthdate = datetime.strptime(birthdate, "%Y-%m-%d")
    today = datetime.today()
    age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
    return age

def process_bronze_to_silver():
    df = pd.read_csv(bronze_path)
    df['email'] = df['email'].apply(lambda x: x if '@' in x else None)
    df.dropna(subset=['email', 'date_of_birth', 'signup_date'], inplace=True)
    df['age'] = df['date_of_birth'].apply(calculate_age)
    df.to_csv(silver_path, index=False)

def process_silver_to_gold():
    df = pd.read_csv(silver_path)
    bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    labels = ["0-10", "11-20", "21-30", "31-40", "41-50", "51-60", "61-70", "71-80", "81-90", "91-100"]
    df['age_range'] = pd.cut(df["age"], bins=bins, labels=labels, right=False)
    age_range_status_counts = df.groupby(['age_range', "subscription_status"]).size().reset_index(name='count')
    age_range_status_counts.to_csv(gold_path, index=False)
