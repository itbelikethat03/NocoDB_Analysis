import requests
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from pandas.tseries.holiday import USFederalHolidayCalendar
import time

# NocoDB Configuration
BASE_URL = "http://localhost:8080/api/v1/db/data/v1"
PROJECT_ID = "pfbsbp2pue608o4"
API_KEY = "asd"
TABLE_NAME = "mihud821ug40jns"
HEADERS = {"xc-token": API_KEY}

# MySQL Configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 1111,
    'user': 'root',
    'password': 'asd',
    'database': 'yelp_dataset'
}


def fetch_from_nocodb(limit=1000, max_records=1000000):
    """Fetch data from NocoDB API."""
    start_time = time.time()
    offset = 0
    all_records = []
    lines_processed = 0

    while len(all_records) < max_records:
        url = f"{BASE_URL}/{PROJECT_ID}/{TABLE_NAME}?limit={limit}&offset={offset}"
        response = requests.get(url, headers=HEADERS)

        if response.status_code == 200:
            data = response.json()
            all_records.extend(data['list'])
            lines_processed += len(data['list'])
            if lines_processed % 10000 == 0:
                print(f"NocoDB: Processed {lines_processed} lines...")
            if len(data['list']) < limit:
                break
        else:
            print(f"NocoDB Error: {response.status_code} - {response.text}")
            break

        offset += limit

    end_time = time.time()
    print(f"NocoDB fetch time: {end_time - start_time:.2f} seconds")
    print(f"Total lines processed in NocoDB: {lines_processed}")
    return pd.DataFrame(all_records)


def fetch_from_mysql(query):
    """Fetch data directly from MySQL."""
    start_time = time.time()
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    lines_processed = 0

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        lines_processed = len(rows)
        print(f"MySQL: Total lines processed: {lines_processed}")
        df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
    finally:
        connection.close()

    end_time = time.time()
    print(f"MySQL fetch time: {end_time - start_time:.2f} seconds")
    return df


def analyze_and_visualize(df, title_suffix):
    """Perform analysis and visualize the results."""
    if 'stars' in df.columns and 'date' in df.columns:
        df['stars'] = pd.to_numeric(df['stars'], errors='coerce')
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['stars', 'date'])
    else:
        print("Missing 'stars' or 'date' columns!")
        return

    # Define holidays
    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(start=df['date'].min(), end=df['date'].max())
    df['is_holiday'] = df['date'].isin(holidays)

    # Analyze data
    holiday_avg = df.groupby('is_holiday')['stars'].mean()
    holiday_counts = df['is_holiday'].value_counts()
    print(f"Data from {title_suffix}:")
    print(f"Holiday records: {holiday_counts.get(True, 0)}")
    print(f"Non-holiday records: {holiday_counts.get(False, 0)}")

    # Visualization
    plt.figure(figsize=(8, 5))
    holiday_avg.plot(kind='bar', color=['blue', 'green'], alpha=0.7)
    plt.title(f'Average Ratings During Holidays vs Non-Holidays ({title_suffix})')
    plt.xlabel('Holiday')
    plt.ylabel('Average Rating')
    plt.xticks(ticks=[0, 1], labels=['Non-Holiday', 'Holiday'], rotation=0)
    plt.show()


# Fetch and analyze data from NocoDB
print("Fetching data from NocoDB...")
nocodb_data = fetch_from_nocodb()
if not nocodb_data.empty:
    analyze_and_visualize(nocodb_data, "NocoDB")

# Fetch and analyze data from MySQL
print("Fetching data from MySQL...")
mysql_query = """
    SELECT stars, date
    FROM review
    WHERE stars IS NOT NULL AND date IS NOT NULL
"""
mysql_data = fetch_from_mysql(mysql_query)
if not mysql_data.empty:
    analyze_and_visualize(mysql_data, "MySQL")
