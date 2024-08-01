from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import mysql.connector

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def parse_custom_timestamp(timestamp):
    try:
        # Assuming the timestamp format is 'YYYY-MM-DDTHH:MM:SS+HH:MM'
        parsed_time = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        # Handle cases where the timestamp format is unexpected
        parsed_time = None
        print(f"Error parsing timestamp: {e}")
    return parsed_time

def fetch_air_quality_data(city):
    token = "6afff0ba0ec07fc165bf7f377467941056def3f5"
    city_map = {
        "Shanghai": "shanghai",
        "Israel": "israel",
        "New York": "new-york",
        "French": "paris",
        "London": "london",
        "Hong Kong": "hongkong"
    }
    
    city_url_part = city_map.get(city)
    if not city_url_part:
        raise ValueError(f"City '{city}' is not mapped to a valid URL part.")
    
    url = f"http://api.waqi.info/feed/{city_url_part}/?token={token}"
    response = requests.get(url)
    data = response.json()
    
    if data["status"] == "ok":
        # Extract data
        city_name = data["data"]["city"]["name"]
        latitude = data["data"]["city"]["geo"][0]
        longitude = data["data"]["city"]["geo"][1]
        timestamp = data["data"]["time"]["iso"]
        aqi = data["data"]["aqi"]
        dominant_pollutant = data["data"]["dominentpol"]
        iaqi = data["data"]["iaqi"]
        
        # Convert timestamp to the correct format
        timestamp = parse_custom_timestamp(timestamp)
        
        if timestamp is None:
            raise ValueError("Invalid timestamp format received from API")
        
        # Flatten IAQI values
        co = iaqi.get("co", {}).get("v", None)
        h = iaqi.get("h", {}).get("v", None)
        no2 = iaqi.get("no2", {}).get("v", None)
        o3 = iaqi.get("o3", {}).get("v", None)
        p = iaqi.get("p", {}).get("v", None)
        pm10 = iaqi.get("pm10", {}).get("v", None)
        pm25 = iaqi.get("pm25", {}).get("v", None)
        so2 = iaqi.get("so2", {}).get("v", None)
        t = iaqi.get("t", {}).get("v", None)
        w = iaqi.get("w", {}).get("v", None)
        
        # Connect to the MariaDB database
        conn = mysql.connector.connect(
            host="10.1.1.35",
            user="root",
            password="mapr123",
            database="air_quality_db"
        )
        cursor = conn.cursor()
        
        # Check if data already exists
        query = """
        SELECT COUNT(*) FROM air_quality_data
        WHERE timestamp = %s AND city = %s
        """
        cursor.execute(query, (timestamp, city_name))
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Insert new data
            insert_query = """
            INSERT INTO air_quality_data (city, latitude, longitude, aqi, dominant_pollutant, co, h, no2, o3, p, pm10, pm25, so2, t, w, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (city_name, latitude, longitude, aqi, dominant_pollutant, co, h, no2, o3, p, pm10, pm25, so2, t, w, timestamp))
            conn.commit()
        
        cursor.close()
        conn.close()
    else:
        raise ValueError("Failed to fetch data")

with DAG(
    'air_quality_dag',
    default_args=default_args,
    description='A DAG to fetch and process air quality data for multiple cities',
    schedule_interval='0 7 * * *',  # Run at 7 AM  every day
    access_control={
        'role_<username>': {
            'can_read',
            'can_edit',
            'can_delete'
        }
    }
) as dag:
    
    cities = ["Shanghai", "Israel", "New York", "French", "London", "Hong Kong"]
    
    for city in cities:
        fetch_data_task = PythonOperator(
            task_id=f'fetch_air_quality_data_{city.replace(" ", "_")}',
            python_callable=fetch_air_quality_data,
            op_args=[city],
        )