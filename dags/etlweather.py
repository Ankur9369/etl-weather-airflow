#======================================================================================
#              impot
#======================================================================================
from airflow import DAG
#  this is the library that allows us to use the HTTP hook to make API calls
from airflow.providers.http.hooks.http import HttpHook
# there is various hook for the  varios  DB calling 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta
import requests
import json

#======================================================================================
# Latitude and longtitude  for the  desired loction (london in this case)
LATITUDE ='51.5074'
LONGITUDE ='-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'
default_args={ 
    
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
}

## Dag definition
with DAG(
        dag_id='etl_weather',
        default_args=default_args,
        schedule='@daily',
        catchup=False,
        ) as dags:
    @task()
    def extract_weather_data():
        """
        This task extracts weather data from the Open-Meteo API for a specific location (London in this case).
        It uses the HTTP hook to make an API call and retrieves the weather data in JSON format.
        The extracted data is returned as a dictionary.
        """
        # https hooks to get the data from the api
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        # api  endpoint  for the weather data
        ## https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        
        endpoint = f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=True"
        
        ## Make the Api call and get the response
        response = http_hook.run(endpoint)
        
        
        if response.status_code != 200:
            raise Exception(f"API call failed with status code {response.status_code}") 
        return response.json()
    @task()
    def transform_weather_data(weather_data):
        """
        this task transform the data to the desired format for the database insertion
        """   
        current_wearher=weather_data['current_weather']
        transformed_data = {
            'temperature': current_wearher['temperature'],
            'windspeed': current_wearher['windspeed'],
            'winddirection': current_wearher['winddirection'],
            'weathercode': current_wearher['weathercode'],
            'time': current_wearher['time'],
            'latitude': LATITUDE,
            'longitude': LONGITUDE

        }
        return transformed_data


    @task()
    def load_weather_data(transformed_data):
        """
        This task loads the transformed weather data into a PostgreSQL database.
        It uses the Postgres hook to connect to the database and execute an INSERT statement.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn=pg_hook.get_conn()
        cursor= conn.cursor()
        # create the table if it does not exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data(
                latitude FLOAT
                ,longitude FLOAT
                ,temperature FLOAT
                ,windspeed FLOAT
                ,winddirection FLOAT
                ,weathercode INT
                ,time TIMESTAMP
                );
                """)
        # insert the data into the table
        cursor.execute("""
            INSERT INTO weather_data (temperature, windspeed, winddirection, weathercode, time, latitude, longitude)
        
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        , (
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode'],
            transformed_data['time'],
            transformed_data['latitude'],
            transformed_data['longitude']
        ))
        conn.commit()
        cursor.close()
        conn.close()

    ## Dag workflow -- etl pipeline 
    weather_data= extract_weather_data()
    transformed_data=transform_weather_data(weather_data)
    load_weather_data(transformed_data)
     