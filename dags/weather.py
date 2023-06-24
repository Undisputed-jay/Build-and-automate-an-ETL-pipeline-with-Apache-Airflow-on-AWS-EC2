import json
import boto3
import psycopg2
import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from airflow.operators.email import EmailOperator

USER = Variable.get('user')
PASSWORD= Variable.get('password')
HOST = Variable.get('host')
ACCESS_KEY = Variable.get('access_key')
SECRET_KEY = Variable.get('secret_key')
API = Variable.get('api')



default_arguments = {
    'owner': 'ahmed',
    'start_date': datetime(2023, 6, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
    'email' : ['bayodele73@gmail.com'],
    'email_on retry': True,
    'email_on_failure': True
}

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit

def transformed_data(task_instance):
    data = task_instance.xcom_pull(task_ids = 'extract_data')
    city = data['name']
    temp_in_fahrenheit = kelvin_to_fahrenheit(data['main']['temp'])
    weather_description = data['weather'][0]['description']
    feels_like_fahrenheit = kelvin_to_fahrenheit(data['main']['feels_like'])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp_min'])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp_max'])
    pressure = data['main']['pressure']
    humidity = data['main']['humidity']
    wind_speed = data['wind']['speed']
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])
        
    transformed_data = { 'City': city, 'Weather Description': weather_description,
                            'Feels Like (F)': feels_like_fahrenheit, 'Temperature (F)': temp_in_fahrenheit,
                            'Minimum Temperature (F)': min_temp_fahrenheit, 'Maximum Temperature (F)': max_temp_fahrenheit,
                            'Pressure': pressure, 'Humidity': humidity, 'Wind Speed': wind_speed,
                            'Time of Record': time_of_record, 'Sunrise (Local Time)': sunrise, 'Sunset (Local Time)': sunset}
        
        
    transformed_list = [transformed_data]
    df = pd.DataFrame(data = transformed_list)
    df.to_csv('/opt/airflow/data/london_weather_report.csv', index = False)

def upload_to_s3(filename, s3_bucket, s3_key):
    s3 = boto3.client("s3", aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    s3.upload_file(
        filename, 
        s3_bucket, 
        s3_key
    )

def s3_to_redshift():
    conn_string = "dbname = '{}' port = '{}' user = '{}' password = '{}' host = '{}'"\
        .format('dev', 5439, USER, PASSWORD, HOST)
    
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    sqlQuery = '''copy public.weather
    from 's3://weather-openapi/london_weather_report.csv'
    iam_role 'arn:aws:iam::970884250493:role/service-role/AmazonRedshift-CommandsAccessRole-20230624T153527'
    delimiter ','
    IGNOREHEADER as 1
    csv;'''
    cursor.execute(sqlQuery)
    conn.commit()
    conn.close()

with DAG(
    dag_id = 'Weather_API',
    catchup = False,
    default_args = default_arguments,
    schedule = '@daily'
) as dag:
    
    # check weatherAPI is right
    t1 = HttpSensor(
        task_id = 'is_weather_api_ready',
        http_conn_id = 'weathermap_api',
        endpoint = '/data/2.5/weather?q=london&appid=1722f54caebd6f4a0f4a3d7dc4de5869'
    )
    
    # extract data from the weatherAPI
    t2 = SimpleHttpOperator(
        task_id = 'extract_data',
        method = 'GET',
        http_conn_id = 'weathermap_api',
        endpoint = '/data/2.5/weather?q=london&appid=1722f54caebd6f4a0f4a3d7dc4de5869',
        response_filter = lambda r: json.loads(r.text),
        log_response = True
    )
    
    t3 = PythonOperator(
        task_id = 'transform_data',
        python_callable = transformed_data
    )
    
    t4 = PythonOperator(
        task_id = 'Upload_to_S3',
        python_callable = upload_to_s3,
        op_args = ['/opt/airflow/data/london_weather_report.csv', 'weather-openapi', 'london_weather_report.csv']
    )
    
    t5 = PythonOperator(
        task_id = 'store_in_redshift',
        python_callable = s3_to_redshift
    )
    
    # t6 = EmailOperator(
        # task_id ='send_email',
        # to = ['bayodele73@gmail.com'],
        # subject='Task Completed',
        # html_content = "Date: {{ ds }}"
    # )
    
    t1 >> t2 >> t3 >> t4 >> t5 