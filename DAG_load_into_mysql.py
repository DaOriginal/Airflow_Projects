#my_dags path : /home/ghii-medu004/.local/lib/python3.10/site-packages/airflow/example_dags
#command to start airflow : airflow standalone
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import mysql.connector

    #Now Let's Connect to MySQL database
connection = mysql.connector.connect(
        host     = 'localhost',
        user     = 'root',
        password = 'root',
        database = 'airflow_test'
    )
cursor = connection.cursor()

#Let's define the DAG
dag = DAG(
    'data_workflow',
    description='A simple data workflow',
    schedule_interval='55 10 27 6 2', #-- Change this part for testing
    start_date=datetime(2023,6,27),
    catchup=False    
)

#Let's define a task for Data Extraction
def extract_data():
    #Define logic to extract data from a source
    csv_file_path = '/home/dereck-katuli/Desktop/My_Scripts/PYTHON_SCRIPTS/Airflow_Practice/mock_data.csv'
    data1 = pd.read_csv(csv_file_path)
    data = pd.DataFrame(data1, columns = ['id','first_name','last_name','email','gender'])
    return data

extract_task = PythonOperator(
    task_id = 'extract_data',
    python_callable = extract_data,
    dag = dag
)

#Let's define a task for Data Loading
def load_data(**context):
    #The below script will get the extracted data from the previous task
    extracted_data = context['ti'].xcom_pull(task_ids = 'extract_data')

    #Now Let's Connect to MySQL database
    # connection = mysql.connector.connect(
    #     host     = 'localhost',
    #     user     = 'root',
    #     password = 'root',
    #     database = 'airflow_test'
    # )
    # cursor = connection.cursor()

    #The below for loop will be used to iterate over the extracted data and load it into MySQL
    for record in extracted_data.index:
        query = "INSERT INTO clients (id,first_name,last_name,email,gender) VALUES (%s, %s,%s,%s,%s)"
        values = (int(extracted_data['id'][record]),extracted_data['first_name'][record],extracted_data['last_name'][record],extracted_data['email'][record],extracted_data['gender'][record])
        cursor.execute(query, values)

    #The below script will commit the changes and close the connection
    connection.commit()
    connection.close()

load_task = PythonOperator(
    task_id = 'load_data',
    python_callable = load_data,
    provide_context = True,
    dag = dag
)

#The script below will set task dependencies
extract_task >> load_task 

#Another piece of code to test
# with DAG('user_processing', start_date=datetime(2023,1,1),schedule_interval='@daily', catchup=False,template_searchpath=['/opt/aiflow/include']) as dag:
#     create_table = PostgresOperator(
#         task_id = 'create_table',
#         postgres_conn_id = 'postgres',
#         sql = 'sql/CREATE_TABLE_USERS.sql'
#     )

