# My airflow directory : /home/dereck-katuli/airflow/dags
import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago

args = {'owner': 'airflow'}

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_psql = DAG(
    dag_id="postgres_operator_dag",
    default_args=args,
    # start_date=datetime.datetime(2023, 26, 9),
    # schedule_interval="@hourly",
    schedule_interval="27 14 26 09 *",
    dagrun_timeout=timedelta(minutes=60),
    description='postgresql logic in airflow',
    start_date=airflow.utils.dates.days_ago(1),
    template_searchpath="/home/dereck-katuli/Documents/airflow_tests",
    catchup=True
)
drop_tables = PostgresOperator(
    task_id="drop_tables",
    postgres_conn_id="postgres_default",
    sql="drop_tables.sql",
    # sql = """
    #     drop table patient_drug_dispensations;
    #     """,
    dag=dag_psql
)

create_tables = PostgresOperator(
    task_id="create_tables",
    postgres_conn_id="postgres_default",
    sql="create_tables.sql",
    # sql = """
    #     create table patient_drug_dispensations as
    #     select
    #     p.patient_id ,
    #     p.date_of_birth ,
    #     p.gender,
    #     dd.drug_name,
    #     dd.dispense_date ,
    #     dd.quantity_dispensed
    #     from patients p
    #     inner join drug_dispensations dd on p.patient_id  = dd.patient_id;
    #      """,
    autocommit=True,
    dag=dag_psql
)

summary_stats = PostgresOperator(
    task_id="summary_stats",
    postgres_conn_id="postgres_default",
    params={"year": "'2023'"},
    sql="insert_tables.sql",
    # sql="""
    #         insert into summaries(table_name,total_records) (
    #         select 'patient_drug_dispensations' table_name ,count(*) Total_Records from patient_drug_dispensations
    #         where extract(year from dispense_date) =  {{params.year}}
    #         );
    #       """,
    autocommit=True,
    dag=dag_psql
)

drop_tables >> create_tables >> summary_stats

if __name__ == "__main__":
    dag_psql.cli()
