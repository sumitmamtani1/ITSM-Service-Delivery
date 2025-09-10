from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='itsm_pipeline',
    default_args=default_args,
    description='Load ServiceNow CSV -> dbt -> validate',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['itsm'],
) as dag:

    
    load_csv_sql = """
    CREATE TABLE IF NOT EXISTS public.tickets (
      inc_business_service TEXT,
      inc_category TEXT,
      inc_number TEXT PRIMARY KEY,
      inc_priority TEXT,
      inc_sla_due TEXT,
      inc_sys_created_on TEXT,
      inc_resolved_at TEXT,
      inc_assigned_to TEXT,
      inc_state TEXT,
      inc_cmdb_ci TEXT,
      inc_caller_id TEXT,
      inc_short_description TEXT,
      inc_assignment_group TEXT,
      inc_close_code TEXT,
      inc_close_notes TEXT
    );
    TRUNCATE TABLE public.tickets;
    COPY public.tickets(
      inc_business_service,
      inc_category,
      inc_number,
      inc_priority,
      inc_sla_due,
      inc_sys_created_on,
      inc_resolved_at,
      inc_assigned_to,
      inc_state,
      inc_cmdb_ci,
      inc_caller_id,
      inc_short_description,
      inc_assignment_group,
      inc_close_code,
      inc_close_notes
    )
    FROM '/data/servicenow_tickets.csv'
    DELIMITER ',' CSV HEADER;
    """

    load_csv = PostgresOperator(
        task_id='load_csv_to_postgres',
        postgres_conn_id='postgres_default',
        sql=load_csv_sql
    )

    
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command=(
            "cd /opt/airflow/dbt_project/itsm_dbt && "
            "dbt deps --profiles-dir /opt/airflow/dbt_project || true && "
            "dbt run --profiles-dir /opt/airflow/dbt_project"
        )
    )

    
    validate = PostgresOperator(
        task_id='validate_staging',
        postgres_conn_id='postgres_default',
        sql="SELECT COUNT(*) FROM public.stg_tickets;"
    )

    load_csv >> run_dbt >> validate
