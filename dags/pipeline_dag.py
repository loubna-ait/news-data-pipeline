from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="news_pipeline",
    start_date=datetime(2025, 1, 1),
    # schedule_interval="@hourly",
    schedule_interval="* * * * * *",
    catchup=False,
) as dag:

    # scraping = BashOperator(
    #     task_id="scraping",
    #     bash_command="cd /opt/airflow/project && python scraper.py"
    # )
    scraping = BashOperator(
        task_id="scraping",
        bash_command="""
            cd /opt/airflow/project &&
            python consumer_bronze.py &
            python scraper.py &&
            sleep 15 &&
            kill %1 || true
        """
    )
    silver = BashOperator(
        task_id="silver_cleaning",
        bash_command="cd /opt/airflow/project && python silver_cleaner.py"
    )
    quality = BashOperator(
        task_id="quality_checks",
        bash_command="cd /opt/airflow/project && python quality_checks.py"
    )
    gold = BashOperator(
        task_id="gold_transformation",
        bash_command="cd /opt/airflow/project && python gold_transformer.py"
    )
    dwh = BashOperator(
        task_id="load_to_dwh",
        bash_command="cd /opt/airflow/project && python load_to_dwh.py"
    )

    scraping >> silver >> quality >> gold >> dwh