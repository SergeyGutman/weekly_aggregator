import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "dag_weekly_aggr",
    default_args = {
        "owner": "Sergey Gutman",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "0 7 * * *"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Aggregation started"),
    dag=dag
)

scala_aggregator = SparkSubmitOperator(
    task_id="scala_aggregator",
    conn_id="spark-conn",
    application="src/main/scala/target/scala-2.12/csvaggregator_2.12-0.1.jar",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Aggregation completed successfully"),
    dag=dag
)

start >> scala_aggregator >> end
