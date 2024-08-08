from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
    PythonOperator,
    PythonVirtualenvOperator,
    BranchPythonOperator
)

with DAG(
    'pyspark_movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='processing pyspark_movie',
   # schedule_interval=timedelta(days=1),
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 1, 5),
    catchup=True,
    tags=['movie', 'movie_db', 'pyspark'],
) as dag:

    def re_partition(ds_nodash):
        from spark_flow.repartition import re_partition
        df_row_cnt, read_path, write_path= re_partition(ds_nodash)
        print(f'df_row_cnt:{df_row_cnt}')
        print(f'read_path:{read_path}')
        print(f'write_path:{write_path}')


    re_task = PythonVirtualenvOperator(
	    task_id="re.partition",
    	python_callable=re_partition,
        system_site_packages=False,
        requirements=["git+https://github.com/Nicou11/spark_flow.git@240808/spark"],

    ) 


    join_df = BashOperator(
        task_id="join.df",
        bash_command='''
            echo "spark-submit....."
            echo "{{ds_nodash}}"
            ''',
    )
    
    
    agg_df = BashOperator(
        task_id="agg.df",
        bash_command='''
            echo "spark-submit....."
            echo "{{ds_nodash}}"
            ''',
    )


    task_end  = EmptyOperator(task_id='end', trigger_rule="all_done")
    start  = EmptyOperator(task_id='start')
    


    start >> re_task >> join_df
    join_df >> agg_df >> task_end
