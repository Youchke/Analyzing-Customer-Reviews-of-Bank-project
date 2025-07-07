from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
sys.path.append("/home/younes/airflow/scripts")
import extract_maps
import multilingual_sentiment_analysis
import topic_modeling
import pandas as pd
from sqlalchemy import create_engine

# Définition des arguments par défaut
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définition du DAG
dag = DAG(
    'bank_reviews_dag',
    default_args=default_args,
    schedule_interval=None,
    description="DAG de data warehouse pour effectuer l'extraction et le chargement des données  .",
   # schedule_interval=timedelta(days=1), 
)

def load():
    df = pd.read_json("/home/younes/airflow/data/all_reviews2.json")
    df.index.name = 'id'
    df = df.reset_index()
    engine = create_engine('postgresql://review_user:review_user@localhost:5432/airflow_db')
    df.to_sql('staging_reviews', engine, if_exists='replace', index=True)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_maps.extract,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load,
    dag=dag,
)


# Phase 2: Data Cleaning & Transformation Clean the Data (DBT & SQL)

dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    # bash_command='cd ~/reviews_prouject/dbt_project/outputs && dbt run --models staging+'
    bash_command='cd ~/reviews_prouject/dbt_project/outputs && dbt run --models stg_reviews_cleaned'

)

# Phase 2: Data Cleaning & Transformation / Enrich the Data (Extract language and  Classify reviews into "Positive," "Negative," "Neutral" using sentiment analysis).

enrich_the_data = PythonOperator(
    task_id='enrich_the_data',
    python_callable=multilingual_sentiment_analysis.enrich_reviews_data,
    dag=dag,
)
# Phase 2: Data Cleaning & Transformation / Extract common topics using NLP (Latent Dirichlet Allocation - LDA).
extract_topics = PythonOperator(
    task_id='extract_topics',
    python_callable=topic_modeling.main,
    dag=dag,
)
# # Phase 3: Load Data into PostgreSQL Implement transformation models in DBT

dbt_run_marts = BashOperator(
    task_id='dbt_run_marts',
    bash_command='cd ~/reviews_prouject/dbt_project/outputs && dbt run --models marts+'
)

# Phase 4 :create views pourSentiment trend per bank & branch./Top positive & negative topics./Performance ranking of branches/Customer experience insights.
dbt_run_views = BashOperator(
    task_id='dbt_run_views',
    bash_command='cd ~/reviews_prouject/dbt_project/outputs && dbt run --models views+'
)



# Exécuter dbt test pour valider les transformations
#dbt_test = BashOperator(
 #   task_id='dbt_test',
 #   bash_command='cd ~/dbt_reviews && dbt build && dbt test'
#)

#extract_task >> load_task >> dbt_run >> dbt_test
# extract_task # >>
load_task >> dbt_run_staging  >> enrich_the_data >> extract_topics  >> dbt_run_marts >> dbt_run_views