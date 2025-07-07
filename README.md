# Bank Reviews Data Warehouse Project
## Project Overview
Analyzing Customer Reviews of Bank Agencies in Morocco using a Modern Data Stack
This project implements a comprehensive data pipeline to collect, process, and analyze Google Maps reviews for bank agencies in Morocco. The goal is to extract valuable insights using sentiment analysis, topic modeling, and customer experience metrics.

## Objectives

-Collect customer reviews from Google Maps for Moroccan bank agencies
-Process unstructured review data through modern ETL pipelines
-Analyze sentiment trends and customer satisfaction patterns
-Visualize insights through interactive dashboards
-Automate the entire data pipeline for continuous monitoring

## Stack Technologique

| Étape                 | Outils/Tech utilisés                      |
|---------------------- |-------------------------------------------|
| Extraction de données | Python, Google Maps API, Scrapy, Selenium |
| Orchestration         | Apache Airflow                            |
| Stockage              | PostgreSQL                                |
| Transformation        | DBT (Data Build Tool)                     |
| Analyse & BI          | Looker Studio (Google Data Studio)        |
| Contrôle de version   | Git, GitHub                               |

## Folder Structure

bank_reviews_dw_project/                # Main directory.
├── data/                               # Raw and processed data files.
│
├── airflow/                            # Airflow setup for pipeline orchestration.
│   ├── dags/                           # DAG scripts for data collection and transformation.
│   └── logs/                           # Execution logs from Airflow tasks.
│
├── scripts/                            # Python scripts for scraping, cleaning, and enrichment (NLP).
│
├── projet_dbt/                         # DBT project for data transformation and modeling.
│   ├── analyses/                       # Optional ad hoc analysis SQL files.
│   ├── dbt_packages/                   # DBT dependencies (installed packages).
│   ├── logs/                           # DBT execution logs.
│   ├── macros/                         # Custom DBT macros (reusable SQL functions).
│   ├── models/                         # Main transformation logic (SQL models).
│   │   ├── staging/                    # Initial cleaned models from raw data.
│   │   └── marts/                      # Final fact and dimension models (star schema).
│   ├── seeds/                          # Static CSV datasets loaded into the warehouse.
│   ├── snapshots/                      # Historical snapshots of slowly changing data.
│   ├── target/                         # Compiled DBT outputs and artifacts.
│   └── tests/                          # DBT tests for data quality and integrity.
│
├── dashboards/                         # Looker Studio configs, links, or screenshots of dashboards.
│
└── docs/                               # Project documentation, reports, and setup guides.

###  Project Roadmap 

| **Phase** | **Focus**                           | **Key Deliverables**                                                                 |
|-----------|-------------------------------------|---------------------------------------------------------------------------------------|
| ✅ Phase 1 | **Data Collection**                 | - Google Maps scraping/API scripts<br>- Raw reviews stored in `/data/`<br>- Airflow DAG in `/airflow/dags/` to automate collection |
| ✅ Phase 2 | **Data Cleaning & Enrichment**      | - Text normalization, language detection, sentiment analysis, topic modeling (scripts in `/scripts/`)<br>- DBT staging models in `/projet_dbt/models/staging/` |
| ✅ Phase 3 | **Data Modeling (Star Schema)**     | - DBT marts models (`fact_reviews`, `dim_*`) in `/projet_dbt/models/marts/`<br>- SQL star schema in `/sql/` |
| ✅ Phase 4 | **Data Analytics & Dashboards**     | - Looker Studio dashboards linked in `/dashboards/`<br>- KPIs: sentiment trends, top topics, branch ranking |
| ✅ Phase 5 | **Pipeline Automation**             | - Airflow DAGs for regular updates (collection & DBT runs)<br>- Error alerting and scheduling setup |
| ✅ Phase 6 | **Documentation & Final Report**    | - Full documentation in `/docs/`<br>- Final report and presentation<br>- Clean `README.md` for GitHub |

## Phase 1: Data Collection (Scraping Google Maps Reviews)

### 1. Extraction des données

Les scripts Python [`google_maps_scraper.py`](./scripts/google_maps_scraper.py)  permet d’extraire les avis des agences bancaires marocaines via :

- **Web Scraping**  à l’aide de **BeautifulSoup** ou **seleniem**

#### ✅ Champs extraits :
...json

{
  "bank_name": "Nom de la banque",
  "branch_name": "Nom de l'agence",
  "location": "Adresse/GPS",
  "review_text": "Texte de l'avis",
  "rating": "Note (1-5)",
  "review_date": "YYYY-MM-DD"
}
### 2. Automatisation avec Apache Airflow

L’automatisation du processus d’extraction est assurée à l’aide d’un **DAG Airflow** défini dans [`bank_reviews_dag.py`](./dags/bank_reviews_dag.py).

Ce DAG permet de :

- **Planifier l’exécution** du script de scraping de manière quotidienne ou hebdomadaire
- **Automatiser l’ensemble du pipeline**
- **Charger automatiquement** les données brutes extraites dans PostgreSQL (table `stg_google_maps_reviews`)

#### Exemple de tâches dans le DAG

```python
from airflow.operators.python_operator import PythonOperator

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
