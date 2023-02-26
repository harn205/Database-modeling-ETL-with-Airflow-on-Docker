from environs import Env 
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import psycopg2
import numpy as np
import pandas_gbq
from google.oauth2 import service_account

env = Env()
env.read_env('/home/airflow/data/.env')
data_path = '/home/airflow/data'

def get_neighbourhoods_info():
    conn = psycopg2.connect(dbname= env('PG_DB'),user=env('PG_USER'),host=env('PG_HOST'),password=env('PG_PASSWORD'),port=env('PG_PORT'))
    cur = conn.cursor()
    sql = """SELECT neighbourhoods.id, neighbourhoods.neighbourhood, neighbourhoods_geo.geometry  
            FROM neighbourhoods
                LEFT JOIN neighbourhoods_geo
                    ON neighbourhoods.neighbourhood = neighbourhoods_geo.neighbourhood
    """
    cur.execute(sql)
    neighbourhoods_info = cur.fetchall()
    conn.close()
    
    neighbourhoods_info= pd.DataFrame(neighbourhoods_info)
    neighbourhoods_info.columns=['id','neighbourhood','geometry']
    neighbourhoods_info.to_csv(f"{data_path}/neighbourhoods_info.csv", index=False)


def get_listing_detail_info():
    conn = psycopg2.connect(dbname= env('PG_DB'),user=env('PG_USER'),host=env('PG_HOST'),password=env('PG_PASSWORD'),port=env('PG_PORT'))
    cur = conn.cursor()
    sql = """SELECT id, name, host_id, host_name, neighbourhood_cleansed, latitude,longitude, price
            FROM listings_details
    """
    cur.execute(sql)
    listings_details_info = cur.fetchall()
    conn.close()

    listings_details_info= pd.DataFrame(listings_details_info)
    listings_details_info.columns=['id','name', 'host_id','host_name', 'neighbourhood', 'latitude','longitude', 'price']
    listings_details_info.to_csv(f"{data_path}/listings_details_info.csv", index=False)

def get_reviews_info():
    conn = psycopg2.connect(dbname= env('PG_DB'),user=env('PG_USER'),host=env('PG_HOST'),password=env('PG_PASSWORD'),port=env('PG_PORT'))
    cur = conn.cursor()
    sql = """SELECT reviews_details.id, reviews_details.listing_id, reviews_details.date, reviews_details.reviewer_name,reviews_details.comments,
            listings_details.name, listings_details.host_id, listings_details.host_name, listings_details.review_scores_rating
            FROM reviews_details
                LEFT JOIN listings_details
                    ON reviews_details.listing_id = listings_details.id
    """
    cur.execute(sql)
    reviews_info = cur.fetchall()
    conn.close()

    reviews_info= pd.DataFrame(reviews_info)
    reviews_info.columns=['review_id','listing_id','date','reviewer_name','review','listing_name','host_id','host_name','scores']
    reviews_info.to_csv(f"{data_path}/reviews_info.csv", index=False)

def clean_data():
    #cleaning listings_details_info
    listings_details_info = pd.read_csv(f"{data_path}/listings_details_info.csv", index=False)
    
    listings_details_info["price"] = listings_details_info.apply(lambda x: x["price"].replace("$",""), axis=1)
    listings_details_info["price"] = listings_details_info.apply(lambda x: x["price"].replace(",",""), axis=1)
    
    listings_details_info["price"] = listings_details_info["price"].astype(float)
    listings_details_info["host_id"] = listings_details_info["host_id"].astype(int)

    listings_details_info.to_csv(f"{data_path}/listings_details_info.csv", index=False)
    #cleaning reviews_info
    reviews_info = pd.read_csv(f"{data_path}/reviews_info.csv", index=False)

    reviews_info = reviews_info.replace('NaN',np.nan)

    reviews_info["host_id"] = reviews_info["host_id"].astype(int)
    reviews_info["scores"] = reviews_info["scores"].astype(float)

    reviews_info.to_csv(f"{data_path}/reviews_info.csv", index=False)

def load_to_gbq(file_name,table_id):
    credential = service_account.Credentials.from_service_account_file('/home/airflow/data/earnest-topic-376419-fd168024c1d9.json')
    df = pd.read_csv(data_path+'/'+file_name)
    pandas_gbq.to_gbq(df, table_id, project_id='earnest-topic-376419', if_exists='append', credentials=credential)


    

with DAG(
    "ETL_with_Docker",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["project"]
) as dag:

    dag.doc_md = """
    # Project

    ## ETL job with Docker
    """

    t1 = PythonOperator(
        task_id="get_neighbourhoods_info",
        python_callable=get_neighbourhoods_info,
        dag=dag,
    )

    t2 = PythonOperator(
        task_id="get_listing_detail_info",
        python_callable=get_listing_detail_info,
        dag=dag,
    )

    t3 = PythonOperator(
        task_id="get_reviews_info",
        python_callable=get_reviews_info,
        dag=dag,
    )

    t4 = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
        dag=dag,
    )
    
    t5 = PythonOperator(
        task_id="load_neighbourhoods_info_to_gbq",
        python_callable=load_to_gbq,
        op_kwargs={
            "file_name": "neighbourhoods_info.csv",
            "table_id": "earnest-topic-376419.etl_with_docker.neighbourhoods_info"
        },
    )

    t6 = PythonOperator(
        task_id="load_listings_details_info_to_gbq",
        python_callable=load_to_gbq,
        op_kwargs={
            "file_name": "listings_details_info.csv",
            "table_id": "earnest-topic-376419.etl_with_docker.listing_detail_info"
        },
    )

    t7 = PythonOperator(
        task_id="load_reviews_info_to_gbq",
        python_callable=load_to_gbq,
        op_kwargs={
            "file_name": "reviews_info.csv",
            "table_id": "earnest-topic-376419.etl_with_docker.reviews_info"
        },
    )

    [t1, t2, t3] >> t4 >> [t5, t6, t7]