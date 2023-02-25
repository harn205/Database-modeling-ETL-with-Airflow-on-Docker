# Database Modeling and Creation & ETL Data Pipeline using Apache Airflow with Docker

![project_overview](images/project_overview.PNG)
### Project overview:

For this personal project, 
- I downloaded a [dataset from Kaggle](https://www.kaggle.com/datasets/erikbruin/airbnb-amsterdam) and **designed a PostgresSQL database model** via `Google Cloud SQL` to store that dataset. I also designed a database ER diagram by myself to ensure a well-structured and organized database.
  - I used **Pandas to understand and clean the data in the dataset**, ensuring data accuracy and consistency. This step helped me to get a better understanding of   the data and its characteristics.

- **To automate the data extraction, transformation, and loading stages, I built an ETL pipeline** using `Apache Airflow on Docker`. Setting up the Apache Airflow environment on Docker was challenging, but it allowed me to run the pipeline efficiently on my local machine.

- For **the extraction stage**, I queried the data from PostgreSQL created in the first step. Then, I used **Pandas for the transformation and cleaning stage**, where I  transformed and cleaned the data to make it ready for loading.

- Finally, **I loaded the final clean data to the data warehouse** on `Google BigQuery` using the ETL pipeline created in the previous step. This allowed me to store the data in a well-organized manner and provided me with an efficient way to query the data for further analysis.

Overall, this project showcases my skills in data engineering, data modeling, and data warehousing. It also highlights my ability to work with different technologies such as Google Cloud SQL, Pandas, Docker, Apache Airflow, and Google BigQuery.




## STEP 1: Understanding the dataset being used.
I downloaded the [Airbnb Amsterdam dataset](https://www.kaggle.com/datasets/erikbruin/airbnb-amsterdam) from Kaggle.

![dataset](images/dataset.PNG)

The dataset contains 7 files (6 .csv files and 1 .geojson file). Each of the files contains Airbnb Amsterdam data for different categories, with some column of them being related to others.

## STEP 2: Database modeling and Creation
After understanding the dataset files from previous step with Pandas, I designed the database ER-diagram to be like this.
![ER](images/ER.png)

I created a PostgreSQL database via Google Cloud SQL based on the ER-diagram that I designed by cleaning the data with Pandas and using PostgreSQL syntax to create and insert tables for each file in the dataset.

![database](images/database.PNG)
