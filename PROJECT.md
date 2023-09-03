# E-Commerce Data Pipeline

## 1. Extract

For the pipeline data extraction phase all the functions will be inside the `src/extract.py` module.

If you want to check your code meets the requirements, you can test that particular module simply by running:

```console
$ pytest tests/test_extract.py
```

## 2. Load

Now you have all the data from different sources, it's time to store that in a Data Warehouse. We will use SQLite as our database engine to keep things simpler but, in larger companies, Snowflake is one of the most popular options for Data Warehouses these days.

All the functions will be inside the `src/load.py` module.

## 3. Transform

Having the data inside our Data Warehouse, we can start making queries and transformations.

All the functions will be inside the `src/src/extract.py` module.

You can make use of other tools like DBeaver to write and test the queries in a more interactive way. Lastly, you can check your queries meets the requirements by running the provided tests with:

```console
$ pytest tests/test_transform.py
```

You can also validate how the output from the query should look like checking the `.json` file under `tests/query_results` with the same name as the `.sql` file.

## 4. Visualize your results

Finally, having all the results from our queries, it's time to start making some visualization for the presentation.

For this, you should open the `EDA.ipynb` Jupyter notebook provided. In here is all the code to see the visualizations needed.

## 5. Data Orchestration with Apache Airflow

Current ELT pipeline code to create an Airflow DAG to automatize the entire process. The code is in the `/airflow` module and it wil replicate the project inside the Apache Airflow container.

## Project Structure

Before starting to work, let's take a deep overview of the project structure and each module inside:

```console
├── dataset
│   ├── olist_customers_dataset.csv
│   ├── olist_geolocation_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_orders_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   └── product_category_name_translation.csv
├── images
│   ├── data_schema.png
│   ├── freight_value_weight_relationship.png
│   └── orders_per_day_and_holidays.png
├── queries
│   ├── delivery_date_difference.sql
│   ├── global_ammount_order_status.sql
│   ├── real_vs_estimated_delivered_time.sql
│   ├── revenue_by_month_year.sql
│   ├── revenue_per_state.sql
│   ├── top_10_least_revenue_categories.sql
│   └── top_10_revenue_categories.sql
├── src
│   ├── __init__.py
│   ├── config.py
│   ├── extract.py
│   ├── load.py
│   ├── plots.py
│   └── transform.py
└── tests
│   ├── __init__.py
│   ├── query_results/
│   ├── test_extract.py
│   └── test_transform.py
├── airflow
│   ├── dags
│   │   ├── queries
│   │   │   ├── delivery_date_difference.sql
│   │   │   ├── global_ammount_order_status.sql
│   │   │   ├── real_vs_estimated_delivered_time.sql
│   │   │   ├── revenue_by_month_year.sql
│   │   │   ├── revenue_per_state.sql
│   │   │   ├── top_10_least_revenue_categories.sql
│   │   │   └── top_10_revenue_categories.sql|
│   │   ├── src
│   │   │   ├── __init__.py
│   │   │   ├── config.py
│   │   │   ├── extract.py
│   │   │   ├── load.py
│   │   │   ├── plots.py
│   │   │   └── transform.py
│   │   └── elt_sprin1_dag.py
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   └── requirements.txt
├── ASSIGNMENT.md
├── EDA.ipynb
├── README.md
└── requirements.txt
```

Now let's look at the main components:

### dataset

It has all the .csvs with the information that will be used in the project.

- `dataset/olist_customers_dataset.csv`: csv with info regarding the location of the customers.
- `dataset/olist_order_items_dataset.csv`: csv with info regarding the shipping.
- `dataset/olist_order_payments_dataset.csv`: csv with info regarding the payment.
- `dataset/olist_order_reviews_dataset.csv`: csv with info regarding the clients' reviews.
- `dataset/olist_orders_dataset.csv`: csv with info regarding the different dates of each sale's process.
- `dataset/olist_products_dataset.csv`: csv with info regarding the details of each product.
- `dataset/olist_sellers_dataset.csv`: csv with info regarding the location of the sellers.
- `dataset/product_category_name_translation.csv`: csv with info regarding the translation of each category from Portuguese to English.

### queries

It contains all the SQL queries and the code you will need to complete, to later create tables and plots.

- `queries/delivery_date_difference.sql`: This query will return a table with two columns; State, and Delivery_Difference. The first one will have the letters that identify the states, and the second one the average difference between the estimated delivery date and the date when the items were actually delivered to the customer.
- `queries/global_ammount_order_status.sql`: This query will return a table with two columns; order_status, and Amount. The first one will have the different order status classes and the second one the total amount of each.
- `queries/real_vs_estimated_delivered_time.sql`: This query will return a table with the differences between the real and estimated delivery times by month and year. It will have different columns: month_no, with the month numbers going from 01 to 12; month, with the 3 first letters of each month (e.g. Jan, Feb); Year2016_real_time, with the average delivery time per month of 2016 (NaN if it doesn't exist); Year2017_real_time, with the average delivery time per month of 2017 (NaN if it doesn't exist); Year2018_real_time, with the average delivery time per month of 2018 (NaN if it doesn't exist); Year2016_estimated_time, with the average estimated delivery time per month of 2016 (NaN if it doesn't exist); Year2017_estimated_time, with the average estimated delivery time per month of 2017 (NaN if it doesn't exist) and Year2018_estimated_time, with the average estimated delivery time per month of 2018 (NaN if it doesn't exist).
- `queries/revenue_by_month_year.sql`: This query will return a table with the revenue by month and year. It will have different columns: month_no, with the month numbers going from 01 to 12; month, with the 3 first letters of each month (e.g. Jan, Feb); Year2016, with the revenue per month of 2016 (0.00 if it doesn't exist); Year2017, with the revenue per month of 2017 (0.00 if it doesn't exist) and Year2018, with the revenue per month of 2018 (0.00 if it doesn't exist).
- `queries/revenue_per_state.sql`: This query will return a table with two columns; customer_state, and Revenue. The first one will have the letters that identify the top 10 states with the most revenue and the second one the total revenue of each.
- `queries/top_10_least_revenue_categories.sql`: This query will return a table with the top 10 least revenue categories in English, the number of orders, and their total revenue. The first column will be Category, which will contain the top 10 least revenue categories; the second one will be Num_order, with the total amount of orders of each category; and the last one will be Revenue, with the total revenue of each category.
- `queries/top_10_revenue_catgories.sql`: This query will return a table with the top 10 revenue categories in English, the number of orders, and their total revenue. The first column will be Category, which will contain the top 10 revenue categories; the second one will be Num_order, with the total amount of orders of each category; and the last one will be Revenue, with the total revenue of each category.

### src

The source that contains different files needed for the whole project to work.

- `src/_init__.py`: File required to make Python treat directories containing the other files in the folder as a package.
- `src/config.py`: File that contains the configuration of root paths.
- `src/extract.py`: File that extracts the data from the .csv and API files and loads them into dataframes.
- `src/load.py`: File that loads the dataframes into the SQLite databases.
- `src/plots.py`: File where all the plotting functions are.
- `src/transform.py`: File that transforms the queries into tables.

### tests

Folder with the necessary files to test the project.

- query_results: This folder contains all the .json files that will be used to test the queries you created.
- `tests/_init__.py`: File required to make Python treat directories containing the other files in the folder as a package.
- `tests/test_extract.py`: File that tests if the query functions have been extracted properly.
- `tests/test_transform.py`: File that tests if the query functions have been created in the proper tables.

### Airflow

Folder with the necessary to have the project run as a dag inside de Apache Airflow arquitecture.

- `dags`: Folder with the exact python files in the project (`src`,`queries`) and a dag (`elt_sprint1_dag.py`) where it will reference the files in the folders previously mencioned
- `docker-compose.yaml`: Defines Airflow and Postgres containers with necessary configurations..
- `Dockerfile`: Specifies image, dependencies, and copies project files into the container.


### Others

- `ASSIGNMENT.md`: File that has key information to understand the project.
- `EDA.ipynb`: File that unifies the different parts of the project and tells you the steps you should follow to properly complete this project.
- `README.md`: File that explains the business problem, the data you will consume, the expected code style and technical aspects.
- `requirements.txt`: File that contains all the libraries that need to be installed.

