# Airbnb ETL Pipeline with Metaflow

## Introduction

This project demonstrates a robust and scalable ETL (Extract, Transform, Load) pipeline to process the Airbnb New York City dataset. The pipeline efficiently extracts data from a PostgreSQL database, applies various transformations to enhance and clean the data, and then loads it into a new table for further analysis. Metaflow is used for workflow orchestration and management, ensuring reproducibility and reliability.

The primary goals of this project are:

- **Data Ingestion:** Efficiently load the Airbnb dataset into a PostgreSQL database.
- **Data Transformation:** Clean, enrich, and prepare the data for analysis by handling missing values, calculating relevant metrics, and engineering new features.
- **Workflow Orchestration:** Utilize Metaflow to create a structured, maintainable, and scalable workflow for the ETL process.

## Table of Contents

- [Introduction](#introduction)
- [Getting Started](#getting-started)
- [Data Source](#data-source)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [ETL Pipeline](#etl-pipeline)
  - [Extraction](#extraction)
  - [Transformation](#transformation)
  - [Loading](#loading)
- [Metaflow Workflow](#metaflow-workflow)
- [How to Run](#how-to-run)
- [Demonstration](#demonstration)

## Getting Started

### Prerequisites

- Python (3.9 OR higher)
- PostgreSQL (Local Installation)
- Metaflow
- Libraries: pandas, sqlalchemy, psycopg2, metaflow, python-dotenv, tabulate
- Virtual Environment (e.g., virtualenv, conda)

#### Additional Notes

- This repository includes a pre-configured virtual environment (`data_engineer_env`) for your convenience. You can activate it using the instructions provided in the "Getting Started" section.
- If you prefer to create your own virtual environment, you can delete the `data_engineer_env` folder and follow the standard steps for creating and activating a new environment.

### Installation / Setup

1. Clone this repository: `git clone https://github.com/SaitejaNagishetty/Saiteja-Nagishetty-DE-Assignment-AI-Planet.git`
2. Create a virtual environment (run the following commands in the root directory of project folder):

   ```
   # macOS/Linux
   python3 -m venv data_engineer_env

   # Windows
   python -m venv data_engineer_env
   ```

3. Activate the Virtual Environment:

   ```
   # macOS/Linux
   ource data_engineer_env/bin/activate

   # Windows
   .\data_engineer_env\Scripts\activate
   ```

4. install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

5. Set up PostgreSQL

   If you don't have PostgreSQL installed, download and install it from the [official website](https://www.postgresql.org/download/).

   Create a database named `airbnb_nyc`:

   #### macOS/Linux (using `psql`):

   ```bash
   psql -U postgres -c "CREATE DATABASE airbnb_nyc;"
   ```

   #### Windows (using `pgAdmin` or `psql`):

   ```bash
   psql -U postgres -c "CREATE DATABASE airbnb_nyc;"
   ```

6. Configuration

   In the file named config.py in the project's root directory.
   Add your database credentials in the following format:

   ```Python
   DATABASE_URL = "postgresql://postgres:<your_password>@localhost/airbnb_nyc"
   ```

## Data Source

This ETL pipeline uses the Airbnb New York City dataset available on Kaggle: [https://www.kaggle.com/datasets/dgomonov/new-york-city-airbnb-open-data](https://www.kaggle.com/datasets/dgomonov/new-york-city-airbnb-open-data)

The dataset provides a comprehensive overview of Airbnb listings in New York City in 2019. It includes information such as:

- **Listing ID**
- **Listing Name**
- **Host ID**
- **Host Name**
- **Neighbourhood Group** (borough)
- **Neighbourhood**
- **Latitude**
- **Longitude**
- **Room Type**
- **Price**
- **Minimum Nights**
- **Number of Reviews**
- **Last Review Date**
- **Reviews per Month**
- **Calculated Host Listings Count**
- **Availability 365**

The dataset contains approximately 49,000 records and is in CSV format.

## Project Structure

The project is organized into the following directories and files:

- **`data/`:** Contains the raw Airbnb dataset (`AB_NYC_2019.csv`).
- **`src/`:** Contains the Python source code for the ETL pipeline:
  - `etl.py`: Main script to orchestrate the entire ETL process.
  - `extract.py`: Script for extracting data from the database.
  - `load_data.py`: Script for loading the Raw data into the database.
  - `load.py`: Script for loading the transformed data into the database.
  - `transform.py`: Script for transforming the data.
- **`airbnb_flow.py`:** Defines the Metaflow flow for orchestrating the ETL pipeline.
- **`config.py`:** Stores the database connection configuration.
- **`models.py`:** Defines the SQLAlchemy model (table schema) for the Airbnb data.
- **`config.py`:** Stores the database connection configuration.
- **`README.md`:** This file, providing project documentation.
- **`requirements.txt`:** Lists the required Python libraries for the project.

## Configuration

This project utilizes a `config.py` file located in the project's root directory to store sensitive configuration details, such as the PostgreSQL database connection URL. I took this approach becuase it ensures that these credentials are not hardcoded in the scripts and are not accidentally exposed in version control.

**config.py:**

```python
DATABASE_URL = "postgresql://postgres:<your_password>@localhost/airbnb_nyc"
TRANSFORMED_TABLE_NAME = "listings_transformed"
```

## ETL Pipeline

This ETL (Extract, Transform, Load) pipeline is designed to process Airbnb listing data for New York City. It consists of three main steps: extraction, transformation, and loading. The pipeline is orchestrated using Metaflow, which ensures the reproducibility and reliability of the process.

### Extraction (`src/extract.py`)

The extraction step retrieves all data from the listings table in the PostgreSQL database. It uses SQLAlchemy, a Python SQL toolkit and Object-Relational Mapper (ORM), to interact with the database. The data is fetched in chunks using `fetchmany()` to avoid loading the entire dataset into memory at once, ensuring scalability for larger datasets. The retrieved data is then converted into a Pandas DataFrame for further processing.

**Key Points:**

- **SQLAlchemy Engine:** Used to establish a connection to the database.
- **Session:** Creates a SQLAlchemy Session to manage the database connection and transactions.
- **Query:** A raw SQL query (`SELECT * FROM listings`) is executed to fetch all data from the listings table.
- **Chunking:** Fetches data in manageable chunks of 1000 rows each to optimize memory usage.
- **DataFrame:** Converts the fetched data into a Pandas DataFrame for convenient manipulation in the subsequent steps.
- **Error Handling:** Includes try-except blocks to handle potential errors during the extraction process, such as database connection errors or SQL syntax errors.

### Transformation (`src/transform.py`)

The transformation step takes the extracted DataFrame and applies various operations to clean, normalize, and enhance the data. The specific transformations performed include:

**Data Type Conversion:**

- `last_review:` This column is converted to the datetime data type to ensure consistent handling of date values.

**Handling Missing Values:**

- `reviews_per_month:` Missing values in this column are replaced with 0, assuming no reviews mean zero reviews per month.
- `price, name:` Rows with missing values in these critical columns are dropped since these attributes are essential for analysis.

**Feature Engineering:**

- `is_superhost:` A binary feature (1 or 0) is created to indicate whether a host is a superhost, determined by the presence of "superhost" in their name.
- `is_longterm:` A binary feature (1 or 0) is created to indicate if a listing requires a minimum stay of more than seven nights.

**Calculating Additional Metrics:**

- `price_per_person:` Calculates the price per person by dividing the price by 2 for "Private room" listings (assuming two guests) and leaving it unchanged for other room types.
- `avg_price_per_neighbourhood:` Calculates the average price for each neighbourhood_group, providing insights into pricing trends across different areas.

**Normalizing the Data:**

- The `last_review` column is split into separate columns for year, month, and day (`last_review_year, last_review_month,` and `last_review_day`), facilitating time-based analysis.

**Removing Unnecessary Columns:**

- `host_name` and `last_review:` These columns are dropped as they are either redundant (`host_name` can be obtained by joining with a host table) or their information has been extracted into separate columns.

### Loading (`src/load.py`)

The loading step takes the transformed DataFrame and inserts it into a new table named `listings_transformed` in the PostgreSQL database. The script ensures that the table is either created or replaced (if it already exists) before the data is loaded.

**Key Points:**

- **Database Connection:** The script uses the same database engine from the previous steps to establish a connection.
- **Session:** Uses a SQLAlchemy Session to ensure data integrity and manage transactions.
- **Table Handling:** Checks if the `listings_transformed` table exists and drops it if it does, preventing data duplication.
- **Data Loading:** Efficiently loads the data from the DataFrame into the table using `df.to_sql`.
- **Error Handling:** Includes a try-except block to handle potential SQL errors during the loading process.

### Metaflow Workflow (`airbnb_flow.py`)

Metaflow is a powerful framework for building and managing data science and machine learning workflows. In this project, Metaflow is utilized to orchestrate the entire ETL pipeline, ensuring a streamlined and reproducible process.

**Flow Structure:**
The `AirbnbETLFlow` class in `airbnb_flow.py` defines the Metaflow flow for the ETL pipeline. The flow consists of five distinct steps:

1. **start:** Initializes the flow by setting up the database connection using the `DATABASE_URL` from `config.py`.
2. **extract:** Calls the `extract_data` function from `src/extract.py` to retrieve data from the listings table in the PostgreSQL database.
3. **transform:** Calls the `transform_data` function from `src/transform.py` to perform the necessary data transformations on the extracted data.
4. **load:** Calls the `load_data` function from `src/load.py` to load the transformed data into the `listings_transformed` table in the database.
5. **end:** Marks the successful completion of the ETL flow.

**Parameterization:**
The Metaflow flow includes a `table_name` parameter. This parameter allows you to customize the name of the table where the transformed data is loaded. By default, it's set to `listings_transformed`, but you can change it when running the flow using the `--table-name` flag.

**Error Handling and Logging:**
The flow incorporates error handling using try-except blocks within each step. If an error occurs during any step, a descriptive error message is logged using the `logging` module, and the flow is terminated to prevent further execution.

**Data Passing and Reusability:**
The flow utilizes the `self.df` attribute to pass the extracted and transformed data between steps efficiently. This promotes code reusability and avoids unnecessary recomputation of intermediate results.

**Reproducibility and Scalability:**
Metaflow automatically tracks code and data versions, ensuring reproducibility of your results. While the current implementation is sufficient for the given dataset, Metaflow's features like parallelization with `foreach` and integration with distributed computing frameworks like Apache Spark could be employed for scaling to larger datasets in the future.

**Execution:**
To run the Metaflow flow, make sure you have activated your virtual environment and execute the following command in your terminal:

```bash
python airbnb_flow.py run
```

## How to Run

### Data Ingestion (One-Time Setup):

**Create Tables (One-Time):**

1. Activate your virtual environment:

   ```bash
   source data_engineer_env/bin/activate
   ```

   (Use the appropriate command for your OS)

2. Run the `models.py` script:

   ```bash
   python models.py
   ```

   This will create the `listings` table in your PostgreSQL database if it doesn't already exist.

**Load Raw Data (One-Time):**

1. Make sure your virtual environment is activated.
2. Run the `load_data.py` script:

   ```bash
   python -m src.load_data
   ```

   This will load the raw data from the `AB_NYC_2019.csv` file into the `listings` table.

### Running the ETL Pipeline:

1. Activate your virtual environment.
2. Execute the Metaflow flow:

   ```bash
   python airbnb_flow.py run
   ```

   This will initiate the entire ETL process, performing the extraction, transformation, and loading steps.

**(Optional) Customize Output Table:**

To load the transformed data into a table with a different name than the default `listings_transformed`, use the `--table-name` parameter:

```bash
python airbnb_flow.py run --table-name <your_custom_table_name>
```
