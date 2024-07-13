import pandas as pd
from sqlalchemy import create_engine
import logging
from config import DATABASE_URL

from models import AirbnbListing


def load_data(file_path, table_name, engine):
    """Loads data from a CSV file into a PostgreSQL table."""

    try:
        df = pd.read_csv(file_path)
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        logging.info(f"Data successfully loaded into {table_name}")
        print(
            f"Data loading complete! Check the '{table_name}' table in your database."
        )
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        raise


if __name__ == "__main__":
    # Using the database connection string from config.py
    engine = create_engine(DATABASE_URL)
    file_path = "data/AB_NYC_2019.csv"
    load_data(file_path, "listings", engine)
