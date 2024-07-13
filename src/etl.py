import os
import logging
from sqlalchemy import create_engine


# Importing ETL functions
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data

# Importing configuration settings
import config

# Configuring logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """
    Executes the ETL (Extract, Transform, Load) pipeline for the Airbnb NYC dataset.

    The pipeline performs the following steps:
        1. Extracts data from the 'listings' table in the PostgreSQL database.
        2. Transforms the data by handling missing values, calculating metrics, and engineering features.
        3. Loads the transformed data into the 'listings_transformed' table.
    """

    try:
        logging.info("Starting ETL pipeline...")

        # Creating database engine
        engine = create_engine(config.DATABASE_URL)

        # Extracting data
        logging.info("Extracting data...")
        df = extract_data(engine)

        # Transforming data
        if df is not None:
            logging.info("Transforming data...")
            df_transformed = transform_data(df)
        else:
            raise ValueError("Data extraction failed.")

        # Loading data
        if df_transformed is not None:
            logging.info("Loading data into %s...", config.TRANSFORMED_TABLE_NAME)
            load_data(df_transformed, config.TRANSFORMED_TABLE_NAME, engine)
        else:
            raise ValueError("Data transformation failed.")

        logging.info("ETL pipeline completed successfully!")

    except Exception as e:
        logging.error("ETL pipeline failed: %s", e)


if __name__ == "__main__":
    main()
