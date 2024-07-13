import pandas as pd
from sqlalchemy import create_engine, exc, MetaData
from sqlalchemy.orm import Session

import config
import logging

# Configuring logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_data(df: pd.DataFrame, table_name: str, engine) -> None:
    """Loads transformed data into a specified table in the PostgreSQL database.

    Args:
        df: DataFrame containing the transformed data.
        table_name: Name of the table to load data into.
        engine: SQLAlchemy database engine object.
    """
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)

        # Checking for and dropping table is only done once outside 'with' block
        if table_name in metadata.tables:
            logging.info(f"Dropping existing table: {table_name}")
            with Session(engine) as session:
                session.execute(f"DROP TABLE IF EXISTS {table_name}")
                session.commit()

        # Loading the transformed data using a session and commit the transaction
        with Session(engine) as session:
            # Removed extra logging message
            df.to_sql(table_name, session.connection(), index=False)
            session.commit()

        logging.info(f"Data successfully loaded into {table_name}")
    except exc.SQLAlchemyError as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        raise
