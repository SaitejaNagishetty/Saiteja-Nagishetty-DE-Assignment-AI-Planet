# import pandas as pd
# from sqlalchemy import create_engine
# from sqlalchemy.orm import Session


# def load_data(df: pd.DataFrame, table_name: str, engine) -> None:
#     """Loads transformed data into a specified table in the PostgreSQL database.

#     Args:
#         df: DataFrame containing the transformed data.
#         table_name: Name of the table to load data into.
#         engine: SQLAlchemy database engine object.
#     """
#     try:
#         # Using a session to ensure data integrity and to avoid data duplication
#         with Session(engine) as session:
#             # Check if the table exists and drop it if it does to avoid data duplication
#             if engine.dialect.has_table(engine.connect(), table_name):
#                 print(f"Dropping existing table: {table_name}")
#                 session.execute(f"DROP TABLE {table_name}")
#                 session.commit()

#             # Create the new table
#             df.to_sql(table_name, session.connection(), index=False)
#             session.commit()
#         print(f"Data successfully loaded into {table_name}")
#     except Exception as e:
#         print(f"Error loading data into {table_name}: {e}")


import pandas as pd
from sqlalchemy import create_engine, exc, MetaData
from sqlalchemy.orm import Session

import config
import logging

# Configure logging
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

        # Load the transformed data using a session and commit the transaction
        with Session(engine) as session:
            # Removed extra logging message
            df.to_sql(table_name, session.connection(), index=False)
            session.commit()

        logging.info(f"Data successfully loaded into {table_name}")
    except exc.SQLAlchemyError as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        raise
