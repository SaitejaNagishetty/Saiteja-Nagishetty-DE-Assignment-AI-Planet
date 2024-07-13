# import os
# import pandas as pd
# from sqlalchemy import create_engine, text
# from sqlalchemy.orm import sessionmaker


# def extract_data(engine):
#     """
#     Extracts all data from the 'listings' table in the PostgreSQL database.

#     Args:
#         engine: SQLAlchemy database engine object.

#     Returns:
#         pd.DataFrame: The extracted data as a Pandas DataFrame.
#     """
#     try:
#         # Create a session for interacting with the database
#         Session = sessionmaker(bind=engine)
#         with Session() as session:
#             # Construct and execute the query to fetch all data
#             query = text("SELECT * FROM listings")
#             result = session.execute(query)

#             # Convert result to DataFrame
#             df = pd.DataFrame(result.fetchall(), columns=result.keys())
#             return df

#     except Exception as e:
#         print(f"Error extracting data: {e}")
#         return None


import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def extract_data(engine, chunksize=1000):  # Added chunksize for potential optimization
    """
    Extracts data from the 'listings' table in the PostgreSQL database.

    Args:
        engine: SQLAlchemy database engine object.
        chunksize: Number of rows to fetch per chunk (optional, default: 1000).

    Returns:
        pd.DataFrame: The extracted data as a Pandas DataFrame.
    """
    try:
        Session = sessionmaker(bind=engine)

        # Initialize an empty dataframe to accumulate the chunks
        df_final = pd.DataFrame()

        # Execute the query and get results in chunks
        with Session() as session:
            query = text("SELECT * FROM listings")
            result = session.execute(query)
            while True:
                chunk = result.fetchmany(chunksize)
                if not chunk:
                    break
                df_chunk = pd.DataFrame(chunk, columns=result.keys())
                df_final = pd.concat(
                    [df_final, df_chunk], ignore_index=True
                )  # Add a new chunk to the dataframe

        # Commit the transaction if the database doesn't auto-commit
        session.commit()
        return df_final

    except Exception as e:
        if isinstance(e, sqlalchemy.exc.OperationalError):  # Specific error handling
            print(f"Database connection error: {e}")
        elif isinstance(e, sqlalchemy.exc.ProgrammingError):
            print(f"SQL query error: {e}")
        else:
            print(f"Error extracting data: {e}")
        return None
