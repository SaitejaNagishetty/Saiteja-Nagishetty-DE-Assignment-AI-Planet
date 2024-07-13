import logging
from metaflow import FlowSpec, step, Parameter
from sqlalchemy import create_engine, exc

# Import functions from other modules
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data

# Import configuration settings
import config

# Configure logging to a file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("airbnb_etl.log"),  # Log to a file
        logging.StreamHandler(),  # Log to console as well (optional)
    ],
)


class AirbnbETLFlow(FlowSpec):
    """
    A Metaflow flow to orchestrate the ETL process for Airbnb data.

    This flow extracts data from the 'listings' table in the PostgreSQL database,
    performs the following transformations:

    - Handles missing values in 'reviews_per_month', 'name', and 'price' columns.
    - Calculates new features:
        - 'is_superhost': Indicates if a host is a superhost.
        - 'is_longterm': Indicates if a listing requires a minimum stay of more than 7 days.
        - 'last_review_year', 'last_review_month', and 'last_review_day': Extracts date components from 'last_review'.
        - 'price_per_person': Calculates the price per person based on room type.
        - 'avg_price_per_neighbourhood': Calculates the average price per neighborhood group.
    - Removes unnecessary columns: 'host_name', 'last_review'.

    Finally, it loads the transformed data into the 'listings_transformed' table.
    """

    table_name = Parameter(
        "table_name",
        help="Name of the table to load transformed data into.",
        default="listings_transformed",
    )

    @step
    def start(self):
        """Start the flow and initialize variables."""
        logging.info("Starting Airbnb ETL flow...")

        # Store database connection URL
        self.database_url = config.DATABASE_URL

        self.next(self.extract)

    @step
    def extract(self):
        """Extract data from the database."""
        try:
            logging.info("Extracting data from the 'listings' table...")
            engine = create_engine(self.database_url)
            self.df = extract_data(engine)
        except exc.OperationalError as e:  # Catch connection errors
            logging.error("Database connection error: %s", e)
            raise
        except Exception as e:  # Catch all other exceptions
            logging.error(f"Data extraction failed: {e}")
            raise
        self.next(self.transform)

    @step
    def transform(self):
        """Transform the extracted data."""
        try:
            logging.info("Transforming data...")
            self.df = transform_data(self.df)
        except Exception as e:
            logging.error(f"Data transformation failed: {e}")
            raise
        self.next(self.load)

    @step
    def load(self):
        """Load the transformed data into the database."""
        try:
            logging.info(f"Loading data into {self.table_name}...")
            engine = create_engine(self.database_url)  # Recreate engine
            load_data(self.df, self.table_name, engine)
        except Exception as e:
            logging.error(f"Data loading failed: {e}")
            raise
        self.next(self.end)

    @step
    def end(self):
        """End the flow and log a summary."""
        logging.info("Airbnb ETL flow completed successfully!")


if __name__ == "__main__":
    AirbnbETLFlow()
