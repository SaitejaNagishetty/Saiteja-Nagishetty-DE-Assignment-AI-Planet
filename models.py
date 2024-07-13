import os
import config
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base

# Created a base class for declarative models
Base = declarative_base()


# Defined the AirbnbListing model (maps to the 'listings' table in the database)
class AirbnbListing(Base):
    __tablename__ = "listings"

    id = Column(Integer, primary_key=True)  # Unique identifier for each listing
    name = Column(String(255))  # Name of the listing (limited to 255 characters)
    host_id = Column(
        Integer, index=True
    )  # ID of the host, indexed for faster joins/queries
    host_name = Column(String(100))  # Name of the host (limited to 100 characters)
    neighbourhood_group = Column(
        String(50), index=True
    )  # Borough or area of NYC (indexed)
    neighbourhood = Column(
        String(100), index=True
    )  # Neighborhood within the borough (indexed)
    latitude = Column(Float)  # Latitude coordinate of the listing
    longitude = Column(Float)  # Longitude coordinate of the listing
    room_type = Column(
        String(50), index=True
    )  # Type of room (e.g., 'Entire home/apt', 'Private room', 'Shared room') (indexed)
    price = Column(Integer)  # Price per night in USD (integer for simplicity)
    minimum_nights = Column(Integer)  # Minimum nights required for a stay
    number_of_reviews = Column(Integer)  # Number of reviews the listing has received
    last_review = Column(Date)  # Date of the last review
    reviews_per_month = Column(Float)  # Average number of reviews per month
    calculated_host_listings_count = Column(
        Integer
    )  # Total number of listings the host has
    availability_365 = Column(
        Integer
    )  # Number of days the listing is available within the next 365 days


# Create a database engine (connection to the database)
engine = create_engine(config.DATABASE_URL)

# Create the table in the database based on the model's definition (if it doesn't already exist)
Base.metadata.create_all(engine)
