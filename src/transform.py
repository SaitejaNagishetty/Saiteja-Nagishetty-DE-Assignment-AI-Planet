# import pandas as pd
# from pandas.tseries.offsets import DateOffset
# import numpy as np


# def transform_data(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Transforms the extracted Airbnb data by handling missing values, converting data types,
#     and calculating additional metrics.

#     Args:
#         df: The DataFrame containing the extracted data.

#     Returns:
#         pd.DataFrame: The transformed DataFrame.
#     """
#     try:
#         # 1. Convert Columns to Correct Datatypes
#         df["last_review"] = pd.to_datetime(df["last_review"], errors="coerce")

#         # 2. Handle Missing Values
#         df["reviews_per_month"] = df["reviews_per_month"].fillna(0)
#         df.dropna(subset=["price", "name"], inplace=True)

#         # 3. Feature Engineering
#         df["host_name"] = df["host_name"].astype(str)
#         df["is_superhost"] = df["host_name"].apply(
#             lambda x: 1 if "superhost" in x.lower() else 0
#         )
#         df["is_longterm"] = df["minimum_nights"].apply(lambda x: 1 if x > 7 else 0)
#         df["last_review_year"] = df["last_review"].dt.year
#         df["last_review_month"] = df["last_review"].dt.month
#         df["last_review_day"] = df["last_review"].dt.day

#         # 4. Calculate Additional Metrics
#         df["price_per_person"] = np.where(
#             df["room_type"] == "Private room", df["price"] / 2, df["price"]
#         )

#         # Fix the error in this line
#         avg_price_by_neighborhood = (
#             df.groupby("neighbourhood_group")["price"]
#             .mean()
#             .reset_index()
#             .rename(columns={"price": "avg_price_per_neighbourhood"})
#         )

#         df = df.merge(avg_price_by_neighborhood, on="neighbourhood_group", how="left")

#         # 5. Remove unnecessary columns
#         df = df.drop("host_name", axis=1)
#         df = df.drop("last_review", axis=1)

#         return df

#     except Exception as e:
#         print(f"Error transforming data: {e}")
#         return None


import pandas as pd
import numpy as np


def transform_data(airbnb_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the extracted Airbnb data by handling missing values, converting data types,
    and calculating additional metrics.

    Args:
        airbnb_df: The DataFrame containing the extracted Airbnb data.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    try:
        # 1. Convert Columns to Correct Datatypes
        airbnb_df["last_review"] = pd.to_datetime(
            airbnb_df["last_review"], errors="coerce"
        )

        # 2. Handle Missing Values
        airbnb_df["reviews_per_month"] = airbnb_df["reviews_per_month"].fillna(0)
        airbnb_df.dropna(subset=["price", "name"], inplace=True)

        # 3. Feature Engineering
        # Convert host name to string and handle potential missing values
        airbnb_df["host_name"] = airbnb_df["host_name"].fillna("").astype(str)
        airbnb_df["is_superhost"] = airbnb_df["host_name"].apply(
            lambda x: 1 if "superhost" in x.lower() else 0
        )
        airbnb_df["is_longterm"] = airbnb_df["minimum_nights"].apply(
            lambda x: 1 if x > 7 else 0
        )
        airbnb_df["last_review_year"] = airbnb_df["last_review"].dt.year
        airbnb_df["last_review_month"] = airbnb_df["last_review"].dt.month
        airbnb_df["last_review_day"] = airbnb_df["last_review"].dt.day

        # 4. Calculate Additional Metrics
        airbnb_df["price_per_person"] = np.where(
            airbnb_df["room_type"] == "Private room",
            airbnb_df["price"] / 2,
            airbnb_df["price"],
        )

        avg_price_by_neighborhood = (
            airbnb_df.groupby("neighbourhood_group")["price"]
            .mean()
            .reset_index()
            .rename(columns={"price": "avg_price_per_neighbourhood"})
        )
        airbnb_df = airbnb_df.merge(
            avg_price_by_neighborhood, on="neighbourhood_group", how="left"
        )

        # 5. Remove unnecessary columns
        airbnb_df = airbnb_df.drop("host_name", axis=1)
        airbnb_df = airbnb_df.drop("last_review", axis=1)

        return airbnb_df

    except Exception as e:
        print(f"Error transforming data: {e}")
        return None
