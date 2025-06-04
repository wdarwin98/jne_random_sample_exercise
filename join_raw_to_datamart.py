import pandas as pd

# Load raw tables
df_packages = pd.read_csv("Raw Data/raw_packages.csv")
df_locations = pd.read_csv("Raw Data/raw_locations.csv")
df_customers = pd.read_csv("Raw Data/raw_customer_info.csv")
df_metadata = pd.read_csv("Raw Data/raw_package_meta.csv")

# Join everything on tracking_number
df_joined = df_packages \
    .merge(df_locations, on="tracking_number", how="left") \
    .merge(df_customers, on="tracking_number", how="left") \
    .merge(df_metadata, on="tracking_number", how="left")

# Save clean unified dataset
df_joined.to_csv("raw_data_mart.csv", index=False)