import os
from datetime import datetime


#NOTE: Refer to the following site for various ways for formatting the connection string - 'https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING'

START = {
    "START_DATE": datetime(2021,8,7), # 7th August 2021
}

POSTGRES = {
    "CONNECTION_STRING": "postgresql://postgres:password@172.28.240.1/udacity_capstone_project"
}


ETL_PROP_INFO = {
    "PROP_INFO_CSV":"prop_info.csv"
}



ETL_RESALE_PRICES = {
    "RESALE_PRICES_CSV":"resale_prices.csv"
}
