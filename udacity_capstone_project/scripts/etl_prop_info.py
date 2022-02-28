#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import requests
import psycopg2
import pandas as pd
import math
from udacity_capstone_project import config


# In[ ]:


def api_to_csv():
    '''
    Function to get records from Data.gov.sg API for the HDB resale prices and HDB property info datasets;
    May not generalise to other datasets from Data.gov.sg - to test.
    Max records extracted each time is 1000, hence need to loop.
    This automatically extracts 1000 records each time and loops over all records.

    Returns:
    - Dataframe with records.

    '''

    resource_id = '482bfa14-2977-4035-9c61-c85f871daf4e'

    ## Initialise dataframe
    dataset_df = pd.DataFrame()

    ## Create URL variable
    url = 'https://data.gov.sg/api/action/datastore_search'

    ## First pass to get the first 1000 records and also the 'total' variable to decide how many times to loop.
    params = {'resource_id':resource_id, 'limit':1000}
    r = requests.get(url, params=params)
    temp_json = r.json()
    total = temp_json['result']['total']
    no_of_pages = math.floor(total / 1000)

    ## Second pass to get all the remaining records by looping through the pages.
    for page in range(no_of_pages+1):
        offset = int(1000 * (page))
        print("Retrieving approximately {} records out of {}.".format(offset,total), end = '\r')
        params = {'offset': offset, 'resource_id':resource_id, 'limit':1000} # list_resale_p_resource_ids[i]
        r = requests.get(url, params = params)
        temp_json = r.json()
        temp_df = pd.DataFrame.from_dict(temp_json['result']['records'])
        dataset_df = dataset_df.append(temp_df)

    ## Reset index.
    dataset_df = dataset_df.reset_index(drop=True)
    dataset_df.rename(columns =
                   {
                       "1room_sold": "one_room_sold",
                       "5room_sold": "five_room_sold",
                       "3room_sold": "three_room_sold",
                       "4room_sold": "four_room_sold",
                       "2room_rental": "two_room_rental",
                       "2room_sold": "two_room_sold",
                       "1room_rental": "one_room_rental",
                       "3room_rental": "three_room_rental"
                    }, inplace=True)
    dataset_df.to_csv('prop_info.csv', index=False)


# In[ ]:


def csv_to_dwh():
    conn_string = config.POSTGRES["CONNECTION_STRING"]
    conn = psycopg2.connect(conn_string)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    file = config.ETL_PROP_INFO["PROP_INFO_CSV"]

    # Copy csv into staging tables as created by the 'create_dwh_tables' script.
    with open(file, "r") as f:
        f.readline() # skips header
        cur.execute("TRUNCATE TABLE staging_property_info")
        cur.copy_from(f, "staging_property_info", sep = ",")

    # Insert into fact table
    cur.execute("""
        INSERT INTO fact_prop_info (
            _id,
            street,
            blk_no
        )
        SELECT
            _id,
            street,
            blk_no
        FROM staging_property_info
        ON CONFLICT (_id)
        DO NOTHING;
    """)

    # Insert into dimension tables
    cur.execute("""
        INSERT INTO dim_sold (
            _id,
            multigen_sold,
            exec_sold,
            one_room_sold,
            two_room_sold,
            three_room_sold,
            four_room_sold,
            five_room_sold,
            studio_apartment_sold
        )
        SELECT
            _id,
            multigen_sold,
            exec_sold,
            one_room_sold,
            two_room_sold,
            three_room_sold,
            four_room_sold,
            five_room_sold,
            studio_apartment_sold
        FROM staging_property_info
        ON CONFLICT (_id)
        DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO dim_rental (
            _id,
            one_room_rental,
            two_room_rental,
            three_room_rental,
            other_room_rental
        )
        SELECT
            _id,
            one_room_rental,
            two_room_rental,
            three_room_rental,
            other_room_rental
        FROM staging_property_info
        ON CONFLICT (_id)
        DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO dim_prop_features (
            _id,
            year_completed,
            bldg_contract_town,
            multistorey_carpark,
            total_dwelling_units,
            max_floor_lvl,
            residential,
            precinct_pavilion,
            commercial,
            miscellaneous,
            market_hawker
        )
        SELECT
            _id,
            year_completed,
            bldg_contract_town,
            multistorey_carpark,
            total_dwelling_units,
            max_floor_lvl,
            residential,
            precinct_pavilion,
            commercial,
            miscellaneous,
            market_hawker
        FROM staging_property_info
        ON CONFLICT (_id)
        DO NOTHING;
    """)
    conn.close()

    #Need to delete file? If so, uncomment the below.
    #if os.path.isfile(file): os.remove(file)


# In[ ]:


def check_rows_inserted():
    """
    This checks if any rows have been added into the staging table in the data warehouse that was created from the CSV.
    """

    conn = psycopg2.connect(config.POSTGRES["CONNECTION_STRING"])
    cur = conn.cursor()
    cur.execute(f"""
        SELECT COUNT(*)
        FROM staging_property_info
    """)
    result = cur.fetchone()[0]

    if result == 0:
        print("WARNING: no rows inserted. Check that CSV is not empty.")

    conn.close()
