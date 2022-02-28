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
    # HDB Resale Prices dataset resource ids - in order from part 1 to part 5
    list_resale_p_resource_ids = ['adbbddd3-30e2-445f-a123-29bee150a6fe',
                              '8c00bf08-9124-479e-aeca-7cc411d884c4',
                              '83b2fc37-ce8c-4df4-968b-370fd818138b',
                              '1b702208-44bf-4829-b620-4615ee19b57c',
                              'f1765b54-a209-4718-8d38-a39237f502b3']


    overall_resale_prices_df = pd.DataFrame()
    for resource_id in list_resale_p_resource_ids:
        print('Running resource_id: {}'.format(resource_id))

        ## Initialise dataframe
        dataset_df = pd.DataFrame()

        ## Create URL variable
        url = 'https://data.gov.sg/api/action/datastore_search'

        ## First pass to get the first 1000 records and also the 'total' variable to decide how many times to loop.
        params = {'resource_id':resource_id, 'limit':1000}
        r = requests.get(url, params=params)
        print('requests.get code = {}'.format(r))
        try:
            temp_json = r.json()
            total = temp_json['result']['total']
            no_of_pages = math.floor(total / 1000)

            ## Second pass to get all the remaining records by looping through the pages.
            for page in range(no_of_pages+1):
                offset = int(1000 * (page))
                print("                                                                                              ", end = '\r')
                print("Retrieving approximately {} records out of {}.".format(offset,total), end = '\r')
                params = {'offset': offset, 'resource_id':resource_id, 'limit':1000} # list_resale_p_resource_ids[i]
                r = requests.get(url, params = params)
                temp_json = r.json()
                temp_df = pd.DataFrame.from_dict(temp_json['result']['records'])
                dataset_df = dataset_df.append(temp_df)

            ## Reset index.
            dataset_df = dataset_df.reset_index(drop=True)
            overall_resale_prices_df = overall_resale_prices_df.append(dataset_df)
        except ValueError as e:
            print("Error: {}".format(e))
            print("HTTP response from requests: {}".format(r))
            print("Dataframe not created for resource_id - {}".format(resource_id))

    overall_resale_prices_df = overall_resale_prices_df.reset_index(drop=True)
    overall_resale_prices_df.to_csv('resale_prices.csv', index=False)


# In[ ]:


def csv_to_dwh():
    conn_string = config.POSTGRES["CONNECTION_STRING"]
    conn = psycopg2.connect(conn_string)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    file = config.ETL_RESALE_PRICES["RESALE_PRICES_CSV"]

    # Copy csv into staging tables as created by the 'create_dwh_tables' script.
    with open(file, "r") as f:
        f.readline() # skips header
        cur.execute("TRUNCATE TABLE staging_resale_prices")
        cur.copy_from(f, "staging_resale_prices", sep = ",")

    # Insert into fact table
    cur.execute("""
        INSERT INTO fact_resale_prices (
            _id,
            street_name,
            block,
            month,
            resale_price
        )
        SELECT
            _id,
            street_name,
            block,
            month,
            resale_price
        FROM staging_resale_prices
        ON CONFLICT (_id, street_name, block, month, resale_price)
        DO NOTHING;
    """)

    # Insert into dimension table
    cur.execute("""
        INSERT INTO dim_resale_features (
            _id,
            flat_type,
            flat_model,
            floor_area_sqm,
            lease_commence_date,
            storey_range,
            remaining_lease
        )
        SELECT
            _id,
            flat_type,
            flat_model,
            floor_area_sqm,
            lease_commence_date,
            storey_range,
            remaining_lease
        FROM staging_resale_prices
        ON CONFLICT (_id, flat_type, flat_model, floor_area_sqm, lease_commence_date, storey_range, remaining_lease)
        DO NOTHING;
    """)

    conn.close()


# In[ ]:


def check_rows_inserted():
    """
    This checks if any rows have been added into the staging table in the data warehouse that was created from the CSV.
    """

    conn = psycopg2.connect(config.POSTGRES["CONNECTION_STRING"])
    cur = conn.cursor()
    cur.execute(f"""
        SELECT COUNT(*)
        FROM staging_resale_prices
    """)
    result = cur.fetchone()[0]

    if result == 0:
        print("WARNING: no rows inserted. Check that CSV is not empty.")
    else:
        print("CSV is not empty.")
    conn.close()
