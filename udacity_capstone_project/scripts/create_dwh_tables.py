#!/usr/bin/env python
# coding: utf-8

# **Import relevant libraries**

# In[ ]:


import psycopg2
from udacity_capstone_project import config


# **Create staging tables**

# In[ ]:


QUERIES = {}

# Create staging tables.
## Staging table for resale prices.
'''QUERIES["drop_tables"] = ("""
    DROP TABLE staging_resale_prices, staging_property_info, fact_prop_info, fact_resale_prices, dim_sold, dim_rental, dim_prop_features, dim_resale_features;
    """)
'''

QUERIES["staging_resale_prices"] = ("""
    CREATE TABLE IF NOT EXISTS staging_resale_prices
    (
        town VARCHAR,
        flat_type VARCHAR,
        flat_model VARCHAR,
        floor_area_sqm NUMERIC,
        street_name VARCHAR,
        resale_price NUMERIC,
        month VARCHAR,
        lease_commence_date INTEGER,
        storey_range VARCHAR,
        _id INTEGER,
        block VARCHAR,
        remaining_lease VARCHAR
        );
""")

## Staging table for property information.
QUERIES["staging_property_info"] = ("""
    CREATE TABLE IF NOT EXISTS staging_property_info
    (
        year_completed INTEGER,
        multigen_sold SMALLINT,
        bldg_contract_town VARCHAR,
        multistorey_carpark VARCHAR,
        street VARCHAR,
        total_dwelling_units SMALLINT,
        blk_no VARCHAR,
        exec_sold SMALLINT,
        max_floor_lvl SMALLINT,
        residential VARCHAR,
        one_room_sold SMALLINT,
        precinct_pavilion VARCHAR,
        other_room_rental SMALLINT,
        five_room_sold SMALLINT,
        three_room_sold SMALLINT,
        commercial VARCHAR,
        four_room_sold SMALLINT,
        miscellaneous VARCHAR,
        studio_apartment_sold SMALLINT,
        two_room_rental SMALLINT,
        two_room_sold SMALLINT,
        one_room_rental SMALLINT,
        three_room_rental SMALLINT,
        market_hawker VARCHAR,
        _id INTEGER
        );
""")


# **Create fact and dimensions tables for `HDB Property Information` dataset**

# In[ ]:


## Fact table for `HDB Property Information` dataset
QUERIES ["fact_prop_info"] = ("""
    CREATE TABLE IF NOT EXISTS fact_prop_info
    (
        _id INTEGER PRIMARY KEY,
        street VARCHAR NOT NULL,
        blk_no VARCHAR NOT NULL
    );
""")

## Dimension tables for `HDB Resale Prices` dataset
QUERIES ["dim_sold"] = ("""
    CREATE TABLE IF NOT EXISTS dim_sold
    (
        _id INTEGER PRIMARY KEY,
        multigen_sold SMALLINT NOT NULL,
        exec_sold SMALLINT NOT NULL,
        one_room_sold SMALLINT NOT NULL,
        two_room_sold SMALLINT NOT NULL,
        three_room_sold SMALLINT NOT NULL,
        four_room_sold SMALLINT NOT NULL,
        five_room_sold SMALLINT NOT NULL,
        studio_apartment_sold SMALLINT NOT NULL
    );
""")

QUERIES ["dim_rental"] = ("""
    CREATE TABLE IF NOT EXISTS dim_rental
    (
        _id INTEGER PRIMARY KEY,
        one_room_rental SMALLINT NOT NULL,
        two_room_rental SMALLINT NOT NULL,
        three_room_rental SMALLINT NOT NULL,
        other_room_rental SMALLINT NOT NULL
    );
""")

QUERIES ["dim_prop_features"] = ("""
    CREATE TABLE IF NOT EXISTS dim_prop_features
    (
        _id INTEGER PRIMARY KEY,
        year_completed INTEGER NOT NULL,
        bldg_contract_town VARCHAR NOT NULL,
        multistorey_carpark VARCHAR NOT NULL,
        total_dwelling_units SMALLINT NOT NULL,
        max_floor_lvl SMALLINT NOT NULL,
        residential VARCHAR NOT NULL,
        precinct_pavilion VARCHAR NOT NULL,
        commercial VARCHAR NOT NULL,
        miscellaneous VARCHAR NOT NULL,
        market_hawker VARCHAR NOT NULL
    );
""")


# **Create fact and dimensions tables for `HDB Resale Prices` dataset**

# In[ ]:


## Fact table for `HDB Resale Prices` dataset
QUERIES ["fact_resale_prices"] = ("""
    CREATE TABLE IF NOT EXISTS fact_resale_prices
    (
        _id INTEGER NOT NULL,
        street_name VARCHAR NOT NULL,
        block VARCHAR NOT NULL,
        month VARCHAR NOT NULL,
        resale_price NUMERIC NOT NULL,
        PRIMARY KEY(_id, street_name, block, month, resale_price)
    );
""")

## Dimension tables for `HDB Resale Prices` dataset
QUERIES ["dim_resale_features"] = ("""
    CREATE TABLE IF NOT EXISTS dim_resale_features
    (
        _id INTEGER NOT NULL,
        flat_type VARCHAR NOT NULL,
        flat_model VARCHAR NOT NULL,
        floor_area_sqm NUMERIC NOT NULL,
        lease_commence_date INTEGER NOT NULL,
        storey_range VARCHAR NOT NULL,
        remaining_lease VARCHAR,
        PRIMARY KEY(_id, flat_type, flat_model, floor_area_sqm, lease_commence_date, storey_range, remaining_lease)
    );
""")


# **Python `main` idiom**

# In[ ]:


def main():
    conn_string = config.POSTGRES['CONNECTION_STRING']
    conn = psycopg2.connect(conn_string)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    for key, query in QUERIES.items():
        cur.execute(query)
        print('test')

    conn.close()

if __name__ == "__main__":
    main()
