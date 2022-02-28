# Udacity Data Engineering Nanodegree Capstone Project

For this capstone project, it was decided to build a data warehouse to store Singapore's Housing Development Board (HDB) public housing resale flat data from 1990 to the present day as of the time of creation of this README (Dec 2021). This data is available via API from https://data.gov.sg, and was extracted using Python and loaded into the PostgreSQL database. The entire process is programmatically scheduled and monitored using Apache Airflow, with the Directed Acyclic Graphs (DAGs) set to run once a month.

# Overview
## Scope
The data warehouse consists of data pulled from 2 data sources:
- HDB Resale Flat Prices - Resale transacted prices from 1990 to 2021 and includes features like resale price, address, size, lease commencement date etc. This currently consists of 5 separate resource IDs which need to be separately requested from the API.
- HDB Property Information - Includes location of existing HDB blocks, highest floor level, year of completion, type of building and number of HDB flats (breakdown by flat type) per block etc.

The detailed breakdown of available fields will be stipulated later in this document. The data warehouse is modelled using a Star Schema, with Facts and Dimensions tables. This data warehouse could be useful for potential property buyers looking for the latest market data and traits of the properties they are interested in or for performing analytics.

Scale of project (rounded up to nearest ten thousand):
* Rows in HDB Resale Flat Prices: 980,000
* Rows in HDB Property Information: 20,000

## Steps
**Data.gov.sg API** This is an API for public datasets as provided by GovTech - a Singapore statutory board. The API takes in a resource ID parameter and returns a response with the resale flat data and property information as briefly described above.

**API --> CSV** The API response is in JSON format. This was converted into pandas DataFrames which were then converted into CSV and saved onto a temporary local directory using built-in functions in the pandas Python library.

**Data Quality Checks** A check is performed to check that the insertion of data into the facts and dimensions tables is not zero. This will identify any issues with the extraction of data from the API and will throw up an error.

Another check in the form of constraints when inserting and updating the tables like 'NOT NULL' help to ensure data quality. The 'ON CONFLICT' clause allows the developer to decide what to do with the data in cases where there are conflicts.

## Considerations for Scaling Up
### If data was increased by 100x
Currently all the datasets are re-extracted each time the DAG is run, which might lead to slow runtimes.

For the HDB Resale Flat Prices dataset, a solution for this would be to just run the DAG on the latest resource ID (which is updated periodically by GovTech) as it is likely that this will be the only dataset updated and partition my tables based on the resource ID (thus having 5 tables for the 5 resource IDs). For the other 4 older datasets, a check may be performed on the metadata (e.g. date dataset was updated) to determine if there is a need to re-extract the data. This will help to make 'INSERT' functions more manageable although querying these data would require more 'JOIN's and hence be more troublesome.

For the HDB Property Information dataset, there is no need for this as the dataset will still be relatively small (20,000 rows x 100 = 2,000,000 rows) and it is not expected to run into performance issues nor PostgreSQL database size issues.

### If pipelines were run daily at 7am
Currently the pipelines are run monthly given that the frequency of update by GovTech is low. If the DAGs were run daily, a first check would be done on metadata for the datasets to first check if there have been any changes to the datasets. If no changes, there would be no need to re-extract the data for the two datasets (HDB Resale Flat Prices and HDB Property Information). This will be a good check to perform to reduce unnecessary runtime.

### If 100+ people need to access the database
There should not be a problem with reading and querying data given the current setup. Perhaps one way to optimise reading of data is to further granularise and generate OLAP tables that are subsets of the entire dataset to better cater to the needs of the users and also increase read speed. However, it is not foreseen that the reading will be an issue given that the size of the datasets is still manageable. If performance will be an issue, another solution might be to give priority to certain groups of users with 'operation-critical' functions to be able to access at critical busy periods.

## Technologies used
### Python (pandas, psycopg2, requests)
Python was used as it provides many useful libraries for scripting and interacting with various other technologies.

`pandas` was used for data manipulation because it was built for that purpose and has many functions that are useful for that - e.g. it has built-in functions to convert a DataFrame into CSV.

`psycopg2` was used for writing SQL queries and interacting with the PostgreSQL database. It allows one to nest SQL queries without changing the syntax.

`requests` was a useful Python library for making requests to the API and handling the response as well.

### PostgreSQL
This is an SQL database which was chosen for use as the data warehouse. The availability of constraints like 'NOT NULL' and clauses like 'ON CONFLICT' help to ensure data quality. PostgreSQL is also able to automatically ensure that the above constraints and clauses are met. It also has a very handy user interface in the form of `pgadmin4` which allows the developer to easily access the database. PostgreSQL is also very commonly used and is open-source, which means there would be regular updates and would most likely remain free.

### Apache Airflow
This was taught in the Nanodegree, and is very useful for scheduling pipeline runs - especially if these are frequent. It has a very user-friendly user interface as well, and supports backfilling and scheduling of pipeline runs. This is perfect for keeping this data warehouse updated without too much manual intervention.

## Data Model (OR Data Dictionary)
### HDB Property Information Data
**Fact Table**
* `fact_prop_info`
    * Contains each property's address (street and block number)
    * Columns: `_id`, `street`, `blk_no`
    * PRIMARY KEY constraint: `_id`

|   Field Name  | Data Type | Description                            | Example     |
|:-------------:|:---------:|:--------------------------------------:|:-----------:|
| _id           | INTEGER   | Primary Key                            |  1          |
| street        | VARCHAR   | Name of street property is located on  |  BEACH ROAD |
| blk_no        | VARCHAR   | Block number of Property               |  1          |


**Dimension Table**
* `dim_sold`
    * Contains property information regarding units sold.
    * Columns: `_id`, `multigen_sold`, `exec_sold`,`1room_sold`, `2room_sold`, `3room_sold`, `4room_sold`, `5room_sold`, `studio_apartment_sold`
    * PRIMARY KEY constraint: `_id`
|   Field Name          | Data Type | Description                            | Example     |
|:---------------------:|:---------:|:--------------------------------------:|:-----------:|
| _id                   | INTEGER   | Primary Key                            |  1          |
| multigen_sold         | SMALLINT  | Number of multi-generation sold flats  |  12         |
| exec_sold             | SMALLINT  | Number of executive sold flats         |  50         |
| 1room_sold            | SMALLINT  | Number of 1-room sold flats            |  0          |
| 2room_sold            | SMALLINT  | Number of 2-room sold flats            |  255        |
| 3room_sold            | SMALLINT  | Number of 3-room sold flats            |  312        |
| 4room_sold            | SMALLINT  | Number of 4-room sold flats            |  100        |
| 5room_sold            | SMALLINT  | Number of 5-room sold flats            |  40         |
| studio_apartment_sold | SMALLINT  | Number of studio apartment sold flats  |  40         |


* `dim_rental`
    * Contains property information regarding units rented.
    * Columns: `_id`, `1room_rental`, `2room_rental`,`3room_rental`, `other_room_rental`
    * PRIMARY KEY constraint: `_id`
|   Field Name          | Data Type | Description                            | Example     |
|:---------------------:|:---------:|:--------------------------------------:|:-----------:|
| _id                   | INTEGER   | Primary Key                            |  1          |
| 1room_rental          | SMALLINT  | Number of 1-room rental flats          |  40         |
| 2room_rental          | SMALLINT  | Number of 2-room rental flats          |  100        |
| 3room_rental          | SMALLINT  | Number of 3-room rental flats          |  50         |
| other_room_rental     | SMALLINT  | Number of 4-room rental flats          |  200        |

* `dim_prop_features`
    * Contains miscellaneous property features that are deemed of secondary usage.
    * Columns: `_id`, `year_completed`, `bldg_contract_town`,`multistorey_carpark`, `total_dwelling_units`, `max_floor_lvl`, `residential`, `precinct_pavilion`, `commercial`, `miscellaneous`, `market_hawker`
    * PRIMARY KEY constraint: `_id`
|   Field Name          | Data Type | Description                                                                                                                                     | Example     |
|:---------------------:|:---------:|:-----------------------------------------------------------------------------------------------------------------------------------------------:|:-----------:|
| _id                   | INTEGER   | Primary Key                                                                                                                                     |  1          |
| year_completed        | INTEGER   | Year property was completed                                                                                                                     |  1980       |
| bldg_contract_town    | VARCHAR   | Town (shortened) – see legend below for full town name                                                                                          |  KWN        |
| multistorey_carpark   | VARCHAR   | Indicates presence or absence of multistorey carparks                                                                                           |  Y          |
| total_dwelling_units  | SMALLINT  | Number of dwelling units                                                                                                                        |  100        |
| max_floor_lvl         | SMALLINT  | Maximum floor level of property                                                                                                                 |  25         |
| residential           | VARCHAR   | Indicates usage as residential property or not                                                                                                  |  Y          |
| precinct_pavilion     | VARCHAR   | Indicates presence or absence of precinct pavilion                                                                                              |  N          |
| commercial            | VARCHAR   | Indicates usage as commercial property or not                                                                                                   |  N          |
| miscellaneous         | VARCHAR   | Indicates presence or absence of miscellaneous features like admin office, childcare centre, education centre, Residents’ Committees centre etc.|  Y          |
| market_hawker         | VARCHAR   | Indicates presence or absence of market and hawker centres                                                                                      |  Y          |


### HDB Resale Flat Prices Data
**Fact Table**
* `fact_resale_prices`
    * Contains the address of each property sold and in what month each property was sold and for what price.
    * Columns: `_id`, `street_name`, `blk`, `month`, `resale_price`
    * PRIMARY KEY constraint: `_id`, `street_name`, `blk`, `month`, `resale_price`
    * NOTE: The primary key is a composite key because there is no particular field which can be a unique identifier for each entry.
|   Field Name          | Data Type | Description                                                     | Example          |
|:---------------------:|:---------:|:---------------------------------------------------------------:|:----------------:|
| _id                   | INTEGER   | Primary Key (composite)                                         |  1               |
| street_name           | VARCHAR   | Primary Key (composite), Name of street property is located on  | ANG MO KIO AVE 1 |
| blk                   | VARCHAR   | Primary Key (composite), Block number of property               |  309             |
| month                 | VARCHAR   | Primary Key (composite), Month and year of resale_price         |  1990-01         |
| resale_price          | INTEGER   | Primary Key (composite), Resale price of units                  |  48000           |


**Dimension Table**
* `dim_resale_features`
    * Contains miscellaneous property features that are deemed of secondary usage.
    * Columns: `_id`, `flat_type`, `flat_model`,`floor_area_sqm`, `lease_commence_date`, `storey_range`, `remaining_lease`
    * PRIMARY KEY constraint: `_id`, `flat_type`, `flat_model`,`floor_area_sqm`, `lease_commence_date`, `storey_range`, `remaining_lease`
    * NOTE: The primary key is a composite key because there is no particular field which can be a unique identifier for each entry.
|   Field Name        | Data Type | Description                                                                             | Example             |
|:-------------------:|:---------:|:---------------------------------------------------------------------------------------:|:-------------------:|
| _id                 | INTEGER   | Primary Key (composite)                                                                 |  1                  |
| flat_type           | VARCHAR   | Primary Key (composite), Type of flat i.e. 1 to 5-room or executive or multi-generation |  1 ROOM             |
| flat_model          | VARCHAR   | Primary Key (composite), Model of flat                                                  |  NEW GENERATION     |
| floor_area_sqm      | NUMERIC   | Primary Key (composite), Floor area of unit in sqm                                      |  31                 |
| lease_commence_date | INTEGER   | Primary Key (composite), Lease commencement date                                        |  1977               |
| storey_range        | VARCHAR   | Primary Key (composite), Storey range of unit resold                                    |  10 TO 12           |
| remaining_lease     | VARCHAR   | Primary Key (composite), Remaining lease as of time of resale                           |  75 years 02 months |


# Data Pipeline and Scripts
## Data Sources
The two datasets were pulled from https://data.gov.sg as maintained by GovTech (a statutory board under the Singapore Government). An API was provided to query the data given a particular resource ID. Resource IDs can be extracted by navigating to the dataset using the browser and clicking on 'Data API' which will open a new page - resource ID will be in the URL of this page. Previously, an Excel sheet was provided which contained all the datasets available on the website and also their resource IDs- however, this has since been taken down (as of time of writing), hence this interim measure to individually extract the resource IDs.

The endpoints are as below:
* For the HDB Resale Flat Prices dataset: https://data.gov.sg/api/action/datastore_search?resource_id=<SPECIFY RESOURCE ID HERE>&limit=<SPECIFY LIMIT HERE>
    * Resource IDs (1990 to Dec 2021):
        * 'adbbddd3-30e2-445f-a123-29bee150a6fe'
        * '8c00bf08-9124-479e-aeca-7cc411d884c4'
        * '83b2fc37-ce8c-4df4-968b-370fd818138b'
        * '1b702208-44bf-4829-b620-4615ee19b57c'
        * 'f1765b54-a209-4718-8d38-a39237f502b3'
* For the HDB Property Information dataset: https://data.gov.sg/api/action/datastore_search?resource_id=<SPECIFY RESOURCE ID HERE>&limit=<SPECIFY LIMIT HERE>
    * Resource ID:
        * '482bfa14-2977-4035-9c61-c85f871daf4e'

## Scripts - `~/scripts/...`
* `create_dwh_tables.py` - This is run first to create the tables for the data warehouse. It contains code to create the 8 tables as shown in the previous sections.
* `etl_prop_info.py` - This performs the ETL for the HDB property information.
    * `api_to_csv`: This function extracts the JSON from the API and converts it to CSV.
    * `csv_to_dwh`: This function loads the CSV from the previous step to the PostgreSQL database.
    * `check_rows_inserted`: This function does the data quality checks.
* `etl_resale_prices.py` - This performs the ETL for the HDB resale flat prices. It contains the same functions as `etl_prop_info.py`


## Data Pipeline (using Airflow DAGs) - `~/dags/...`
The DAGs makes use of the in-built PythonOperator library from Airflow to run the DAGs, and helps orchestrate the different functions in the ETL Python files in the `Scripts` section above in the order specified below.
* `dag_prop_info.py`: `prop_info_api_to_csv` >> `prop_info_csv_to_dwh` >> `prop_info_check_rows_inserted`
* `dag_resale_prices.py`: `resale_prices_api_to_csv` >> `resale_prices_csv_to_dwh` >> `resale_prices_check_rows_inserted`


**NOTE 1: `config.py` contains the connection string to the PostgreSQL database (see the POSTGRES environment variable defined). This contains the IP address for the WSL 2 VM which happens to change each time WSL is loaded - a peculiarity that should be noted of my setup which is run locally. I could not find a way to make it static nor capture the dynamic value, therefore there is a need to update this IP address each time WSL is launched.**

**NOTE 2: DAGs can also be triggered manually (note to self).**
