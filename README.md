# Geocoded National Address File (G-NAF) Importer

Imports a G-NAF extract into a new PostgreSQL database running in Docker, and creates appropriate indexes for fast querying of the data.

The script uses as much of the data provided in a G-NAF release as possible, so it should remain compatible with future data releases. The script is written in Python 3 and requires the module Psycopg (Python-PostgreSQL Database Adapter).

The G-NAF dataset includes over 13 million Australian address records, and is provided by Australia's federal, state, and territory governments under a Creative Commons Attribution 4.0 International licence (CC BY 4.0).

You can adapt this script to use an existing PostgreSQL installation if you prefer.

## Usage

1. Clone this repository.
1. Download a copy of the latest G-NAF dataset from [data.gov.au](https://data.gov.au/dataset/geocoded-national-address-file-g-naf).
1. Extract the G-NAF folder inside the archive to the same directory as this repository (such that you have a top level directory called "G-NAF").
1. Create a new PostgreSQL container with Docker:
    ```
    docker run --name gnaf -d -e POSTGRESQL_PASSWORD=postgres -p 5432:5432 bitnami/postgresql:10.0
    ```
1. Install Psycopg
    ```
    pip3 install psycopg2-binary
    ```
1. Run the script to import the data:
    ```
    python3 import_gnaf.py
    ```

The import should take less than 30 minutes to complete on most hardware, and should leave you with a container of around 16GB.
