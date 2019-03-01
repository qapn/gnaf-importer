# Geocoded National Address File (G-NAF) Importer

Imports an Australian G-NAF extract into a new PostgreSQL database running in Docker. It will transform the data into a readable single table with every address in Australia, including a formatted address column ready for querying/filtering with appropriate indexing. You can optionally skip this process and import the raw data directly into multiple tables.

The script uses as much of the data provided in a G-NAF release as possible, so it should remain compatible with future data releases. It's is written in Python 3 and requires the module Psycopg (Python-PostgreSQL Database Adapter).

The G-NAF dataset includes over 13 million Australian address records, and is provided by Australia's federal, state, and territory governments under a Creative Commons Attribution 4.0 International licence (CC BY 4.0).

## Usage

1. Clone this repository.
1. Download a copy of the latest G-NAF dataset from [data.gov.au](https://data.gov.au/dataset/geocoded-national-address-file-g-naf).
1. Extract the G-NAF folder inside the archive to the same directory as this repository (such that you have a top level directory called "G-NAF").
1. Create a new PostgreSQL container with Docker:
    ```
    docker run --name gnaf -d -e POSTGRESQL_PASSWORD=postgres -p 5432:5432 postgres:latest
    ```
1. Install Psycopg:
    ```
    pip3 install psycopg2-binary
    ```
1. Run the importer.

    Flat table import:
    ```
    python3 import_gnaf.py
    ```

    Raw import:
    ```
    python3 import_gnaf.py --raw
    ```

## FAQ

* **How long will it take to run? How large will the database be?**

    The table below should give you a rough idea of how long it will take to build the database, and how large the container volume will be. I ran the script on my ageing laptop (i5-3320M) so you will likely get better results.

    | Type | Time Taken | Final Size |
    | --- | --- | --- |
    | Flat table import | ~60 minutes | ~9GB |
    | Raw import | ~30 minutes | ~16GB |

* **Can I use this on macOS / Windows / Linux?**

    I have tested this on macOS Mojave and Ubuntu 18.04. I use GNU `sed` on one of the G-NAF source files to escape a backslash, this won't work out of the box on macOS (includes BSD `sed`) or Windows. You can either escape the backslash manually (it's in the `NSW_ADDRESS_SITE_psv.psv` file) and comment out that line of the importer, or macOS users can install GNU `sed` with Homebrew:
    ```
    brew install gnu-sed
    ```
    And add this to your .bashrc (or .zshrc etc.) file:
    ```
    # GNU sed
    PATH="/usr/local/opt/gnu-sed/libexec/gnubin:$PATH"
    ```

* **What does your flat address table/autocomplete column look like?**

    You'll get a flat table of every address in Australia - here's an example for Parramatta High School:
    
  | address_detail_pid | street_locality_pid | locality_pid | building_name          | lot_number_prefix | lot_number | lot_number_suffix | flat_type | flat_number_prefix | flat_number | flat_number_suffix | level_type | level_number_prefix | level_number | level_number_suffix | number_first_prefix | number_first | number_first_suffix | number_last_prefix | number_last | number_last_suffix | street_name   | street_class_code | street_class_type | street_type_code | street_suffix_code | street_suffix_type | locality_name | state_abbreviation | postcode | latitude     | longitude    | geocode_type      | confidence | alias_principal | primary_secondary | legal_parcel_id | date_created | autocomplete                                                           |
  | ------------------ | ------------------- | ------------ | ---------------------- | ----------------- | ---------- | ----------------- | --------- | ------------------ | ----------- | ------------------ | ---------- | ------------------- | ------------ | ------------------- | ------------------- | ------------ | ------------------- | ------------------ | ----------- | ------------------ | ------------- | ----------------- | ----------------- | ---------------- | ------------------ | ------------------ | ------------- | ------------------ | -------- | ------------ | ------------ | ----------------- | ---------- | --------------- | ----------------- | --------------- | ------------ | ---------------------------------------------------------------------- |
  | GANSW711122120     | NSW325502           | NSW3184      | PARRAMATTA HIGH SCHOOL |                   |            |                   |           |                    |             |                    |            |                     |              |                     |                     | 76           | A                   |                    |             |                    | GREAT WESTERN | C                 | CONFIRMED         | HIGHWAY          |                    |                    | PARRAMATTA    | NSW                | 2150     | -33.81790658 | 150.99606946 | PROPERTY CENTROID | 1          | P               |                   | 1//DP795042     | 2010-05-02   | PARRAMATTA HIGH SCHOOL, 76A GREAT WESTERN HIGHWAY, PARRAMATTA NSW 2150 |
  
  So for this address, the generated autocomplete string is:
  
  > PARRAMATTA HIGH SCHOOL, 76A GREAT WESTERN HIGHWAY, PARRAMATTA NSW 2150

* **How can I search the flat address table?**

    I suggest you run a LIKE query with two wildcards on the `autocomplete` column, like follows:
    ```
    SELECT * FROM national_address_list WHERE autocomplete LIKE '%PARRAMATTA HIGH SCHOOL%';
    ```
    With the included index, this should be very quick for any query.

* **How often do I need to update this database?**

    There's a new G-NAF release every month.

* **Which G-NAF release is this script compatible with?**

    Any of them should work. I've most recently run it successfully with the February 2019 release.
    
* **I searched for X address/business and the result was incorrect/missing!**

    The data is put together by a combination of every Australian state and territory government along with the federal government. You can contact the relevant state/territory and get them to fix it in a subsequent G-NAF release.

* **I got an error/the importer crashed partway through the process!**

    That's frustrating! Please open an issue on this repository and I'd be happy to help you.
