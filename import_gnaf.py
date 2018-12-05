#!/usr/bin/env python3
import psycopg2
import csv
import string
import glob
import os
import re
import argparse
from datetime import datetime
from psycopg2.extras import RealDictCursor
from psycopg2.extras import execute_values

parser = argparse.ArgumentParser(description='Build a G-NAF database.')
parser.add_argument('--raw', action='store_true', help='import the raw G-NAF data without transformation')
args = parser.parse_args()

# Return the string or blank if None
def xstr(s):
    if s is None:
        return ''
    return str(s)

# Return the string and a comma or blank if None
def cstr(s):
    if s is None:
        return ''
    return str(s + ", ")

start_time = datetime.now()

if args.raw:
    print('Starting G-NAF PostgreSQL Importer in raw mode.')
else:
    print('Starting G-NAF PostgreSQL Importer in flat address table mode.')

# Set up connection
print('Connecting to PostgreSQL server...')
pg_connection = psycopg2.connect('host=localhost dbname=postgres user=postgres password=postgres')
pg_connection.set_session(autocommit=True)
cursor = pg_connection.cursor()

# Create G-NAF database
print('Creating G-NAF database...')
cursor.execute('CREATE DATABASE gnaf')

# Terminate connection so we can reconnect to our newly created DB instead
cursor.close()
pg_connection.close()

# Set up connection again
pg_connection = psycopg2.connect('host=localhost dbname=gnaf user=postgres password=postgres')
pg_connection.set_session(autocommit=True)
cursor = pg_connection.cursor()

# Import G-NAF schema
print('Importing schema...')
cursor.execute(open('G-NAF/Extras/GNAF_TableCreation_Scripts/create_tables_ansi.sql', 'r').read())

# Go through G-NAF data files
for filename in glob.glob('G-NAF/G-NAF*/Standard/*.psv'):
    file = open(filename, 'r')
    # Remove with regex the first characters up to and including the underscore (to remove state labels) as well as the trailing _psv.psv
    table = re.sub(r"^[^_]*_", "", os.path.basename(file.name).replace('_psv.psv',''))
    print("Importing " + os.path.basename(file.name) + "...")
    # As of at least the November 2018 data, the NSW_ADDRESS_SITE file contains a backslash that breaks PostgreSQL's COPY statement, so we'll escape it with sed
    # We don't do it for every file as it makes everything too slow, let's just hope they don't break an additional file in a later update :-)
    if os.path.basename(file.name) == 'NSW_ADDRESS_SITE_psv.psv':
        sed_command = "sed -i 's/\\\\/\\\\\\\\/g' "
        os.system(sed_command + '"' + filename + '"')
        file.close()
        file = open(filename, 'r')
    with file as f:
        for line in f:
            cursor.copy_from(f, table, sep="|",null = '')
    file.close()

# Go through Authority Code files
for filename in glob.glob('G-NAF/G-NAF*/Authority Code/*.psv'):
    file = open(filename, 'r')
    table = os.path.basename(file.name).replace('Authority_Code_','').replace('_psv.psv','')
    print("Importing " + os.path.basename(file.name) + "...")
    with file as f:
        for line in f:
            cursor.copy_from(f, table, sep="|",null = '')
    file.close()

# Import foreign key constraints
print('Importing foreign key constraints...')
cursor.execute(open('G-NAF/Extras/GNAF_TableCreation_Scripts/add_fk_constraints.sql', 'r').read())

# Import address view
print('Importing address view...')
cursor.execute(open('G-NAF/Extras/GNAF_View_Scripts/address_view.sql', 'r').read())
cursor.close()
pg_connection.close()

if args.raw:
    # Build indexes so our database is nice and fast
    print('Building indexes...')
    pg_connection = psycopg2.connect('host=localhost dbname=gnaf user=postgres password=postgres')
    pg_connection.set_session(autocommit=True)
    cursor = pg_connection.cursor()
    cursor.execute('CREATE INDEX address_detail_flat_type_code ON address_detail (flat_type_code)')
    cursor.execute('CREATE INDEX address_detail_level_type_code ON address_detail (level_type_code)')
    cursor.execute('CREATE INDEX address_detail_street_locality_pid ON address_detail (street_locality_pid)')
    cursor.execute('CREATE INDEX street_locality_street_locality_pid ON street_locality (street_locality_pid)')
    cursor.execute('CREATE INDEX street_locality_street_suffix_code ON street_locality (street_suffix_code)')
    cursor.execute('CREATE INDEX street_locality_street_class_code ON street_locality (street_class_code)')
    cursor.execute('CREATE INDEX locality_locality_pid ON locality (locality_pid)')
    cursor.execute('CREATE INDEX address_detail_locality_pid ON address_detail (locality_pid)')
    cursor.execute('CREATE INDEX address_default_geocode_address_detail_pid ON address_default_geocode (address_detail_pid)')
    cursor.execute('CREATE INDEX address_detail_address_detail_pid ON address_detail (address_detail_pid)')
    cursor.execute('CREATE INDEX address_default_geocode_geocode_type_code ON address_default_geocode (geocode_type_code)')
    cursor.execute('CREATE INDEX address_detail_level_geocoded_code ON address_detail (level_geocoded_code)')
    cursor.execute('CREATE INDEX locality_state_pid ON locality (state_pid)')
    cursor.close()
    pg_connection.close()
else:
    print('Building flat address table from address view...')
    pg_connection = psycopg2.connect('host=localhost dbname=gnaf user=postgres password=postgres')
    pg_connection.set_session(autocommit=True)
    cursor = pg_connection.cursor()
    cursor.execute('CREATE TABLE national_address_list AS SELECT * FROM public.address_view')
    cursor.execute('ALTER TABLE national_address_list ADD COLUMN autocomplete VARCHAR')
    cursor.execute('ALTER TABLE national_address_list ADD PRIMARY KEY (address_detail_pid)')
    cursor.close()
    pg_connection.close()

    # Build a list of every address in the country, alongside a neatly formatted autocomplete string
    # We'll do this state by state so we don't run out of memory
    states = ['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']
    for state in states:
        # Re-open our cursor with the ability to see column names
        pg_connection = psycopg2.connect('host=localhost dbname=gnaf user=postgres password=postgres')
        pg_connection.set_session(autocommit=True)
        cursor = pg_connection.cursor(cursor_factory=RealDictCursor)
        cursor.itersize = 10000
        autocomplete_set = []
        print('Building formatted autocomplete string for every address in %s...' %(state))
        cursor.execute('SELECT * FROM national_address_list WHERE state_abbreviation = %s', (state,))
        for row in cursor:
            autocomplete = ''
            autocomplete += cstr(row['building_name'])
            if row['flat_type'] is not None:
                autocomplete += xstr(row['flat_type']) + " "
            if row['flat_number_prefix'] is not None or row['flat_number'] is not None or row['flat_number_suffix'] is not None:
                autocomplete += xstr(row['flat_number_prefix']) + xstr(row['flat_number']) + xstr(row['flat_number_suffix']) + ", "
            if row['number_first'] is not None or row['number_first_suffix'] is not None:
                autocomplete += xstr(row['number_first']) + xstr(row['number_first_suffix'])
            if row['number_last'] is not None or row['number_last_suffix'] is not None:
                autocomplete += "-" + xstr(row['number_last']) + xstr(row['number_last_suffix']) + " "
            else:
                autocomplete += " "
            autocomplete += xstr(row['street_name']) + " "
            if row['street_type_code'] is not None:
                autocomplete += cstr(row['street_type_code'])
            else:
                autocomplete += ", "
            autocomplete += xstr(row['locality_name']) + " " + xstr(row['state_abbreviation']) + " " + xstr(row['postcode'])
            autocomplete_set.append((row['address_detail_pid'], autocomplete))
        cursor.close()
        pg_connection.close()

        # Bulk update every address in the database from our list
        pg_connection = psycopg2.connect('host=localhost dbname=gnaf user=postgres password=postgres')
        pg_connection.set_session(autocommit=True)
        cursor = pg_connection.cursor()
        print('Bulk updating every address in %s with an autocomplete string...' %(state))
        execute_values(cursor, "UPDATE national_address_list SET autocomplete = autocomplete_set.autocomplete FROM (VALUES %s) AS autocomplete_set (address_detail_pid, autocomplete) WHERE national_address_list.address_detail_pid = autocomplete_set.address_detail_pid", autocomplete_set)
        cursor.close()
        pg_connection.close()

    # Build a fancy GIN index
    print('Building indexes...')
    pg_connection = psycopg2.connect('host=localhost dbname=gnaf user=postgres password=postgres')
    pg_connection.set_session(autocommit=True)
    cursor = pg_connection.cursor()
    cursor.execute('CREATE EXTENSION btree_gin')
    cursor.execute('CREATE EXTENSION pg_trgm')
    cursor.execute('CREATE INDEX national_address_list_autocomplete ON national_address_list USING GIN (autocomplete gin_trgm_ops)')

    # Drop all tables besides our flat one, along with the imported view
    cursor.execute('DROP TABLE address_alias, address_alias_type_aut, address_change_type_aut, address_default_geocode, address_detail, address_feature, address_mesh_block_2011, address_mesh_block_2016, address_site, address_site_geocode, address_type_aut, flat_type_aut, geocode_reliability_aut, geocode_type_aut, geocoded_level_type_aut, level_type_aut, locality, locality_alias, locality_alias_type_aut, locality_class_aut, locality_neighbour, locality_point, mb_2011, mb_2016, mb_match_code_aut, primary_secondary, ps_join_type_aut, state, street_class_aut, street_locality, street_locality_alias, street_locality_alias_type_aut, street_locality_point, street_suffix_aut, street_type_aut CASCADE')
    cursor.close()
    pg_connection.close()

# Vacuum and optimise our database
print('Vacuuming and optimising database...')
pg_connection = psycopg2.connect('host=localhost dbname=gnaf user=postgres password=postgres')
pg_connection.set_session(autocommit=True)
cursor = pg_connection.cursor()
cursor.execute('VACUUM ANALYZE')
cursor.close()
pg_connection.close()

time_taken = datetime.now() - start_time
minutes, seconds = divmod(time_taken.seconds, 60)
print('G-NAF data was successfully imported in ' + str(minutes) + ' minutes, ' + str(seconds) + ' seconds.')
