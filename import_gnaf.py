#!/usr/bin/env python3
import psycopg2
import csv
import string
import glob
import os
import re
from datetime import datetime

start_time = datetime.now()

print('Starting G-NAF PostgreSQL Importer.')

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

# Build indexes so our database is nice and fast
print('Building indexes...')
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

# Vacuum and optimise our database
print('Vacuuming and optimising database...')
cursor.execute('VACUUM ANALYZE')

# Close our cursor and connection, we're done
cursor.close()
pg_connection.close()
time_taken = datetime.now() - start_time
minutes, seconds = divmod(time_taken.seconds, 60)
print('G-NAF data was successfully imported in ' + str(minutes) + ' minutes, ' + str(seconds) + ' seconds.')
