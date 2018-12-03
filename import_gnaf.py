#!/usr/bin/env python3
import psycopg2
import csv
import string
import glob
import os
import re
from datetime import datetime

start_time = datetime.now()

print('Welcome to the GNAF PostgreSQL Importer.')

# Set up connection
print('Connecting to database...')
pg_connection = psycopg2.connect('host=localhost dbname=postgres user=postgres password=postgres')
pg_connection.set_session(autocommit=True)
cursor = pg_connection.cursor()

# Create GNAF database
print('Creating GNAF database...')
cursor.execute('CREATE DATABASE gnaf')

# Terminate connection so we can reconnect to our newly created DB instead
cursor.close()
pg_connection.close()

# Set up connection again
pg_connection = psycopg2.connect('host=localhost dbname=gnaf user=postgres password=postgres')
pg_connection.set_session(autocommit=True)
cursor = pg_connection.cursor()

# Import GNAF schema
print('Importing schema...')
cursor.execute(open('G-NAF/Extras/GNAF_TableCreation_Scripts/create_tables_ansi.sql', 'r').read())

# Go through GNAF data files
for filename in glob.glob('G-NAF/G-NAF*/Standard/*.psv'):
    file = open(filename, 'r')
    # Remove with regex the first characters up to and including the underscore (to remove state labels) as well as the trailing _psv.psv
    table = re.sub(r"^[^_]*_", "", os.path.basename(file.name).replace('_psv.psv',''))
    print("Importing " + os.path.basename(file.name) + "...")
    # As of at least the November 2018 data, the NSW_ADDRESS_SITE file contains a backslash that breaks PostgreSQL's COPY statement, so we'll escape it with sed
    # We don't do it for every file as it makes everything too slow, let's just hope they don't do it in an additional file in a later update :-)
    if os.path.basename(file.name) == 'NSW_ADDRESS_SITE_psv.psv':
        sed_command = "sed -i 's/\\\\/\\\\\\\\/g' "
        os.system(sed_command + os.path.basename(file.name))
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

# Close our cursor and connection, we're done
cursor.close()
pg_connection.close()
time_taken = datetime.now() - start_time
minutes, seconds = divmod(time_taken.seconds, 60)
print('GNAF data was successfully imported in ' + str(minutes) + ' minutes, ' + str(seconds) + ' seconds.')
