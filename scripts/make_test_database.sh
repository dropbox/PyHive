#!/bin/bash -eux
hive -e 'DROP DATABASE IF EXISTS pyhive_test_database'
hive -e 'CREATE DATABASE pyhive_test_database'
hive -e 'GRANT ALL ON DATABASE pyhive_test_database TO USER hadoop'
hive -e 'CREATE TABLE pyhive_test_database.dummy_table (a INT)'
