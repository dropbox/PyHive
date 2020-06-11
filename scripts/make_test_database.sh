#!/bin/bash -eux
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e '
DROP DATABASE IF EXISTS pyhive_test_database CASCADE;
CREATE DATABASE pyhive_test_database;
CREATE TABLE pyhive_test_database.dummy_table (a INT);
'
