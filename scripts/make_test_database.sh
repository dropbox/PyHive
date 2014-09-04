#!/bin/bash -eux
hive -e '
DROP DATABASE IF EXISTS pyhive_test_database CASCADE;
CREATE DATABASE pyhive_test_database;
CREATE TABLE pyhive_test_database.dummy_table (a INT);
DROP DATABASE IF EXISTS sqlalchemy_test CASCADE;
CREATE DATABASE sqlalchemy_test;
DROP DATABASE IF EXISTS test_schema CASCADE;
CREATE DATABASE test_schema;
'
