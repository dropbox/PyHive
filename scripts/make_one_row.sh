#!/bin/bash -eux
hive -e 'DROP TABLE IF EXISTS one_row'
hive -e 'CREATE TABLE one_row (number_of_rows INT)'
hive -e 'INSERT OVERWRITE TABLE one_row SELECT COUNT(*) + 1 FROM one_row'
