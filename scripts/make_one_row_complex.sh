#!/bin/bash -eux
hive -e 'DROP TABLE IF EXISTS one_row_complex'
hive -e 'CREATE TABLE one_row_complex (a map<INT, STRING>, b array<INT>)'
hive -e "INSERT OVERWRITE TABLE one_row_complex SELECT map(1, 'a', 2, 'b'), array(1, 2, 3) FROM one_row"
