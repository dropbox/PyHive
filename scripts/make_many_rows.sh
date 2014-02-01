#!/bin/bash

hive -e 'DROP TABLE IF EXISTS many_rows'
hive -e "
CREATE TABLE many_rows (
    a INT
) PARTITIONED BY (
    b STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE"

temp_file=/tmp/pyhive_test_data_many_rows.tsv
seq 0 9999 > $temp_file
hive -e "LOAD DATA LOCAL INPATH '$temp_file' INTO TABLE many_rows PARTITION (b='blah')"
rm -f $temp_file
