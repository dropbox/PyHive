#!/bin/bash -eux
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e '
set mapred.job.tracker=local;
DROP TABLE IF EXISTS one_row;
CREATE TABLE one_row (number_of_rows INT);
INSERT INTO TABLE one_row VALUES (1);
'
