#!/bin/bash -eux

COLUMNS='
deep_row STRUCT<
        inner_int1: INT,
        inner_int2: INT,
        inner_int_array: ARRAY<INT>,
        inner_row1: STRUCT<
            inner_inner_varbinary: BINARY,
            inner_inner_string: STRING
        >
>,
deep_map MAP<STRING, STRUCT<
    double_attribute: DOUBLE,
    integer_attribute: INT,
    map_attribute: MAP<INT,ARRAY<STRING>>
>>,
deep_array ARRAY<STRUCT<int1: INT,double1: DOUBLE,string1: STRING>>
'

hive -e "
set mapred.job.tracker=local;
DROP TABLE IF EXISTS one_row_deep_complex;
CREATE TABLE one_row_deep_complex ($COLUMNS);
INSERT OVERWRITE TABLE one_row_deep_complex SELECT
    named_struct(
                'inner_int1', 2,
                'inner_int2', 3,
                'inner_int_array', array(4, 5),
                'inner_row1', named_struct(
                    'inner_inner_varbinary', cast('binarydata' as binary),
                    'inner_inner_string', 'some string'
                )
    ),
    map(
        'key1', named_struct(
            'double_attribute', 2.2,
            'integer_attribute', 60,
            'map_attribute', map(
                602, array('string1', 'string2'),
                21, array('other string', 'another string')
            )
        ),
        'key2', named_struct(
            'double_attribute', 42.15,
            'integer_attribute', 6060,
            'map_attribute', map(
                14, array('11string1', 'somestring'),
                22, array('other string', 'another string')
            )
        )
    ),
    array(
        named_struct(
            'int1', 42,
            'double1', 24.5,
            'string1', 'lalala'
        ),
        named_struct(
            'int1', 421,
            'double1', 244.25,
            'string1', 'bababa'
        )
    )
FROM one_row limit 1;
"
